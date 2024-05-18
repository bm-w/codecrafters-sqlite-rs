use std::{fs::File, iter::{Enumerate, FusedIterator, StepBy}, num::NonZeroU64};

use anyhow::Context as _;
use itertools::Itertools as _;

use crate::page::{Header, Page, PageKind};
use crate::vals::{Type, Varint};


pub(crate) struct Cell<'a> {
	pub(crate) left_page: Option<NonZeroU64>,
	pub(crate) rowid: NonZeroU64,
	pub(crate) payload: &'a [u8],
}


// Page cells

struct PageCellsState {
	file_header_len: usize, // 0 or 100
	indexed_cell_ptr_offsets: Enumerate<StepBy<std::ops::Range<usize>>>,
}

pub(crate) struct PageCells<'a> {
	page: &'a Page,
	state: PageCellsState
}

impl PageCellsState {
	fn next<'a>(&mut self, page: &'a Page) -> Option<(usize, anyhow::Result<Cell<'a>>)> {
		fn inner(page: &Page, file_header_len: usize, mut offset: usize)
		-> anyhow::Result<Cell> {
			anyhow::ensure!(page.buf.len() >= offset + 2,
				"expected page length of at least {} bytes, but found {}",
				file_header_len + offset + 2,
				file_header_len + page.buf.len());

			offset = u16::from_be_bytes(page.buf[offset..offset + 2].try_into().unwrap()) as usize;
			// The offset is relative to the start of the persisted page, which may include the file
			// header; but in this impl. the file header is held separately from the page.
			offset -= file_header_len;

			let (left_page, payload_len) = match page.kind {
				PageKind::InteriorTable { .. } => {
					anyhow::ensure!(page.buf.len() >= offset + 4,
						"expected page length of at least {} bytes, but found {}",
						file_header_len + offset + 4,
						file_header_len + page.buf.len());
					let left_page = u32::from_be_bytes(page.buf[offset..offset + 4]
						.try_into().unwrap()) as u64;
					offset += 4;
					let left_page = left_page.try_into()
						.context("expected non-zero left page number")?;
					(Some(left_page), 0)
				}
				PageKind::LeafTable => {
					let (size, payload_len) = Varint::parse(&page.buf[offset..])
						.context("parsing first cell’s payload size `varint`")?;
					offset += size;
					let payload_len = payload_len.0 as usize;
					(None, payload_len)
				}
			};

			let (size, rowid) = Varint::parse(&page.buf[offset..])
				.context("parsing cell’s rowid `varint`")?;
			let rowid: u64 = rowid.0.try_into().context("expected non-negative `rowid`")?;
			let rowid = rowid.try_into().context("expected non-zero `rowid`")?;
			offset += size;

			Ok(Cell { left_page, rowid, payload: &page.buf[offset..offset + payload_len] })
		}

		let (cell_idx, offset) = self.indexed_cell_ptr_offsets.next()?;
		let next = inner(page, self.file_header_len, offset)
			.with_context(|| format!("parsing page #{} cell {cell_idx}", page.num));
		if next.is_err() { self.indexed_cell_ptr_offsets.by_ref().for_each(drop) } // Fuse
		Some((cell_idx, next))
	}
}

impl<'a> Iterator for PageCells<'a> {
	type Item = anyhow::Result<Cell<'a>>;
	fn next(&mut self) -> Option<Self::Item> {
		self.state.next(self.page).map(|(_, next)| next)
	}
}

impl FusedIterator for PageCells<'_> {}

impl Page {
	fn parse_cells(&self) -> anyhow::Result<PageCells> {
		let file_header_len = self.file_header.as_ref().map(|h| h.buf.len()).unwrap_or(0);

		let cell_ptrs_offset = self.kind.header_len();
		let cell_ptrs_end = cell_ptrs_offset + self.num_cells * 2;
		anyhow::ensure!(self.buf.len() >= cell_ptrs_end,
			"expected page size of at least {} bytes, but found {}",
			file_header_len + cell_ptrs_end,
			file_header_len + self.buf.len());

		let indexed_cell_ptr_offsets = (cell_ptrs_offset..cell_ptrs_end).step_by(2).enumerate();
		let state = PageCellsState { file_header_len, indexed_cell_ptr_offsets };
		Ok(PageCells { page: self, state })
	}
}


// Records

pub(crate) struct ScanRecords<'a, F> {
	file: &'a mut File,
	file_header: &'a Header,
	root_page: &'a Page,
	root_state: PageCellsState,
	stack: Vec<(Page, PageCellsState)>,
	transform: F,
	failed: bool, // Fuse
}

impl<F, T> Iterator for ScanRecords<'_, F> where F: Fn(Cell) -> anyhow::Result<T> {
	type Item = anyhow::Result<T>;
	fn next(&mut self) -> Option<Self::Item> {
		if self.failed { return None }

		let (page, state) = self.stack.last_mut()
			.map(|(ref page, state)| (page, state))
			.unwrap_or((self.root_page, &mut self.root_state));

		let (err, kind) = match (PageCellsState::next(state, page), &page.kind) {
			(Some((cell_index, Ok(cell))), PageKind::LeafTable) => {
				let rowid = cell.rowid;
				match (self.transform)(cell)
					.with_context(|| format!("processing row cell {cell_index} \
						with `rowid` {rowid}"))
				{
					Ok(result) => return Some(Ok(result)),
					Err(err) => {
						self.failed = true;
						(err, "leaf")
					}
				}
			}
			(Some((cell_idx, Ok(cell))), PageKind::InteriorTable { .. }) => {
				let num = cell.left_page
					.expect("interior pages must have left child pointer page numbers");
				match Page::parse(self.file, self.file_header, num)
					.with_context(|| format!("parsing page #{num}"))
					.and_then(|page| match page.parse_cells() {
						Ok(PageCells { state, ..}) => Ok((page, state)),
						Err(err) => Err(err)
							.with_context(|| format!("parsing records for page #{}", page.num))
					})
					.with_context(|| format!("parsing child page pointed \
						to by cell {cell_idx} with `rowid` {}", cell.rowid))
				{
					Ok((page, state)) => {
						self.stack.push((page, state));
						return self.next()
					}
					Err(err) => {
						self.failed = true;
						(err, "interior")
					}
				}
			}
			(Some((_, Err(err))), PageKind::LeafTable) => (err, "leaf"),
			(Some((_, Err(err))), PageKind::InteriorTable { .. }) => (err, "interior"),
			(None, _) => return self.stack.pop().and_then(|_| self.next())
		};

		Some(Err(err).with_context(|| format!("parsing {kind} page #{}", page.num)))
	}
}

impl<F, T> FusedIterator for ScanRecords<'_, F> where F: Fn(Cell) -> anyhow::Result<T> {}

impl Page {
	pub(crate) fn scan_records<'a, T, F>(
		&'a self,
		file: &'a mut File,
		file_header: &'a Header,
		transform: F,
	) -> anyhow::Result<ScanRecords<'a, F>>
	where F: Fn(Cell) -> anyhow::Result<T> {
		let PageCells { state, .. } = self.parse_cells()
			.with_context(|| format!("parsing cells for page #{}", self.num))?;

		Ok(ScanRecords {
			file,
			file_header,
			root_page: self,
			root_state: state,
			stack: Vec::new(),
			transform,
			failed: false,
		})
	}
}


// Record fields

pub(crate) struct RecordFields<'a> {
	cell: Cell<'a>,
	record_header_len: usize,
	type_offset: usize,
	val_offset: usize,
	idx: usize,
}

impl<'a> RecordFields<'a> {
	fn next_field(&mut self) -> anyhow::Result<(Type, &'a [u8])> {
		let (type_size, typ) = Varint::parse(&self.cell.payload[self.type_offset..])
			.context("parsing type")?;
		let typ: Type = typ.into();
		let val_size = typ.val_len().context("computing value length")?;
		anyhow::ensure!(self.cell.payload.len() >= self.val_offset + val_size,
			"expected cell payload length of at least {} bytes, but found {}",
			self.val_offset + val_size,
			self.cell.payload.len());
		let val = &self.cell.payload[self.val_offset..self.val_offset + val_size];
		self.type_offset += type_size;
		self.val_offset += val_size;
		Ok((typ, val))
	}
}

impl<'a> Iterator for RecordFields<'a> {
	type Item = anyhow::Result<(Type, &'a [u8])>;
	fn next(&mut self) -> Option<Self::Item> {
		if self.type_offset == self.record_header_len { return None }
		self.idx += 1;
		let next = self.next_field().with_context(|| format!("parsing field {}", self.idx - 1));
		if next.is_err() { self.type_offset = self.record_header_len } // Fuse
		Some(next)
	}
}

impl FusedIterator for RecordFields<'_> {}

impl<'a> Cell<'a> {
	fn parse_record_fields(&self) -> anyhow::Result<RecordFields<'a>> {
		anyhow::ensure!(self.left_page.is_none(), "expected record cell");
		let (size, record_header_len) = Varint::parse(self.payload)
			.context("parsing `varint` as record header length")?;
		let record_header_len = record_header_len.0 as usize;
		anyhow::ensure!(self.payload.len() >= record_header_len,
			"expected cell payload length of at least {} bytes, but found {}",
			record_header_len,
			self.payload.len());

		Ok(RecordFields {
			cell: Cell { ..*self },
			record_header_len,
			type_offset: size,
			val_offset: record_header_len,
			idx: 0
		})
	}
}

impl Page {
	pub(crate) fn scan_records_fields<'a, T, F>(
		&'a self,
		file: &'a mut File,
		file_header: &'a Header,
		transform: F,
	) -> anyhow::Result<impl Iterator<Item = anyhow::Result<T>> + 'a>
	where F: Fn(Cell, RecordFields) -> anyhow::Result<T> + 'a {
		self.scan_records(file, file_header, move |cell| {
			let fields = cell.parse_record_fields().with_context(|| format!("parsing fields \
				for record cell with rowid {}", cell.rowid))?;
			transform(cell, fields)
		})
	}

	pub(crate) fn parse_records_fields(&self)
	-> anyhow::Result<impl Iterator<Item = anyhow::Result<(Cell, RecordFields)>>> {
		Ok(self.parse_cells()
			.context("parsing page records")?
			.enumerate()
			.map(|(idx, cell)| {
				let cell = cell
					.with_context(|| format!("parsing cell {idx}"))?;
				let record_fields = cell.parse_record_fields()
					.with_context(|| format!("parsing record fields for cell {idx}"))?;
				Ok((cell, record_fields))
			})
			.take_while_inclusive(Result::is_ok))
	}
}
