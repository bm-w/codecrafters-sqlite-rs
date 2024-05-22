use std::{iter::FusedIterator, num::NonZeroU64};

use anyhow::Context as _;

pub(crate) mod fields;
pub(crate) mod scan;
pub(crate) mod search;

use crate::page::{Page, PageKind};
use crate::vals::Varint;


pub(crate) struct Cell<'a> {
	pub(crate) left_page: Option<NonZeroU64>,
	pub(crate) rowid: Option<NonZeroU64>,
	pub(crate) payload: &'a [u8],
}

impl Page {
	fn parse_cell(&self, cell_idx: usize)
	-> anyhow::Result<Cell> {
		fn parse_left_page_num(page_buf: &[u8], offset: &mut usize) -> anyhow::Result<NonZeroU64> {
			anyhow::ensure!(page_buf.len() >= *offset + 4,
				"expected page length of at least {} bytes, but found {}",
				// TODO: `file_header_len`
				*offset + 4,
				page_buf.len());
			let num = (u32::from_be_bytes(page_buf[*offset ..*offset + 4]
				.try_into().unwrap()) as u64)
				.try_into().context("expected non-zero left page number")?;
			*offset += 4;
			Ok(num)
		}

		fn parse_payload_len(page_buf: &[u8], offset: &mut usize) -> anyhow::Result<usize> {
			let (size, payload_len) = Varint::parse(&page_buf[*offset..])
				.context("parsing first cell’s payload size `varint`")?;
			let payload_len = payload_len.0 as usize;
			*offset += size;
			Ok(payload_len)
		}

		fn parse_rowid(page_buf: &[u8], offset: &mut usize) -> anyhow::Result<NonZeroU64> {
			let (size, rowid) = Varint::parse(&page_buf[*offset..])
				.context("parsing cell’s rowid `varint`")?;
			let rowid: u64 = rowid.0.try_into().context("expected non-negative `rowid`")?;
			let rowid = rowid.try_into().context("expected non-zero `rowid`")?;
			*offset += size;
			Ok(rowid)
		}

		let mut offset = self.kind.header_len() + cell_idx * 2;

		let file_header_len = self.file_header.as_ref().map(|h| h.buf.len()).unwrap_or(0);
		anyhow::ensure!(self.buf.len() >= offset + 2,
			"expected page length of at least {} bytes, but found {}",
			file_header_len + offset + 2,
			file_header_len + self.buf.len());
		offset = u16::from_be_bytes(self.buf[offset..offset + 2].try_into().unwrap()) as usize;
		// The offset is relative to the start of the persisted page, which may include the file
		// header; but in this impl. the file header is held separately from the page.
		offset -= file_header_len;

		let (left_page, payload_len, rowid) = match self.kind {
			PageKind::InteriorIndex { .. } => {
				let left_page = parse_left_page_num(&self.buf, &mut offset)?;
				let payload_len = parse_payload_len(&self.buf, &mut offset)?;
				(Some(left_page), payload_len, None)
			}
			PageKind::InteriorTable { .. } => {
				let left_page = parse_left_page_num(&self.buf, &mut offset)?;
				let rowid = parse_rowid(&self.buf, &mut offset)?;
				(Some(left_page), 0, Some(rowid))
			}
			PageKind::LeafIndex => {
				let payload_len = parse_payload_len(&self.buf, &mut offset)?;
				(None, payload_len, None)
			}
			PageKind::LeafTable => {
				let payload_len = parse_payload_len(&self.buf, &mut offset)?;
				let rowid = parse_rowid(&self.buf, &mut offset)?;
				(None, payload_len, Some(rowid))
			}
		};

		let payload = &self.buf[offset..offset + payload_len];
		Ok(Cell { left_page, rowid, payload })
	}
}


// Page cells

struct PageCellsState {
	cell_idx: usize,
}

pub(crate) struct PageCells<'a> {
	page: &'a Page,
	state: PageCellsState
}

impl PageCellsState {
	fn next<'a>(&mut self, page: &'a Page) -> Option<(usize, anyhow::Result<Cell<'a>>)> {
		if self.cell_idx == page.num_cells { return None }
		let next = page.parse_cell(self.cell_idx)
			.with_context(|| format!("parsing page #{} cell {}", self.cell_idx, page.num));
		if next.is_err() { self.cell_idx = page.num_cells  } // Fuse
		self.cell_idx += 1;
		Some((self.cell_idx - 1, next))
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
	pub(crate) fn cells(&self) -> PageCells {
		PageCells { page: self, state: PageCellsState { cell_idx: 0 } }
	}
}
