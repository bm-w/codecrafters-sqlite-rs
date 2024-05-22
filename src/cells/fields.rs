use std::{fs::File, iter::FusedIterator, num::NonZeroU64};

use anyhow::Context as _;
use itertools::Itertools as _;

use crate::{page::{Header, Page, PageKind}, vals::{Type, Varint}};

use super::Cell;


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
	pub(crate) fn parse_record_fields(&self) -> anyhow::Result<RecordFields<'a>> {
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
	where F: Fn(&Cell, NonZeroU64, RecordFields) -> anyhow::Result<Option<T>> + 'a {
		anyhow::ensure!(matches!(self.kind, PageKind::InteriorTable { .. } | PageKind::LeafTable),
			"can only scan fields of table page records");
		self.scan_records(file, file_header, move |cell| {
			let rowid = cell.rowid.expect("table page records must have `rowid`s");
			let fields = cell.parse_record_fields().with_context(|| format!("parsing fields \
				for record cell with rowid {}", rowid))?;
			transform(cell, rowid, fields)
		})
	}

	pub(crate) fn parse_records_fields(&self)
	-> anyhow::Result<impl Iterator<Item = anyhow::Result<(Cell, RecordFields)>>> {
		Ok(self.cells()
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
