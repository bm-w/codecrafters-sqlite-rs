use std::{borrow::Cow, num::NonZeroU64};

use anyhow::Context as _;
use itertools::Itertools as _;

use crate::{page::Page, cells::{Cell, fields::RecordFields}};


pub(crate) enum SchemaKind<'a> { Table, Index { table_name: Cow<'a, str> } }

pub(crate) struct SchemaRecord<'a> {
	pub(crate) id: NonZeroU64,
	pub(crate) kind: SchemaKind<'a>,
	pub(crate) name: Cow<'a, str>,
	pub(crate) root_page: NonZeroU64,
	pub(crate) sql: Cow<'a, str>,
}

impl<'a> TryFrom<(Cell<'a>, RecordFields<'a>)> for SchemaRecord<'a> {
	type Error = anyhow::Error;
	fn try_from(record_fields: (Cell<'a>, RecordFields<'a>)) -> Result<Self, Self::Error> {
		let (cell, mut fields) = record_fields;

		let rowid = cell.rowid.context("expected table cell with `rowid`")?;

		// `type` column
		let (typ, table_type) = fields.next().context("expected `type` column field")??;
		anyhow::ensure!(typ.is_text() && (table_type == b"table" || table_type == b"index"),
			"expected \"table\" or \"index\" text for `type` value");

		// `name` column
		let (typ, name) = fields.next().context("expected `name` column field")??;
		anyhow::ensure!(typ.is_text(), "expected text type for `name` column");
		let name = std::str::from_utf8(name).expect("reading `name` value as UTF-8");

		// `tbl_name` column
		let (typ, table_name) = fields.next().context("expected `table_name` column field")??;
		anyhow::ensure!(typ.is_text(), "expected text type for `table_name` column");
		let table_name = std::str::from_utf8(table_name)
			.expect("reading `table_name` value as UTF-8");

		let kind = if table_type == b"table" {
			anyhow::ensure!(name == table_name, "expected identical `name` and `tbl_name` fields");
			SchemaKind::Table
		} else {
			anyhow::ensure!(name != table_name, "expected different `name` and `tbl_name` fields");
			SchemaKind::Index { table_name: table_name.into() }
		};

		// `rootpage` column
		let (typ, root_page) = fields.next().context("expected `rootpage` column field")??;
		let (_, root_page) = typ.int_val(root_page)
			.context("reading `rootpage` integer value")?;
		let root_page: u64 = root_page.context("expected non-NULL `rootpage` value")?
			.try_into().context("expected non-negative `rootpage` value")?;
		let root_page = root_page.try_into().context("expected non-zero `rootpage` column value")?;

		let (typ, sql) = fields.next().context("expected `sql` column field")??;
		anyhow::ensure!(typ.is_text(), "expected text type for `sql` column");
		let sql = std::str::from_utf8(sql).expect("reading `sql` value as UTF-8");

		fields.for_each(drop); // Should be empty, but just in case

		Ok(SchemaRecord {
			id: rowid,
			kind,
			name: name.into(),
			root_page,
			sql: sql.into(),
		})
	}
}

impl Page {
	pub(crate) fn parse_schema_records(&self)
	-> anyhow::Result<impl Iterator<Item = anyhow::Result<SchemaRecord>>> {
		Ok(self.parse_records_fields()
			.context("parsing page records’ fields")?
			.map(|cell_record_fields| SchemaRecord::try_from(cell_record_fields?))
			.take_while_inclusive(Result::is_ok))
	}
}