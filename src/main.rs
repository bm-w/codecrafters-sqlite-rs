use std::{borrow::Cow, fs::File, num::NonZeroU64};

use anyhow::Context as _;
use itertools::Itertools as _;

mod cells;
mod page;
mod schema;
mod sql;
mod util;
mod vals;

use cells::search::MatchOrdering;
use page::{Header, Page};
use schema::{SchemaKind, SchemaRecord};
use sql::{SqlCreateColumn, SqlSelect, SqlSelectColumn, SqlSelectCount, SqlWhere, SqlWhereOp};
use util::LiftFallibleIteratorExt as _;
use vals::Type;


fn main() -> anyhow::Result<()> {
	// Parse arguments
	let mut args = std::env::args();
	let database_path = args.nth(1).context("expected <database path> argument")?;
	let command = args.next().context("expected <command> argument")?;

	match command.as_str() {
		".dbinfo" => {
			let (page, _file) = Page::parse_first(database_path.as_ref())
				.context("parsing first page")?;
			let header = page.file_header.as_ref().expect("first page has file header");

			println!("database page size: {}", header.page_size);

			let num_tables = page
				.parse_schema_records()
				.lift_fallible_iterator()
				.try_fold(0usize, |acc, r| r.map(|r| acc
					+ if matches!(r.kind, SchemaKind::Table) { 0 } else { 1 }))?;

			println!("number of tables: {}", num_tables);
		}
		".tables" => {
			let (page, _file) = Page::parse_first(database_path.as_ref())
				.context("parsing first page")?;

			let mut schema_records = page
				.parse_schema_records()
				.lift_fallible_iterator()
				.filter(|r| !r.as_ref().is_ok_and(|r| !matches!(r.kind, SchemaKind::Table)))
				.collect::<anyhow::Result<Vec<_>>>()?;
			schema_records.sort_by_key(|sr| sr.id);

			println!("{}", schema_records.iter().map(|sr| sr.name.as_ref()).join(" "));
		}
		sql if !sql.starts_with('.') => {
			let sql_select = SqlSelect::try_from(sql).context("parsing SQL `SELECT` query")?;

			let (first_page, mut file) = Page::parse_first(database_path.as_ref())
				.context("parsing first page")?;
			let file_header = first_page.file_header.as_ref().expect("first page has file header");

			let (table_record, index_record) = {
				let (mut table, mut index) = (None, None);
				let schema_records = first_page
					.parse_schema_records()
					.lift_fallible_iterator();

				for (idx, schema_record) in schema_records.enumerate() {
					let schema_record = schema_record
						.with_context(|| format!("parsing schema record {idx}"))?;
					match &schema_record.kind {
						SchemaKind::Table if schema_record.name == sql_select.table_name =>
							table = Some(schema_record),
						SchemaKind::Index { table_name } if table_name == sql_select.table_name =>
							anyhow::ensure!(index.replace(schema_record).is_none(),
								"only one index per table is supported"),
						_ => (),
					};
				}

				(
					table.with_context(|| format!("no matching schema record \
						for table {:?}", sql_select.table_name))?,
					index,
				)
			};

			let table_page = Page::parse(&mut file, file_header, table_record.root_page)
				.with_context(|| format!("parsing page #{}", table_record.root_page))?;

			if sql_select.columns.len() == 1
				&& matches!(sql_select.columns[0], SqlSelectColumn::Count(SqlSelectCount::All)) {
				println!("{}", table_page.num_cells);
			} else {
				let sql_create_table_cols = SqlCreateColumn::parse_table(sql_select.table_name, &table_record.sql)
					.with_context(|| format!("parsing SQL create statement for table {:?}", sql_select.table_name))?
					.collect::<anyhow::Result<Vec<_>>>()
					.with_context(|| format!("parsing columns for table {:?}", sql_select.table_name))?;

				let select_col_idxs = sql_select.columns.iter()
					.map(|sel_col| match sel_col {
						&SqlSelectColumn::Ident(col_name) =>
							sql_create_table_cols.iter()
								.position(|c| c.name == col_name)
								.with_context(|| format!("no matching column for name {col_name}")),
						_ => anyhow::bail!("expected column name"),
					})
					.collect::<anyhow::Result<Vec<_>>>()
					.context("finding `SELECT` column indices")?;

				let indexed_where_col = sql_select.wher.as_ref().map(|wher| -> anyhow::Result<_> {
					let col_idx = sql_create_table_cols.iter()
						.position(|col| col.name == wher.left_col_name)
						.with_context(|| format!("no matching column for name {}", wher.left_col_name))?;
					Ok((col_idx, wher))
				}).transpose()?;

				match (indexed_where_col, index_record) {
					(Some(indexed_where_col), Some(index_record)) =>
						index_scan(&mut file, file_header, &table_page, &index_record,
							sql_create_table_cols, indexed_where_col.1, select_col_idxs)?,
					_ => full_scan(&mut file, file_header, &table_page,
						sql_create_table_cols, indexed_where_col, select_col_idxs)?
				}
			}
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
}

fn table_record_vals<'a>(
	rowid: NonZeroU64,
	fields: &'a [(Type, &'a [u8])],
	col_idxs: &'a [usize],
	sql_create_table_cols: &'a [SqlCreateColumn],
) -> anyhow::Result<Box<str>> {
	Ok(col_idxs.iter().map(move |&col_idx| Ok(if sql_create_table_cols[col_idx].is_rowid_alias {
		Cow::from(format!("{rowid}"))
	} else {
		let &(typ, val) = fields.get(col_idx)
			.context("expected valid column index")?;
		anyhow::ensure!(typ.is_text(),
			"expected text type for column {:?}, found type {typ}",
			sql_create_table_cols[col_idx].name);
		Cow::from(std::str::from_utf8(val)
			.with_context(|| format!("expected UTF-8 string for column {:?} value",
				sql_create_table_cols[col_idx].name))?)
	})).try_fold(String::new(), |mut acc, val| {
		let val = val?;
		if !acc.is_empty() { acc.push('|') }
		acc.push_str(val.as_ref());
		anyhow::Result::<_>::Ok(acc)
	})?.into_boxed_str())
}

fn full_scan(
	file: &mut File,
	file_header: &Header,
	table_page: &Page,
	sql_create_table_cols: Vec<SqlCreateColumn>,
	indexed_where_col: Option<(usize, &SqlWhere)>,
	sql_select_col_idxs: Vec<usize>,
) -> anyhow::Result<()> {
	let rows = table_page
		.scan_records_fields(file, file_header, |_cell, rowid, record_fields| {
			// TODO: Avoid allocating?
			let fields = record_fields.collect::<anyhow::Result<Vec<_>>>()?;

			if let Some((
				col_idx,
				&SqlWhere { op: SqlWhereOp::Eq, left_col_name, right_str_operand }
			)) = indexed_where_col {
				let &(typ, val) = fields.get(col_idx)
					.context("expected valid column index")?;
				anyhow::ensure!(typ.is_text(),
					"expected text type for column {:?}, found type {typ}",
					left_col_name);
				let val = std::str::from_utf8(val)
					.with_context(|| format!("expected UTF-8 string for column {:?} value",
						left_col_name))?;
				if val != right_str_operand {
					return Ok(Some(None));
				}
			}

			Ok(Some(Some(table_record_vals(rowid, &fields,
				&sql_select_col_idxs, &sql_create_table_cols)?)))
		})
		.lift_fallible_iterator();

	for (idx, row) in rows.enumerate() {
		if let Some(row) = row.with_context(|| format!("reading row {idx}"))? {
			println!("{row}");
		}
	}

	Ok(())
}

fn index_scan(
	file: &mut File,
	file_header: &Header,
	table_page: &Page,
	index_schema: &SchemaRecord,
	sql_create_table_cols: Vec<SqlCreateColumn>,
	wher: &SqlWhere,
	sql_select_col_idxs: Vec<usize>,
) -> anyhow::Result<()> {
	let SchemaRecord { kind: SchemaKind::Index { ref table_name }, .. } = index_schema
	else { panic!("expected index schema") };

	let index_create_col = SqlCreateColumn
		::parse_index(index_schema.name.as_ref(), table_name.as_ref(), &index_schema.sql)
		.with_context(|| format!("parsing SQL create statement for index {:?}", index_schema.name))?
		// TODO: Suport more columns?
		.exactly_one()
		.map_err(|_| anyhow::anyhow!("expected exactly one index column (more unsupported"))?
		.with_context(|| format!("parsing SQL create columns for index {:?}", index_schema.name))?;

	anyhow::ensure!(matches!(wher.op, SqlWhereOp::Eq),
		"expected `=` equals op in `WHERE` clause (other unsupported");
	
	anyhow::ensure!(wher.left_col_name == index_create_col.name,
		"expected `WHERE` column name to correspond to index (otherwise unsupported)");

	let index_page = Page::parse(file, file_header, index_schema.root_page)
		.with_context(|| format!("parsing page for index {}", index_schema.name))?;

	let search = index_page
		.search_records(file, file_header, |cell| {
			let mut fields = cell.parse_record_fields().unwrap();
			let (_typ, bytes) = fields.next().unwrap().unwrap();
			MatchOrdering::try_from(wher.right_str_operand.as_bytes().cmp(bytes), || {
				let (typ, bytes) = fields.last()
					.context("expected another cell field")?
					.context("parsing last cell field")?;
				fn rowid(typ: Type, bytes: &[u8]) -> anyhow::Result<NonZeroU64> {
					let (_size, rowid) = typ.int_val(bytes)
						.context("expected integer value")?;
					let rowid: u64 = rowid
						.context("expected non-nil value")?
						.try_into()
						.context("expected non-negative value")?;
					rowid.try_into()
						.context("expected non-zero value")
				}
				rowid(typ, bytes)
					.context("parsing cell target `rowid`")
			})
		})
		.with_context(|| format!("searching index {:?}", index_schema.name))?
		.with_context(|| format!("no result searching index {:?}", index_schema.name))?
		.into_scan()
		.collect::<Vec<_>>();

	for (idx, rowid) in search.into_iter().enumerate() {
		let rowid = rowid.with_context(|| format!("scanning index for {}{} `rowid`", idx + 1,
			match (idx + 1) % 10 { 1 => "st", 2 => "nd", 3 => "rd", _ => "th" }))?;
		let row = table_page.search_records(file, file_header, |cell| {
			let cell_rowid = cell.rowid.context("expected cell with `rowid`")?;
			MatchOrdering::try_from(rowid.cmp(&cell_rowid), || {
				if cell.payload.is_empty() { return Ok(None) }
				let fields = cell.parse_record_fields()
					.context("parsing cell records")?
					.collect::<Result<Vec<_>, _>>()?;
				Ok(Some(table_record_vals(rowid, &fields,
					&sql_select_col_idxs, &sql_create_table_cols)?))
			})
		}).unwrap().unwrap().into_value();
		if let Some(row) = row {
			println!("{row}");
		}
	}

	Ok(())
}
