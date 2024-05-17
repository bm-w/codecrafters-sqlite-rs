use std::borrow::Cow;

use anyhow::Context as _;
use itertools::Itertools as _;

mod page;
mod records;
mod schema;
mod sql;
mod util;
mod vals;

use page::Page;
use sql::{SqlCreateColumn, SqlSelect, SqlSelectColumn, SqlSelectCount, SqlWhere, SqlWhereOp};
use util::LiftFallibleIteratorExt as _;



fn main() -> anyhow::Result<()> {
	// Parse arguments
	let mut args = std::env::args();
	let database_path = args.nth(1).context("expected <database path> argument")?;
	let command = args.next().context("expected <command> argument")?;

	match command.as_str() {
		".dbinfo" => {
			let (page, _file) = Page::parse_first(database_path.as_ref())
				.context("parsing first page")?;
			let header = &page.file_header.expect("first page has file header");

			println!("database page size: {}", header.page_size);

			// The `sqlite_schema` table contains one cell per table
			println!("number of tables: {}", page.num_cells);
		}
		".tables" => {
			let (page, _file) = Page::parse_first(database_path.as_ref())
				.context("parsing first page")?;

			let mut schema_records = page
				.parse_schema_records()
				.lift_fallible_iterator()
				.collect::<anyhow::Result<Vec<_>>>()?;
			schema_records.sort_by_key(|sr| sr.id);

			println!("{}", schema_records.iter().map(|sr| sr.name.as_ref()).join(" "));
		}
		sql if !sql.starts_with('.') => {
			let sql_select = SqlSelect::try_from(sql).context("parsing SQL `SELECT` query")?;

			let (first_page, mut file) = Page::parse_first(database_path.as_ref())
				.context("parsing first page")?;
			let file_header = first_page.file_header.as_ref().expect("first page has file header");

			let schema_record = first_page
				.parse_schema_records()
				.lift_fallible_iterator()
				.find(|r| !r.as_ref().is_ok_and(|sr| sr.name != sql_select.table_name))
				.with_context(|| format!("no matching schema record for table {:?}", sql_select.table_name))?
				.context("parsing schema records")?;

			let table_page = Page::parse(&mut file, file_header, schema_record.root_page)
				.with_context(|| format!("parsing page #{}", schema_record.root_page))?;

			if sql_select.columns.len() == 1
				&& matches!(sql_select.columns[0], SqlSelectColumn::Count(SqlSelectCount::All)) {
				println!("{}", table_page.num_cells);
			} else {
				let sql_create_cols = SqlCreateColumn::parse(sql_select.table_name, &schema_record.sql)
					.with_context(|| format!("parsing SQL create statement for table {:?}", sql_select.table_name))?
					.collect::<anyhow::Result<Vec<_>>>()
					.with_context(|| format!("parsing columns for table {:?}", sql_select.table_name))?;

				let indexed_cols = sql_select.columns.iter()
					.map(|sel_col| match sel_col {
						&SqlSelectColumn::Ident(col_name) =>
							sql_create_cols.iter()
								.enumerate()
								.find(|(_, c)| c.name == col_name)
								.with_context(|| format!("no matching column for name {col_name}")),
						_ => anyhow::bail!("expected column name"),
					})
					.collect::<anyhow::Result<Vec<_>>>()
					.context("finding `SELECT` column indices")?;

				let indexed_where_col = sql_select.wher.as_ref().map(|wher| -> anyhow::Result<_> {
					let col_idx = sql_create_cols.iter()
						.position(|col| col.name == wher.left_col_name)
						.with_context(|| format!("no matching column for name {}", wher.left_col_name))?;
					Ok((col_idx, wher))
				}).transpose()?;

				let rows = table_page
					.scan_records_fields(&mut file, file_header, |cell, record_fields| {
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
								return Ok(None);
							}
						}

						let vals = indexed_cols.iter()
							.map(|(col_idx, col)| Ok(if col.is_rowid_alias {
								Cow::from(format!("{}", cell.rowid))
							} else {
								let &(typ, val) = fields.get(*col_idx)
									.context("expected valid column index")?;
								anyhow::ensure!(typ.is_text(),
									"expected text type for column {:?}, found type {typ}",
									col.name);
								Cow::from(std::str::from_utf8(val)
									.with_context(|| format!("expected UTF-8 string for column {:?} value",
										col.name))?)
							}))
							.collect::<anyhow::Result<Vec<_>>>()?;
						Ok(Some(vals.join("|")))
					})
					.lift_fallible_iterator();

				for (idx, row) in rows.enumerate() {
					if let Some(row) = row.with_context(|| format!("reading row {idx}"))? {
						println!("{row}");
					}
				}
			}
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
}
