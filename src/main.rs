use std::{
	borrow::Cow,
	fs::File,
	io::prelude::*,
	iter::FusedIterator,
	num::{NonZeroU64, NonZeroUsize},
	path::Path
};

use anyhow::Context as _;
use itertools::Itertools as _;


fn main() -> anyhow::Result<()> {
	// Parse arguments
	let mut args = std::env::args();
	let database_path = args.nth(1).context("expected <database path> argument")?;
	let command = args.next().context("expected <command> argument")?;

	match command.as_str() {
		".dbinfo" => {
			let (page, _file) = parse_first_page(database_path.as_ref())
				.context("parsing first page")?;
			let header = &page.file_header.expect("first page has file header");

			println!("database page size: {}", header.page_size);

			// The `sqlite_schema` table contains one cell per table
			println!("number of tables: {}", page.num_cells);
		}
		".tables" => {
			let (page, _file) = parse_first_page(database_path.as_ref())
				.context("parsing first page")?;

			let mut schema_records = page
				.parse_schema_records()
				.lift_fallible_iterator()
				.collect::<anyhow::Result<Vec<_>>>()?;
			schema_records.sort_by_key(|sr| sr.id);

			println!("{}", schema_records.iter().map(|sr| sr.name.as_ref()).join(" "));
		}
		sql if !sql.starts_with('.') => {
			let sql_select = parse_sql_select(sql).context("parsing SQL `SELECT` query")?;

			let (first_page, mut file) = parse_first_page(database_path.as_ref())
				.context("parsing first page")?;
			let file_header = first_page.file_header.as_ref().expect("first page has file header");

			let schema_record = first_page
				.parse_schema_records()
				.lift_fallible_iterator()
				.find(|r| !r.as_ref().is_ok_and(|sr| sr.name != sql_select.table_name))
				.with_context(|| format!("no matching schema record for table {:?}", sql_select.table_name))?
				.context("parsing schema records")?;

			let table_page = parse_page(&mut file, file_header, schema_record.root_page)
				.with_context(|| format!("parsing page {}", schema_record.root_page))?;

			if sql_select.columns.len() == 1
				&& matches!(sql_select.columns[0], SqlSelectColumn::Count(SqlSelectCount::All)) {
				println!("{}", table_page.num_cells);
			} else {
				let sql_create_cols = parse_sql_create(sql_select.table_name, &schema_record.sql)
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

				let mut rows = table_page
					.parse_records_fields()
					.lift_fallible_iterator()
					.filter_map(|r| r.and_then(|(cell, fields)| Ok(Some((cell.rowid, {
						// TODO: Avoid allocating?
						let fields = fields.collect::<anyhow::Result<Vec<_>>>()?;

						if let Some((
							col_idx,
							&SqlWhere { op: SqlWhereOp::Eq, left_col_name, right_str_operand }
						)) = indexed_where_col {
							let &(typ, val) = fields.get(col_idx)
								.context("expected valid column index")?;
							anyhow::ensure!(is_text_type(typ),
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
								anyhow::ensure!(is_text_type(typ),
									"expected text type for column {:?}, found type {typ}",
									col.name);
								Cow::from(std::str::from_utf8(val)
									.with_context(|| format!("expected UTF-8 string for column {:?} value",
										col.name))?)
							}))
							.collect::<anyhow::Result<Vec<_>>>()?;
						vals.join("|")
					})))).transpose())
					.collect::<anyhow::Result<Vec<_>>>()?;
				rows.sort_by_key(|(rowid, _)| *rowid);

				println!("{}", rows.into_iter()
					.map(|(_, val)| val)
					.join("\n"))
			}
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
}


enum SqlWhereOp { Eq }

struct SqlWhere<'a> {
	op: SqlWhereOp,
	left_col_name: &'a str,
	right_str_operand: &'a str,
}

enum SqlSelectCount { All }

enum SqlSelectColumn<'a> {
	Count(SqlSelectCount),
	Ident(&'a str),
}

struct SqlSelect<'a> {
	columns: Box<[SqlSelectColumn<'a>]>,
	table_name: &'a str,
	wher: Option<SqlWhere<'a>>,
}

fn parse_sql_select(sql: &str) -> anyhow::Result<SqlSelect<'_>> {
	let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	anyhow::ensure!(sql.len() >= 6 && sql[..6].eq_ignore_ascii_case("select"),
		"expected `SELECT` token");
	let sql = sql[6..].strip_prefix(|c: char| c.is_ascii_whitespace())
		.context("expected ASCII whitespace after `SELECT` token")?;

	let (columns, sql) = {
		let mut acc = Vec::new();
		let mut sql = sql;
		loop {
			if let Some((count, sql_rest)) = maybe_parse_sql_select_count(sql)
				.context("parsing `COUNT(*)` term")? {
				acc.push(SqlSelectColumn::Count(count));
				sql = sql_rest;
			} else {
				let (ident, sql_rest) = parse_sql_ident(sql)
					.context("parsing column identifier")?;
				acc.push(SqlSelectColumn::Ident(ident));
				sql = sql_rest;
			}
			let end_sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
			if !end_sql.starts_with(',') { break }
			sql = &end_sql[1..].trim_start_matches(|c: char| c.is_ascii_whitespace());
		}
		(acc.into(), sql)
	};

	let sql = sql.strip_prefix(|c: char| c.is_ascii_whitespace())
		.context("expected ASCII whitespace after `<select expression>` token")?;
	anyhow::ensure!(sql.len() >= 4 && sql[..4].eq_ignore_ascii_case("from"),
		"expected `FROM` token");
	let sql = sql[4..].strip_prefix(|c: char| c.is_ascii_whitespace())
		.context("expected ASCII whitespace after `FROM` token")?;
	let (table_name, sql) = parse_sql_ident(sql)
		.context("parsing table identifier")?;

	let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());

	let (wher, sql) = if sql.len() >= 5 && sql[..5].eq_ignore_ascii_case("where") {
		let sql = sql[5..].trim_start_matches(|c: char| c.is_ascii_whitespace());
		let (left_col_name, sql) = parse_sql_ident(sql)
			.context("parsing where left column identifier")?;
		let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
		let sql = sql.strip_prefix('=').context("expected `=` token (only supported operator)")?;
		let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
		let sql = sql.strip_prefix('\'').context("expected `'` token (only string operands supported)")?;
		// TODO: Consider escaped quotes "\'"
		let (right_str_operand, sql) = sql.split_once('\'').context("expected closing '\'' token")?;
		(Some(SqlWhere { op: SqlWhereOp::Eq, left_col_name, right_str_operand }), sql)
	} else {
		(None, sql)
	};

	anyhow::ensure!(sql.trim_start_matches(|c: char| c.is_ascii_whitespace()).is_empty(),
		"expected end of SQL `SELECT` query");

	Ok(SqlSelect { columns, table_name, wher })
}

fn parse_sql_ident(sql: &str) -> anyhow::Result<(&str, &str)> {
	anyhow::ensure!(sql.starts_with(|c: char| c.is_ascii_alphabetic()),
		"expected identifier to start with ASCII alphabetic character");
	let end = sql.bytes().skip(1)
		.position(|b| b != b'_' && !char::from(b).is_ascii_alphanumeric())
		.unwrap_or(sql.len() - 1) + 1;
	Ok(sql.split_at(end))
}

fn maybe_parse_sql_select_count(sql: &str) -> anyhow::Result<Option<(SqlSelectCount, &str)>> {
	if sql.len() < 5 || !sql[..5].eq_ignore_ascii_case("count") { return Ok(None) }
	let sql = sql[5..].trim_start_matches(|c: char| c.is_ascii_whitespace());
	let Some(sql) = sql.strip_prefix('(') else { return Ok(None) };
	let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	let sql = sql.strip_prefix('*').context("expected `*` (asterisk)")?;
	let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	let sql = sql.strip_prefix(')').context("expected `)` (right parenthesis)")?;
	Ok(Some((SqlSelectCount::All, sql)))
}


struct SqlCreateColumn<'a> {
	name: Cow<'a, str>,
	is_rowid_alias: bool,
	// TODO: Etc.
}

fn parse_sql_create<'a>(table_name: &'a str, mut sql: &'a str)
-> anyhow::Result<impl Iterator<Item = anyhow::Result<SqlCreateColumn<'a>>>> {
	sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	anyhow::ensure!(sql.len() >= 6 && sql[..6].eq_ignore_ascii_case("create"),
		"expected `CREATE` token");
	sql = sql[6..].trim_start_matches(|c: char| c.is_ascii_whitespace());
	anyhow::ensure!(sql.len() >= 5 && sql[..5].eq_ignore_ascii_case("table"),
		"expected `TABLE` token");
	sql = sql[5..].trim_start_matches(|c: char| c.is_ascii_whitespace());
	sql = sql.strip_prefix(table_name).context("expected table name token")?;
	sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	sql = sql.strip_prefix('(').context("expected '(' (left parenthesis) token")?;
	sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());

	Ok(std::iter::from_fn(move || {
		if sql.is_empty() { return None }

		fn inner<'a>(sql: &mut &'a str) -> anyhow::Result<SqlCreateColumn<'a>> {
			let (col_name, rest) = sql.split_once(|c: char| c.is_ascii_whitespace())
				.context("expected <column name> token followed by whitespace")?;
			let end = rest.find(|c: char| c == ',' || c == ')')
				.context("expected ',' (comma, column delimiter) \
					or ')' (right parenthesis) token")?;
			// TODO: More robust parsing
			let is_rowid = rest[..end].to_ascii_lowercase().contains("integer primary key");
			*sql = rest[end + 1..].trim_start_matches(|c: char| c.is_ascii_whitespace());
			Ok(SqlCreateColumn { name: col_name.into(), is_rowid_alias: is_rowid })
		}
		Some(inner(&mut sql))
	}))
}

#[allow(dead_code)]
struct SchemaRecord<'a> {
	id: NonZeroU64,
	name: Cow<'a, str>,
	// TODO: `table_name: Cow<'a, str>`
	root_page: NonZeroU64,
	sql: Cow<'a, str>,
}

impl<'a> TryFrom<(Cell<'a>, RecordFields<'a>)> for SchemaRecord<'a> {
	type Error = anyhow::Error;
	fn try_from(cell_record_fields: (Cell<'a>, RecordFields<'a>)) -> Result<Self, Self::Error> {
		let (cell, fields) = cell_record_fields;
		let mut fields = fields.enumerate()
			.map(|(i, r)| r.with_context(|| format!("parsing field {i}")));

		// `type` column
		let (typ, table_type) = fields.next().context("expected `type` column field")??;
		anyhow::ensure!(is_text_type(typ) && table_type == b"table",
			"expected \"table\" text for `type` value");

		// `name` column
		let (typ, name) = fields.next().context("expected `name` column field")??;
		anyhow::ensure!(is_text_type(typ), "expected text type for `name` column");
		let name = std::str::from_utf8(name).expect("reading `name` value as UTF-8");

		// `rootpage` column (skipping `tbl_name`)
		let (typ, root_page) = fields.nth(1).context("expected `rootpage` column field")??;
		let (_, root_page) = int_val(typ, root_page)
			.context("reading `rootpage` integer value")?;
		let root_page: u64 = root_page.context("expected non-NULL `rootpage` value")?
			.try_into().context("expected non-negative `rootpage` value")?;
		let root_page = root_page.try_into().context("expected non-zero `rootpage` column value")?;

		let (typ, sql) = fields.next().context("expected `sql` column field")??;
		anyhow::ensure!(is_text_type(typ), "expected text type for `sql` column");
		let sql = std::str::from_utf8(sql).expect("reading `sql` value as UTF-8");

		fields.for_each(drop); // Should be empty, but just in case

		Ok(SchemaRecord {
			id: cell.rowid,
			name: name.into(),
			root_page,
			sql: sql.into(),
		})
	}
}

impl Page {
	fn parse_schema_records(&self)
	-> anyhow::Result<impl Iterator<
		Item = anyhow::Result<SchemaRecord>
	>> {
		Ok(self.parse_records_fields()
			.context("parsing page records’ fields")?
			.map(|cell_record_fields| SchemaRecord::try_from(cell_record_fields?))
			.take_while_inclusive(Result::is_ok))
	}

	fn parse_records_fields(&self)
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

struct RecordFields<'a> {
	cell: Cell<'a>,
	record_header_len: usize,
	type_offset: usize,
	val_offset: usize,
	idx: usize,
}

impl<'a> RecordFields<'a> {
	fn next_field(&mut self) -> anyhow::Result<(i64, &'a [u8])> {
		let (type_size, typ) = parse_varint(&self.cell.payload[self.type_offset..])
			.context("parsing type")?;
		let val_size = val_len(typ).context("computing value length")?;
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
	type Item = anyhow::Result<(i64, &'a [u8])>;
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
		let (size, record_header_len) = parse_varint(self.payload)
			.context("parsing `varint` as record header length")?;
		let record_header_len = record_header_len as usize;
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

struct PageCells<'a> {
	page: &'a Page,
	cell_offset: usize,
	cell_idxs: std::ops::Range<usize>,
}

impl<'a> PageCells<'a> {
	fn next_cell(&mut self) -> anyhow::Result<Cell<'a>> {
		let (size, payload_len) = parse_varint(&self.page.buf[self.cell_offset..])
			.context("parsing first cell’s payload size `varint`")?;
		let payload_len = payload_len as usize;

		let rowid_offset = self.cell_offset + size;
		let (size, rowid) = parse_varint(&self.page.buf[rowid_offset..])
			.context("parsing first cell’s payload size `varint`")?;
		let rowid: u64 = rowid.try_into().context("expected non-negative `rowid`")?;
		let rowid = rowid.try_into().context("expected non-zero `rowid`")?;

		let payload_offset = rowid_offset + size;

		self.cell_offset = payload_offset + payload_len;

		let payload = &self.page.buf[payload_offset..payload_offset + payload_len];
		Ok(Cell { rowid, payload })
	}
}

impl<'a> Iterator for PageCells<'a> {
	type Item = anyhow::Result<Cell<'a>>;
	fn next(&mut self) -> Option<Self::Item> {
		let cell_idx = self.cell_idxs.next()?;
		let next = self.next_cell()
			.with_context(|| format!("parsing page cell {cell_idx}"));
		if next.is_err() { self.cell_idxs.by_ref().for_each(drop) } // Fuse
		Some(next)
	}
}

impl FusedIterator for PageCells<'_> {}

impl Page {
	fn parse_cells(&self) -> anyhow::Result<PageCells> {
		let file_header_len = self.file_header.as_ref().map(|h| h.buf.len()).unwrap_or(0);
		let cell_offset = self.cells_offset - file_header_len;

		anyhow::ensure!(self.buf.len() >= cell_offset,
			"expected page size of at least {} bytes, but found {}",
			self.cells_offset,
			self.buf.len() + file_header_len);

		Ok(PageCells { page: self, cell_offset, cell_idxs: 0..self.num_cells })
	}
}

struct Cell<'a> {
	rowid: NonZeroU64,
	payload: &'a [u8],
}

fn int_val(typ: i64, buf: &[u8]) -> anyhow::Result<(usize, Option<i64>)> {
	fn try_arr<const LEN: usize>(buf: &[u8]) -> anyhow::Result<[u8; LEN]> {
		anyhow::ensure!(buf.len() >= LEN,
			"expected {LEN} byte{} reading a {}-bit twos-complement integer",
			if LEN == 1 { "" } else { "s" },
			LEN * 8);
		buf[..LEN].try_into()
			.with_context(|| format!("reading a {}-bit twos-complement integer", LEN * 8))
	}
	match typ {
		0 => Ok((0, None)),
		1 => Ok((1, Some(i8::from_be_bytes(try_arr(buf)?) as i64))),
		2 => Ok((2, Some(i16::from_be_bytes(try_arr(buf)?) as i64))),
		3 => anyhow::bail!("24-bit integers not implemented"),
		4 => Ok((4, Some(i32::from_be_bytes(try_arr(buf)?) as i64))),
		5 => anyhow::bail!("48-bit integers not implemented"),
		6 => Ok((8, Some(i64::from_be_bytes(try_arr(buf)?) as i64))),
		8 => Ok((0, Some(0))),
		9 => Ok((0, Some(1))),
		_ => anyhow::bail!("expected integer type")
	}
}

fn val_len(typ: i64) -> anyhow::Result<usize> {
	Ok(match typ {
		0..=4 => typ as usize,
		5 => 6,
		6 | 7 => 8,
		8 | 9 => 9,
		10 | 11 => anyhow::bail!("unexpected internal type"),
		_blob_typ if typ >= 12 && typ % 2 == 0 => (typ as usize - 12) / 2,
		text_typ if is_text_type(text_typ) => (typ as usize - 13) / 2,
		_ => anyhow::bail!("unexpected type {typ}"),
	})
}

fn is_text_type(typ: i64) -> bool { typ >= 13 && typ % 2 == 1 }

fn parse_varint(buf: &[u8]) -> anyhow::Result<(usize, i64)> {
	let mut bits = 0u64;
	let mut offset = 0;
	loop {
		anyhow::ensure!(buf.len() > offset, "expected longer buffer");
		if offset < 8 {
			if buf[offset] & 0b1000_0000 != 0 {
				bits <<= 7;
				bits |= (0b0111_1111 & buf[offset]) as u64;
				offset += 1;
			} else {
				bits <<= 7;
				bits |= buf[offset] as u64;
				break
			}
		} else {
			bits <<= 8;
			bits |= buf[offset] as u64;
			break
		}
	}
	Ok((offset + 1, unsafe { std::mem::transmute(bits) }))
}

struct Page {
	file_header: Option<Header>,
	num_cells: usize,
	cells_offset: usize,
	buf: Box<[u8]>,
}

fn parse_first_page(database_path: &Path) -> anyhow::Result<(Page, File)> {
	let (file_header, mut file) = parse_header(database_path).context("parsing header")?;
	let mut page = parse_page(&mut file, &file_header, unsafe { NonZeroU64::new_unchecked(1) })?;
	page.file_header = Some(file_header);

	Ok((page, file))
}

fn parse_page(file: &mut File, file_header: &Header, num: NonZeroU64) -> anyhow::Result<Page> {
	let is_first_page = num.get() == 1; // Page numbers are 1-indexed
	let file_header_len = if is_first_page { file_header.buf.len() } else { 0 };

	let num: NonZeroUsize = num.try_into()
		.expect("`u64` should fit in `usize` (assuming 64-bit arch)");
	let page_byte_offset = file_header.page_size * (num.get() - 1) + file_header_len;
	file.seek(std::io::SeekFrom::Start(page_byte_offset as u64))
		.with_context(|| format!("seeking to file offset {page_byte_offset} for page {num}"))?;

	let mut buf = vec![0u8; file_header.page_size - file_header_len].into_boxed_slice();
	file.read_exact(&mut buf)
		.with_context(|| format!("reading page {num} from file offset {page_byte_offset}"))?;

	assert_eq!(buf[0], 0x0d, "only supporting leaf table b-tree pages right now");
	let num_cells = u16::from_be_bytes(buf[3..5].try_into().unwrap()) as usize;
	let cells_offset = u16::from_be_bytes(buf[5..7].try_into().unwrap()) as usize;

	Ok(Page { file_header: None, num_cells, cells_offset, buf })
}

struct Header {
	page_size: usize,
	_num_pages: usize,
	buf: [u8; 100]
}

fn parse_header(database_path: &Path) -> anyhow::Result<(Header, File)> {
	let mut file = File::open(database_path)
		.with_context(|| format!("opening database at {database_path:?}"))?;
	let file_size = file.metadata().context("accesing file metadata")?.len() as usize;

	let mut buf = [0; 100];

	file.read_exact(&mut buf[..16]).context("reading first 16 bytes of header")?;
	anyhow::ensure!(&buf[..16] == b"SQLite format 3\0",
		"expected header to start with string \"SQLite format 3\0\"");

	file.read_exact(&mut buf[16..]).context("reading remaining bytes of header")?;

	let page_size = u16::from_be_bytes(buf[16..18].try_into().unwrap()) as usize;
	let num_pages = u32::from_be_bytes(buf[28..32].try_into().unwrap()) as usize;
	anyhow::ensure!(file_size == num_pages * page_size,
		"expected file size of {} bytes, but found {file_size}", num_pages * page_size);

	let freelist_page = u32::from_be_bytes(buf[32..36].try_into().unwrap());
	anyhow::ensure!(freelist_page == 0, "expected no freelist page");

	Ok((Header { page_size, _num_pages: num_pages, buf }, file))
}


// - Utilities

// TODO: Better name?
trait LiftFallibleIteratorExt {
	type IteratorItem;
	fn lift_fallible_iterator(self) -> impl Iterator<Item = Self::IteratorItem>;
}

impl<I, T, E> LiftFallibleIteratorExt for Result<I, E>
where I: Iterator<Item = Result<T, E>> {
	type IteratorItem = Result<T, E>;
	fn lift_fallible_iterator(self) -> impl Iterator<Item = Self::IteratorItem> {
		match self {
			Ok(it) => itertools::Either::Left(it),
			Err(err) => itertools::Either::Right(std::iter::once(Err(err))),
		}
	}
}
