use std::{borrow::Cow, fs::File, io::prelude::*, num::{NonZeroU64, NonZeroUsize}, path::Path};

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

			let mut schema_records = parse_schema_records(&page)
				.collect::<anyhow::Result<Vec<_>>>()?;
			schema_records.sort_by_key(|sr| sr.id);

			println!("{}", schema_records.iter().map(|sr| sr.name.as_ref()).join(" "));
		}
		sql if !sql.starts_with('.') => {
			anyhow::ensure!(sql.len() >= 6 && sql[..6].eq_ignore_ascii_case("select"),
				"expected `SELECT` token");
			let sql = sql[6..].strip_prefix(|c: char| c.is_ascii_whitespace())
				.context("expected ASCII whitespace after `SELECT` token")?;
			let (select_expr, sql) = sql.split_once(|c: char| c.is_ascii_whitespace())
				.context("expected ASCII whitespace after `<select expression>` token")?;
			let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
			anyhow::ensure!(sql.len() >= 4 && sql[..4].eq_ignore_ascii_case("from"),
				"expected `FROM` token");
			let sql = sql[4..].strip_prefix(|c: char| c.is_ascii_whitespace())
				.context("expected ASCII whitespace after `FROM` token")?;
			let table_expr = sql.trim_end_matches(|c: char| c.is_ascii_whitespace());

			fn validate_sql_ident(token: &str) -> anyhow::Result<()> {
				anyhow::ensure!(token.starts_with(|c: char| c.is_ascii_alphabetic()),
					"expected identifier to start with ASCII alphabetic character");
				anyhow::ensure!(token.chars().skip(1)
					.all(|c: char| c.is_ascii_alphanumeric() || c == '_'),
					"epected identifier contains only ASCII alphanumerics or underscores");
				Ok(())
			}
			let is_all_rows_count = select_expr.eq_ignore_ascii_case("count(*)");
			anyhow::ensure!(is_all_rows_count, "expected `COUNT(*)` token");
			validate_sql_ident(table_expr)?;

			let (first_page, mut file) = parse_first_page(database_path.as_ref())
				.context("parsing first page")?;
			let file_header = first_page.file_header.as_ref().expect("first page has file header");

			let schema_record = parse_schema_records(&first_page)
				.find(|r| !r.as_ref().is_ok_and(|sr| sr.name != table_expr))
				.with_context(|| format!("no matching schema record for table {table_expr:?}"))?
				.context("parsing schema records")?;

			let table_page = parse_page(&mut file, file_header, schema_record.root_page)
				.with_context(|| format!("parsing page {}", schema_record.root_page))?;

			println!("{}", table_page.num_cells);
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
}


#[allow(dead_code)]
struct SchemaRecord<'a> {
	id: NonZeroU64,
	name: Cow<'a, str>,
	// TODO: `table_name: Cow<'a, str>`
	root_page: NonZeroU64,
	sql: Cow<'a, str>,
}

fn parse_schema_records(first_page: &Page)
-> impl Iterator<Item = anyhow::Result<SchemaRecord<'_>>> {
	parse_records_fields(first_page).map(|record_fields| {
		let (record, fields) = record_fields.context("parsing record fields")?;
		let mut fields = fields.map(|r| r.context("parsing field"));

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

		Ok(SchemaRecord {
			id: record.rowid,
			name: name.into(),
			root_page,
			sql: sql.into(),
		})
	})
}

fn parse_records_fields(page: &Page)
-> impl Iterator<Item = anyhow::Result<(
	Record<'_>,
	impl Iterator<Item = anyhow::Result<(i64, &[u8])>>
)>> {
	parse_records(page).map(|record| {
		let record = record.context("parsing record")?;

		let (size, record_header_len) = parse_varint(record.payload)
			.context("parsing first cell’s payload size `varint`")?;
		let record_header_len = record_header_len as usize;

		let mut type_offset = size;
		let mut val_offset = record_header_len;

		let payload = record.payload;

		Ok((record, std::iter::from_fn(move || {
			if type_offset == record_header_len { return None }

			fn inner<'a>(payload: &'a [u8], type_offset: &mut usize, val_offset: &mut usize)
			-> anyhow::Result<(i64, &'a [u8])> {
				let (size, typ) = parse_varint(&payload[*type_offset..]).context("parsing type")?;
				*type_offset += size;
				let size = val_len(typ).context("computing value length")?;
				let val = &payload[*val_offset..*val_offset + size];
				*val_offset += size;
				Ok((typ, val))
			}
			Some(inner(payload, &mut type_offset, &mut val_offset))
		})))
	})
}

struct Record<'a> {
	rowid: NonZeroU64,
	payload: &'a [u8],
}

fn parse_records(page: &Page) -> impl Iterator<Item = anyhow::Result<Record<'_>>> {
	let mut cell_offset = page.cells_offset
		- page.file_header.as_ref().map(|h| h.buf.len()).unwrap_or(0);

	(0..page.num_cells).map(move |_| {
		let (size, payload_len) = parse_varint(&page.buf[cell_offset..])
			.context("parsing first cell’s payload size `varint`")?;
		let payload_len = payload_len as usize;

		let rowid_offset = cell_offset + size;
		let (size, rowid) = parse_varint(&page.buf[rowid_offset..])
			.context("parsing first cell’s payload size `varint`")?;
		let rowid: u64 = rowid.try_into().context("expected non-negative `rowid`")?;
		let rowid = rowid.try_into().context("expected non-zero `rowid`")?;

		let payload_offset = rowid_offset + size;

		cell_offset = payload_offset + payload_len;

		Ok(Record { rowid, payload: &page.buf[payload_offset..payload_offset + payload_len] })
	})
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
	buf: Vec<u8>,
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

	let mut buf = vec![0u8; file_header.page_size - file_header_len];
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
		"expected file size of {} but found {file_size}", num_pages * page_size);

	let freelist_page = u32::from_be_bytes(buf[32..36].try_into().unwrap());
	anyhow::ensure!(freelist_page == 0, "expected no freelist page");

	Ok((Header { page_size, _num_pages: num_pages, buf }, file))
}
