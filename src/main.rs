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
			let (page, _file) = read_first_page(database_path.as_ref())
				.context("reading first page")?;
			let header = &page.file_header.expect("first page has file header");

			println!("database page size: {}", header.page_size);

			// The `sqlite_schema` table contains one cell per table
			println!("number of tables: {}", page.num_cells);
		}
		".tables" => {
			let (page, _file) = read_first_page(database_path.as_ref())
				.context("reading first page")?;

			let mut schema_records = parse_schema_records(&page)
				.collect::<anyhow::Result<Vec<_>>>()?;
			schema_records.sort_by_key(|sr| sr.id);

			println!("{}", schema_records.iter().map(|sr| sr.name.as_ref()).join(" "));
		}
		sql if command[..7].to_ascii_lowercase() == "select " => {
			let sql = sql.to_lowercase();
			let table = sql
				.strip_prefix("select count(*) from ")
				.and_then(|t| t.ends_with(|c: char| c.is_ascii_alphanumeric()).then_some(t))
				.context("expected `SELECT COUNT(*) FROM <table>` argument")?;

			let (first_page, mut file) = read_first_page(database_path.as_ref())
				.context("reading first page")?;
			let file_header = first_page.file_header.as_ref().expect("first page has file header");

			let schema_record = parse_schema_records(&first_page)
				.find(|sr| !sr.as_ref().is_ok_and(|sr| sr.name != table))
				.context("no matching schema record")?
				.with_context(|| format!("retrieving schema record for table {table}"))?;

			let table_page = read_page(&mut file, file_header, schema_record.root_page)
				.with_context(|| format!("reading page {}", schema_record.root_page))?;

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

fn parse_schema_records(page: &Page)
-> impl Iterator<Item = anyhow::Result<SchemaRecord<'_>>> {
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

		let record_header_len_offset = rowid_offset + size;
		let (size, record_header_len) = parse_varint(&page.buf[record_header_len_offset..])
			.context("parsing first cell’s payload size `varint`")?;
		let record_header_len = record_header_len as usize;

		let types_offset = record_header_len_offset + size;

		// `type` column (text "table" of length (23 - 13) / 2 == 5)
		anyhow::ensure!(page.buf[types_offset] == 23, "expected text type of length 5");

		// `name` column
		let name_typ_offset = types_offset + 1;
		let (size, typ) = parse_varint(&page.buf[name_typ_offset..])
			.context("parsing `name` column type")?;
		let name_len = text_typ_len(typ).context("parsing `name` column text length")?;

		// `tbl_name` column
		let tbl_name_typ_offset = name_typ_offset + size;
		let (size, typ) = parse_varint(&page.buf[tbl_name_typ_offset..])
			.context("parsing `tbl_name` column type")?;
		let tbl_name_len = text_typ_len(typ).context("parsing `tbl_name` column text length")?;

		// `rootpage` column
		let rootpage_typ_offset = tbl_name_typ_offset + size;
		let (size, rootpage_typ) = parse_varint(&page.buf[rootpage_typ_offset..])
			.context("parsing `tbl_name` column type")?;

		// `sql` column
		let sql_typ_offset = rootpage_typ_offset + size;
		let (size, typ) = parse_varint(&page.buf[sql_typ_offset..])
			.context("parsing `tbl_name` column type")?;
		let sql_len = text_typ_len(typ).context("parsing `tbl_name` column text length")?;

		anyhow::ensure!(sql_typ_offset + size == record_header_len_offset + record_header_len,
			"expected record header length of {} but got {}",
			record_header_len_offset + record_header_len,
			sql_typ_offset + size);

		let record_body_offset = record_header_len_offset + record_header_len;

		let name_offset = record_body_offset + 5; // 5 is length of type column value "table"
		let name = std::str::from_utf8(&page.buf[name_offset..name_offset + name_len])
			.context("reading `name` column value as UTF-8")?;

		let rootpage_offset = name_offset + name_len + tbl_name_len;
		let (size, root_page) = int_val(rootpage_typ, &page.buf[rootpage_offset..])
			.context("reading `rootpage` column value")?;
		let root_page: u64 = root_page.context("expected non-NULL `rootpage` column value")?
			.try_into().context("expected non-negative `rootpage` column value")?;
		let root_page = root_page.try_into().context("expected non-zero `rootpage` column value")?;

		let sql_offset = rootpage_offset + size;
		let sql = std::str::from_utf8(&page.buf[sql_offset..sql_offset + sql_len])
			.context("reading `sql` column value as UTF-8")?;

		cell_offset = record_header_len_offset + payload_len;

		Ok(SchemaRecord {
			id: rowid,
			name: name.into(),
			root_page,
			sql: sql.into(),
		})
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
		_ => anyhow::bail!("expected an integer type")
	}
}

fn text_typ_len(typ: i64) -> anyhow::Result<usize> {
	anyhow::ensure!(typ >= 13 && typ % 2 == 1, "expected text type");
	Ok((typ as usize - 13) / 2)
}

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

fn read_first_page(database_path: &Path) -> anyhow::Result<(Page, File)> {
	let (file_header, mut file) = read_header(database_path).context("reading header")?;
	let mut page = read_page(&mut file, &file_header, unsafe { NonZeroU64::new_unchecked(1) })
		.context("reading first page")?;
	page.file_header = Some(file_header);

	anyhow::ensure!(page.buf[0] == 0x0d, "expected first page to be a leaf table b-tree page");

	Ok((page, file))
}

fn read_page(file: &mut File, file_header: &Header, num: NonZeroU64) -> anyhow::Result<Page> {
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

fn read_header(database_path: &Path) -> anyhow::Result<(Header, File)> {
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
