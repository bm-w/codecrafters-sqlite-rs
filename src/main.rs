use std::{fs::File, io::prelude::*, path::Path};

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

			let page_size = u16::from_be_bytes(page[16..18].try_into().unwrap()) as usize;
			println!("database page size: {}", page_size);

			anyhow::ensure!(page[100] == 0x0d,
				"expected first page to be a leaf table b-tree page");
			let num_cells = u16::from_be_bytes(page[103..105].try_into().unwrap()) as usize;
			// The `sqlite_schema` table contains one cell per table
			println!("number of tables: {num_cells}");
		}
		".tables" => {
			let (page, _file) = read_first_page(database_path.as_ref())
				.context("reading first page")?;

			let num_cells = u16::from_be_bytes(page[103..105].try_into().unwrap()) as usize;
			let cells_offset = u16::from_be_bytes(page[105..107].try_into().unwrap()) as usize;

			let mut cell_offset = cells_offset;
			let mut names = Vec::new();

			for _ in 0..num_cells {
				let (size, payload_len) = parse_varint(&page[cell_offset..cell_offset + 9])
					.context("parsing first cell’s payload size `varint`")?;
				let payload_len = payload_len as usize;

				let rowid_offset = cell_offset + size;
				let (size, rowid) = parse_varint(&page[rowid_offset..rowid_offset + 9])
					.context("parsing first cell’s payload size `varint`")?;

				let record_offset = rowid_offset + size;
				let (size, record_header_len) = parse_varint(&page[record_offset..record_offset + 9])
					.context("parsing first cell’s payload size `varint`")?;
				let record_header_len = record_header_len as usize;

				let col_types_offset = record_offset + size;

				// `type` column (text "table" of length (23 - 13) / 2 == 5)
				anyhow::ensure!(page[col_types_offset] == 23, "expected text type of length 5");

				// `name` column
				let name_col_typ_offset = col_types_offset + 1;
				let (_size, typ) = parse_varint(&page[name_col_typ_offset..name_col_typ_offset + 9])
					.context("parsing `name` column type")?;
				let name_len = text_typ_len(typ).context("parsing `name` column text length")?;

				let record_body_offset = record_offset + record_header_len;

				let name_val_offset = record_body_offset + 5; // 5 is length of type column value "table"
				let name = &page[name_val_offset..name_val_offset + name_len];
				let name = std::str::from_utf8(name)
					.context("reading table name as UTF-8")?;

				names.push((rowid, name));

				cell_offset = record_offset + payload_len;
			}

			names.sort_by_key(|(rowid, _)| *rowid);

			println!("{}", names.iter().map(|(_, name)| *name).join(" "));
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
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

fn read_first_page(database_path: &Path) -> anyhow::Result<(Vec<u8>, File)> {
	let (header, mut file) = read_header(database_path).context("reading header")?;
	let file_size = file.metadata().context("accesing file metadata")?.len() as usize;

	let page_size = u16::from_be_bytes(header[16..18].try_into().unwrap()) as usize;
	let num_pages = u32::from_be_bytes(header[28..32].try_into().unwrap()) as usize;
	anyhow::ensure!(file_size == num_pages * page_size,
		"expected file size of {} but found {file_size}", num_pages * page_size);

	let freelist_page = u32::from_be_bytes(header[32..36].try_into().unwrap());
	anyhow::ensure!(freelist_page == 0, "expected no freelist page");

	let mut page = vec![0u8; page_size];
	page[..100].copy_from_slice(&header[..]);
	file.read_exact(&mut page[100..])
		.context("reading rest of first page")?;

	Ok((page, file))
}

fn read_header(database_path: &Path) -> anyhow::Result<([u8; 100], File)> {
	let mut file = File::open(database_path)
		.with_context(|| format!("opening database at {database_path:?}"))?;
	let mut header = [0; 100];

	file.read_exact(&mut header[..16]).context("reading first 16 bytes of header")?;
	anyhow::ensure!(&header[..16] == b"SQLite format 3\0",
		"expected header to start with string \"SQLite format 3\0\"");

	file.read_exact(&mut header[16..]).context("reading remaining bytes of header")?;

	Ok((header, file))
}
