use std::fs::File;
use std::io::prelude::*;

use anyhow::Context as _;


fn main() -> anyhow::Result<()> {
	// Parse arguments
	let mut args = std::env::args();
	let database_path = args.nth(1).context("expected <database path> argument")?;
	let command = args.next().context("expected <command> argument")?;

	match command.as_str() {
		".dbinfo" => {
			let mut file = File::open(&database_path)
				.with_context(|| format!("opening database at {database_path:?}"))?;
			let file_size = file.metadata().context("accesing file metadata")?.len() as usize;
			let mut header = [0; 100];

			file.read_exact(&mut header[..16]).context("reading first 16 bytes of header")?;
			anyhow::ensure!(&header[..16] == b"SQLite format 3\0",
				"expected header to start with string \"SQLite format 3\0\"");

			file.read_exact(&mut header[16..]).context("reading remaining bytes of header")?;

			let page_size = u16::from_be_bytes(header[16..18].try_into().unwrap()) as usize;
			println!("database page size: {}", page_size);

			let num_pages = u32::from_be_bytes(header[28..32].try_into().unwrap()) as usize;
			anyhow::ensure!(file_size == num_pages * page_size,
				"expected file size of {} but found {file_size}", num_pages * page_size);

			let freelist_page = u32::from_be_bytes(header[32..36].try_into().unwrap());
			anyhow::ensure!(freelist_page == 0, "expected no freelist page");

			let mut page = vec![0u8; page_size];
			page[..100].copy_from_slice(&header[..]);
			file.read_exact(&mut page[100..])
				.context("reading rest of first page")?;

			anyhow::ensure!(page[100] == 0x0d,
				"expected first page to be a leaf table b-tree page");
			let num_cells = u16::from_be_bytes(page[103..105].try_into().unwrap());
			// The `sqlite_schema` table contains one cell per table
			println!("number of tables: {num_cells}");
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
}
