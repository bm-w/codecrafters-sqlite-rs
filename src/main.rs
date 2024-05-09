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
			let mut header = [0; 18];
			file.read_exact(&mut header)?;

			// The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
			let page_size = u16::from_be_bytes(header[16..18].try_into().unwrap());
			println!("database page size: {}", page_size);
		}
		_ => anyhow::bail!("unsupported command {command:?}"),
	}

	Ok(())
}
