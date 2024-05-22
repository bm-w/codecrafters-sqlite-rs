use std::{fs::File, io::prelude::*, num::NonZeroU64, path::Path};
use anyhow::Context as _;


pub(crate) struct Header {
	pub(crate) page_size: usize,
	pub(crate) _num_pages: usize,
	pub(crate) buf: [u8; 100]
}

impl Header {
	pub(crate) fn parse(database_path: &Path) -> anyhow::Result<(Self, File)> {
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

		let _freelist_page = u32::from_be_bytes(buf[32..36].try_into().unwrap());

		Ok((Header { page_size, _num_pages: num_pages, buf }, file))
	}
}


pub(crate) enum PageKind {
	InteriorIndex { rightmost_page: NonZeroU64 },
	#[allow(dead_code)]
	InteriorTable { rightmost_page: NonZeroU64 },
	LeafIndex,
	LeafTable,
}

impl PageKind {
	pub(crate) fn is_interior(&self) -> bool {
		matches!(self, Self::InteriorIndex { .. } | Self::InteriorTable { .. })
	}

	pub(crate) fn header_len(&self) -> usize {
		if self.is_interior() { 12 } else { 8 }
	}

	pub(crate) fn is_index(&self) -> bool {
		matches!(self, Self::InteriorIndex { .. } | Self::LeafIndex)
	}

	pub(crate) fn is_table(&self) -> bool {
		matches!(self, Self::InteriorTable { .. } | Self::LeafTable)
	}
}

pub(crate) struct Page {
	pub(crate) num: NonZeroU64,
	pub(crate) file_header: Option<Header>,
	pub(crate) kind: PageKind,
	pub(crate) num_cells: usize,
	pub(crate) _cells_offset: usize,
	pub(crate) buf: Box<[u8]>,
}

impl Page {
	pub(crate) fn parse_first(database_path: &Path) -> anyhow::Result<(Self, File)> {
		let (file_header, mut file) = Header::parse(database_path).context("parsing header")?;

		// SAFETY: 1 > 0
		let num = unsafe { NonZeroU64::new_unchecked(1) };
		let mut page = Self::parse(&mut file, &file_header, num)?;
		page.file_header = Some(file_header);

		anyhow::ensure!(matches!(page.kind, PageKind::LeafTable),
			"expected first page to be leaf-table");

		Ok((page, file))
	}

	pub(crate) fn parse(file: &mut File, file_header: &Header, num: NonZeroU64)
	-> anyhow::Result<Self> {
		let is_first_page = num.get() == 1; // Page numbers are 1-indexed
		let file_header_len = if is_first_page { file_header.buf.len() } else { 0 };

		let page_byte_offset = file_header.page_size * (num.get() as usize - 1) + file_header_len;
		file.seek(std::io::SeekFrom::Start(page_byte_offset as u64))
			.with_context(|| format!("seeking to file offset {page_byte_offset} for page #{num}"))?;

		let mut buf = vec![0u8; file_header.page_size - file_header_len].into_boxed_slice();
		file.read_exact(&mut buf)
			.with_context(|| format!("reading page #{num} from file offset {page_byte_offset}"))?;

		let kind = PageKind::try_from((buf[0], buf.as_ref())).context("parsing page kind")?;
		let num_cells = u16::from_be_bytes(buf[3..5].try_into().unwrap()) as usize;
		let cells_offset = u16::from_be_bytes(buf[5..7].try_into().unwrap()) as usize;

		Ok(Page { num, file_header: None, kind, num_cells, _cells_offset: cells_offset, buf })
	}

	pub(crate) fn rightmost_page(&self) -> Option<NonZeroU64> {
		match self.kind {
			| PageKind::InteriorIndex { rightmost_page }
			| PageKind::InteriorTable { rightmost_page }
				=> Some(rightmost_page),
			_ => None,
		}
	}

	// pub(crate) fn 
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum PageKindError {
	#[error("invalid page kind `0x{0:02x}`")]
	Invalid(u8),
	#[error("invalid zero value for right-most page")]
	ZeroRightmostPage,
	#[error("invalid page length: expected at least {expected} bytes, but found {found}")]
	BufLen { expected: usize, found: usize },
}

impl TryFrom<(u8, &[u8])> for PageKind {
	type Error = PageKindError;
	fn try_from((byte, page_buf): (u8, &[u8])) -> Result<Self, Self::Error> {
		match byte {
			0x02 | 0x05 => {
				if page_buf.len() < 12 {
					return Err(PageKindError::BufLen { expected: 12, found: page_buf.len() })
				}
				let rightmost_page = u32::from_be_bytes(page_buf[8..12].try_into().unwrap()) as u64;
				let rightmost_page = rightmost_page.try_into()
					.map_err(|_| PageKindError::ZeroRightmostPage)?;
				Ok(if byte == 0x02 { Self::InteriorIndex { rightmost_page } }
					else { Self::InteriorTable { rightmost_page } })
			}
			0x0a => Ok(Self::LeafIndex),
			0x0d => Ok(Self::LeafTable),
			_ => Err(PageKindError::Invalid(byte))
		}
	}
}
