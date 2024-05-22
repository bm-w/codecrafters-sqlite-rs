use std::{fs::File, iter::FusedIterator};

use anyhow::Context as _;

use crate::page::{Header, Page, PageKind};

use super::{Cell, PageCells, PageCellsState};


pub(in super) type StackElem = (
	// To “rebuild” the interior index cell after iterating through its left child
	Option<(usize, std::ops::Range<*const u8>)>,
	Page,
	// `None` signals time for the interior page’s right-most pointer
	Option<PageCellsState>,
);

pub(crate) enum Ended { Failed, Completed }

pub(crate) struct ScanRecords<'a, F> {
	pub(super) file: &'a mut File,
	pub(super) file_header: &'a Header,
	pub(super) root_page: &'a Page,
	pub(super) root_state: Option<PageCellsState>,
	pub(super) stack: Vec<StackElem>,
	pub(super) transform: F,
	pub(super) ended: Option<Ended>, // Fuse
}

impl<F, T> Iterator for ScanRecords<'_, F>
where F: for<'a> Fn(&'a Cell<'a>) -> anyhow::Result<Option<T>> {
	type Item = anyhow::Result<T>;
	fn next(&mut self) -> Option<Self::Item> {
		if self.ended.is_some() { return None }

		let (page, state) = self.stack.last_mut()
			.map(|(_, ref page, state)| (page, state))
			.unwrap_or((self.root_page, &mut self.root_state));
		let page_num = page.num;

		macro_rules! push_page {
			($tree_type:literal, $is_tree_type:ident, $page_num:ident,
				$interior_index_cell:expr, $pointer:tt
			) => { {
				let page_state = Page::parse(self.file, self.file_header, $page_num)
					.with_context(|| format!("parsing {} page #{}", $tree_type, $page_num))
					.and_then(|page| {
						anyhow::ensure!(page.num == $page_num, "expected page #{}, \
							but found #{}", $page_num, page.num);
						anyhow::ensure!(page.kind.$is_tree_type(), "expected {} page", $tree_type);
						let PageCells { state, ..} = page.cells();
						Ok((page, state))
					})
					.with_context(|| format!("parsing child {} page \
						pointed to by {}", $tree_type, $pointer));
				match page_state {
					Ok((page, state)) => {
						self.stack.push(($interior_index_cell, page, Some(state)));
						return self.next()
					}
					Err(err) => {
						self.ended = Some(Ended::Failed);
						(err, concat!("interior ", $tree_type))
					}
				}
			} }
		}

		macro_rules! process_cell {
			($cell_page:ident, $cell_idx:ident, $cell:ident, $rowid:tt, $page_type:literal) => { {
				let result = (self.transform)(&$cell)
					.with_context(|| format!("processing row for cell {}{}", $cell_idx, $rowid));
				match result {
					Ok(Some(result)) => return Some(Ok(result)),
					Ok(None) => {
						self.ended = Some(Ended::Completed);
						return None
					}
					Err(err) => {
						self.ended = Some(Ended::Failed);
						(err, $page_type)
					}
				}
			} }
		}

		let next = state.as_mut().and_then(|state| state.next(page));
		let (err, kind) = match (next, &page.kind) {
			(Some((cell_idx, Ok(cell))), PageKind::InteriorIndex { .. }) => {
				let payload_ptr_range = cell.payload.as_ptr_range();
				let page_num = cell.left_page
					.expect("interior index pages must have left child page numbers");
				push_page!("index", is_index, page_num, Some((cell_idx, payload_ptr_range)),
					{ format!("cell {cell_idx}") })
			}
			(Some((cell_idx, Ok(cell))), PageKind::InteriorTable { .. }) => {
				let page_num = cell.left_page
					.expect("interior table pages must have left child page numbers");
				let rowid = cell.rowid
					.expect("interior table pages must have `rowid`s");
				push_page!("table", is_table, page_num, None,
					{ format!("cell {cell_idx} with `rowid` {rowid}") })
			}
			(Some((cell_idx, Ok(cell))), PageKind::LeafIndex) =>
				process_cell!(page, cell_idx, cell, "", "leaf index"),
			(Some((cell_idx, Ok(cell))), PageKind::LeafTable) => {
				let rowid = cell.rowid
					.expect("leaf table pages must have `rowid`s");
				process_cell!(page, cell_idx, cell, { format!("with `rowid` {rowid}") }, "leaf table")
			}
			(Some((_, Err(err))), PageKind::InteriorIndex { .. }) => (err, "interior index"),
			(Some((_, Err(err))), PageKind::InteriorTable { .. }) => (err, "interior table"),
			(Some((_, Err(err))), PageKind::LeafIndex) => (err, "leaf index"),
			(Some((_, Err(err))), PageKind::LeafTable) => (err, "leaf table"),
			(None, &PageKind::InteriorIndex { rightmost_page }) if state.take().is_some() =>
				push_page!("index", is_index, rightmost_page, None,
					{ format!("right-most pointer of index page #{}", page_num) }),
			(None, &PageKind::InteriorTable { rightmost_page }) if state.take().is_some() =>
				push_page!("table", is_table, rightmost_page, None,
					{ format!("right-most pointer of table page #{}", page_num) }),
			(None, _) => {
				let (cell_payload_ptr_range, page, _) = self.stack.pop()?;
				if let Some((cell_idx, cell_payload_ptr_range)) = cell_payload_ptr_range {
					let cell_page = self.stack.last().map(|(_, p, _)| p).unwrap_or(self.root_page);
					assert!(matches!(cell_page.kind, PageKind::InteriorIndex { .. }));
					let payload_start = cell_payload_ptr_range.start;
					let payload_len = cell_payload_ptr_range.end as usize - payload_start as usize;
					// SAFETY: `payload_start` points into the `buf` of either what is now the top
					// page on the `stack` (after popping), or `root_page`. `buf` is a `Box`, so any
					// moves are guaranteed to have left its pointee intact.
					let payload = unsafe { std::slice::from_raw_parts(payload_start, payload_len) };

					let cell = Cell { left_page: Some(page.num), rowid: None, payload };
					process_cell!(cell_page, cell_idx, cell, "", "interior index")
				} else {
					return self.next()
				}
			}
		};

		Some(Err(err).with_context(|| format!("parsing {kind} page #{page_num}")))
	}
}

impl<F, T> FusedIterator for ScanRecords<'_, F>
where F: for<'a> Fn(&'a Cell<'a>) -> anyhow::Result<Option<T>> {}

impl Page {
	pub(crate) fn scan_records<'a, T, F>(
		&'a self,
		file: &'a mut File,
		file_header: &'a Header,
		transform: F,
	) -> anyhow::Result<ScanRecords<'a, F>>
	where F: Fn(&Cell) -> anyhow::Result<T> {
		let PageCells { state, .. } = self.cells();

		Ok(ScanRecords {
			file,
			file_header,
			root_page: self,
			root_state: Some(state),
			stack: Vec::new(),
			transform,
			ended: None,
		})
	}
}
