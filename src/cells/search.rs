use std::{cmp::Ordering, fs::File};

use anyhow::Context;

use crate::{cells::PageCellsState, page::{Header, Page, PageKind}};

use super::{scan::ScanRecords, Cell};


pub(crate) enum MatchOrdering<T> { Less, Match(T), Greater, }

impl<T> MatchOrdering<T> {
	pub(crate) fn try_from<F>(cmp: Ordering, f: F) -> anyhow::Result<Self>
	where F: FnOnce() -> anyhow::Result<T> {
		Ok(match cmp {
			Ordering::Less => MatchOrdering::Less,
			Ordering::Equal => MatchOrdering::Match((f)()?),
			Ordering::Greater => MatchOrdering::Greater,
		})
	}
}

/// See also [`scan::StackElem`].
type StackElem<T> = (usize, Option<std::ops::Range<*const u8>>, Option<T>, Page);

pub(crate) struct Search<'a, C, T> {
	file: &'a mut File,
	file_header: &'a Header,
	root_page: &'a Page,
	cmp: C,
	stack: Vec<StackElem<T>>,
	match_cell_idx: usize,
	pub(crate) value: T,
}

impl<C, T> Search<'_, C, T> {
	#[allow(dead_code)]
	pub(crate) fn into_value(self) -> T { self.value }
}

impl Page {
	/// `cmp` returns ordering between key and `Cell`, i.e. `Less` means “key < `Cell`” so search
	/// should continue to the `Cell`’s “left”.
	pub(crate) fn search_records<'a, C, T>(
		&'a self,
		file: &'a mut File,
		file_header: &'a Header,
		cmp: C,
	) -> anyhow::Result<Option<Search<'a, C, T>>> 
	where C: Fn(&Cell) -> anyhow::Result<MatchOrdering<T>> {
		use MatchOrdering::*;

		let mut stack = Vec::new();

		let r#match = 'outer: loop {
			let page = stack.last().map(|(_, _, _, page)| page).unwrap_or(self);

			let mut cell_idx_start = 0;
			let mut cell_idx_end = page.num_cells;
			if cell_idx_end == 0 { break None }

			let mut right_cell = None;
			let mut prev_match = None;
			let cell = loop {
				let cell_idx = cell_idx_start + (cell_idx_end - cell_idx_start) / 2;
				let cell = page.parse_cell(cell_idx)
					.with_context(|| format!("parsing cell {cell_idx} of page #{}", page.num))?;
				let cmp = (cmp)(&cell)
					.with_context(|| format!("processing cell {cell_idx} of page #{}", page.num))?;
				match cmp {
					Less if cell_idx == cell_idx_start => break Some(cell),
					Less => {
						right_cell = Some(cell);
						cell_idx_end = cell_idx;
					}
					Match(value) => {
						prev_match = Some(value);
						if cell_idx > cell_idx_start {
							right_cell = Some(cell);
							cell_idx_end = cell_idx;
						} else if cell.left_page.is_some() { break Some(cell) }
						else { break 'outer prev_match.map(|v| (cell_idx_start, v)) }
					}
					Greater => {
						cell_idx_start = cell_idx + 1;
						if cell_idx_start < cell_idx_end { continue }
						else if right_cell.as_ref().is_some_and(|c| c.left_page.is_some()) {
							break right_cell
						} else if let Some(v) = prev_match {
							break 'outer Some((cell_idx_start, v))
						}
						break None
					}
				}
			};

			let Some(page_num) = (if let Some(cell) = cell.as_ref() { cell.left_page }
				else { page.rightmost_page() })
				else { break None };

			let cell_payload = matches!(page.kind, PageKind::InteriorIndex { .. })
				.then(|| cell.map(|c| c.payload.as_ptr_range()))
				.flatten();

			let page = Page::parse(file, file_header, page_num)
				.with_context(|| format!("parsing page #{page_num}"))?;
			stack.push((cell_idx_start, cell_payload, prev_match, page));
		};

		let Some((match_cell_idx, value)) = r#match.or_else(|| {
			while let Some((ci, _, v, _)) = stack.pop() {
				if let Some(v) = v { return Some((ci, v)) }
			}
			None
		}) else { return Ok(None) };

		Ok(Some(Search { file, file_header, root_page: self, cmp, stack, match_cell_idx, value }))
	}
}

impl<'a, C, T> Search<'a, C, T> {
	fn _into_scan_with<F>(self, into_transform: impl FnOnce(C) -> F) -> ScanRecords<'a, F> {
		let root_page = self.root_page;
		let (root_state, stack) = if let Some(root_cell_idx)
			= self.stack.first().map(|(ci, _, _, _)| *ci)
		{
			let mut scan_stack = Vec::new();
			let root_state = if root_cell_idx == root_page.num_cells { None }
				else { Some(PageCellsState { cell_idx: root_cell_idx + 1 }) };
			let mut prev = None;
			for (cell_idx, cell_payload, _, page) in self.stack {
				let Some((prev_cell_idx, prev_cell_payload, page))
					= prev.replace((cell_idx, cell_payload, page)) else { continue };
				let prev_cell = prev_cell_payload.map(|p| (prev_cell_idx, p));
				let state = if cell_idx == page.num_cells { None }
					else { Some(PageCellsState { cell_idx: cell_idx + 1 }) };
				scan_stack.push((prev_cell, page, state));
			}
			if let Some((prev_cell_idx, prev_cell_payload, page)) = prev {
				assert_ne!(self.match_cell_idx, page.num_cells, "match must one of page’s cells");
				let prev_cell = prev_cell_payload.map(|p| (prev_cell_idx, p));
				let state = Some(PageCellsState { cell_idx: self.match_cell_idx });
				scan_stack.push((prev_cell, page, state));
			}
			(root_state, scan_stack)
		} else {
			(Some(PageCellsState { cell_idx: self.match_cell_idx }), Vec::new())
		};

		ScanRecords {
			file: self.file,
			file_header: self.file_header,
			root_page,
			root_state,
			stack,
			transform: (into_transform)(self.cmp),
			ended: None,
		}
	}

	#[allow(dead_code)]
	pub(crate) fn into_scan_with<F>(self, transform: F) -> ScanRecords<'a, F> {
		self._into_scan_with(|_| transform)
	}

	pub(crate) fn into_scan(self)
	-> ScanRecords<'a, impl Fn(&Cell) -> anyhow::Result<Option<T>>>
	where C: Fn(&Cell) -> anyhow::Result<MatchOrdering<T>> + 'a {
		self._into_scan_with(|cmp| move |cell: &Cell| {
			let MatchOrdering::Match(value) = (cmp)(cell)? else { return Ok(None) };
			Ok(Some(value))
		})
	}
}
