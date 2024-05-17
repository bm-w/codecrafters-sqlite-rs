// TODO: Better name?
pub(crate) trait LiftFallibleIteratorExt {
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
