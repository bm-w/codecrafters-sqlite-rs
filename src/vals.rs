use std::fmt::Write as _;

use anyhow::Context as _;


#[derive(Clone, Copy)]
pub(crate) struct Varint(pub(crate) i64);

impl Varint {
	pub(crate) fn parse(buf: &[u8]) -> anyhow::Result<(usize, Self)> {
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

		// SAFETY: Transmuting `u64` to `i64` is always safe.
		Ok((offset + 1, Self(unsafe { std::mem::transmute(bits) })))
	}
}

impl std::fmt::Display for Varint {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}


#[derive(Clone, Copy)]
pub(crate) struct Type(pub(crate) i64);

impl Type {
	pub(crate) fn is_blob(self) -> bool { self.0 >= 12 && self.0 % 2 == 0 }
	pub(crate) fn is_text(self) -> bool { self.0 >= 13 && self.0 % 2 == 1 }

	pub(crate) fn val_len(self) -> anyhow::Result<usize> {
		Ok(match self.0 {
			0..=4 => self.0 as usize,
			5 => 6,
			6 | 7 => 8,
			8 | 9 => 0,
			10 | 11 => anyhow::bail!("unexpected internal type"),
			_ if self.is_blob() => (self.0 as usize - 12) / 2,
			_ if self.is_text() => (self.0 as usize - 13) / 2,
			_ => anyhow::bail!("unexpected type {}", self.0),
		})
	}

	pub(crate) fn int_val(self, buf: &[u8]) -> anyhow::Result<(usize, Option<i64>)> {
		fn try_arr<const LEN: usize>(buf: &[u8]) -> anyhow::Result<[u8; LEN]> {
			anyhow::ensure!(buf.len() >= LEN,
				"expected {LEN} byte{} reading a {}-bit twos-complement integer",
				if LEN == 1 { "" } else { "s" },
				LEN * 8);
			buf[..LEN].try_into()
				.with_context(|| format!("reading a {}-bit twos-complement integer", LEN * 8))
		}
		match self.0 {
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
}

impl From<Varint> for Type {
	fn from(value: Varint) -> Self {
		Self(value.0)
	}
}

impl std::fmt::Display for Type {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let len = self.val_len().map_err(|_| std::fmt::Error)?;
		match (self.0, len) {
			(0, _) => f.write_str("NULL"),
			(1, _) => f.write_str("i8"),
			(2, _) => f.write_str("i16"),
			(3, _) => f.write_str("i24"),
			(4, _) => f.write_str("i32"),
			(5, _) => f.write_str("i48"),
			(6, _) => f.write_str("i64"),
			(7, _) => f.write_str("f64"),
			(8, _) => f.write_char('0'),
			(9, _) => f.write_char('1'),
			(_, len) if self.is_blob() => write!(f, "blob({len})"),
			(_, len) if self.is_text() => write!(f, "blob({len})"),
			_ => Err(std::fmt::Error),
		}
	}
}
