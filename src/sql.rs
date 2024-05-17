use std::borrow::Cow;

use anyhow::Context as _;


pub(crate) enum SqlWhereOp { Eq }

pub(crate) struct SqlWhere<'a> {
	pub(crate) op: SqlWhereOp,
	pub(crate) left_col_name: &'a str,
	pub(crate) right_str_operand: &'a str,
}


// Select

pub(crate) enum SqlSelectCount { All }

pub(crate) enum SqlSelectColumn<'a> {
	Count(SqlSelectCount),
	Ident(&'a str),
}

pub(crate) struct SqlSelect<'a> {
	pub(crate) columns: Box<[SqlSelectColumn<'a>]>,
	pub(crate) table_name: &'a str,
	pub(crate) wher: Option<SqlWhere<'a>>,
}

impl<'a> TryFrom<&'a str> for SqlSelect<'a> {
	type Error = anyhow::Error;
	fn try_from(sql: &'a str) -> anyhow::Result<Self> {
		let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
		anyhow::ensure!(sql.len() >= 6 && sql[..6].eq_ignore_ascii_case("select"),
			"expected `SELECT` token");
		let sql = sql[6..].strip_prefix(|c: char| c.is_ascii_whitespace())
			.context("expected ASCII whitespace after `SELECT` token")?;

		let (columns, sql) = {
			let mut acc = Vec::new();
			let mut sql = sql;
			loop {
				if let Some((count, sql_rest)) = maybe_parse_sql_select_count(sql)
					.context("parsing `COUNT(*)` term")? {
					acc.push(SqlSelectColumn::Count(count));
					sql = sql_rest;
				} else {
					let (ident, sql_rest) = parse_sql_ident(sql)
						.context("parsing column identifier")?;
					acc.push(SqlSelectColumn::Ident(ident));
					sql = sql_rest;
				}
				let end_sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
				if !end_sql.starts_with(',') { break }
				sql = &end_sql[1..].trim_start_matches(|c: char| c.is_ascii_whitespace());
			}
			(acc.into(), sql)
		};

		let sql = sql.strip_prefix(|c: char| c.is_ascii_whitespace())
			.context("expected ASCII whitespace after `<select expression>` token")?;
		anyhow::ensure!(sql.len() >= 4 && sql[..4].eq_ignore_ascii_case("from"),
			"expected `FROM` token");
		let sql = sql[4..].strip_prefix(|c: char| c.is_ascii_whitespace())
			.context("expected ASCII whitespace after `FROM` token")?;
		let (table_name, sql) = parse_sql_ident(sql)
			.context("parsing table identifier")?;

		let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());

		let (wher, sql) = if sql.len() >= 5 && sql[..5].eq_ignore_ascii_case("where") {
			let sql = sql[5..].trim_start_matches(|c: char| c.is_ascii_whitespace());
			let (left_col_name, sql) = parse_sql_ident(sql)
				.context("parsing where left column identifier")?;
			let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
			let sql = sql.strip_prefix('=')
				.context("expected `=` token (only supported operator)")?;
			let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
			let sql = sql.strip_prefix('\'')
				.context("expected `'` token (only string operands supported)")?;
			// TODO: Consider escaped quotes "\'"
			let (right_str_operand, sql) = sql.split_once('\'')
				.context("expected closing '\'' token")?;
			(Some(SqlWhere { op: SqlWhereOp::Eq, left_col_name, right_str_operand }), sql)
		} else {
			(None, sql)
		};

		anyhow::ensure!(sql.trim_start_matches(|c: char| c.is_ascii_whitespace()).is_empty(),
			"expected end of SQL `SELECT` query");

		Ok(SqlSelect { columns, table_name, wher })
	}
}

fn parse_sql_ident(sql: &str) -> anyhow::Result<(&str, &str)> {
	anyhow::ensure!(sql.starts_with(|c: char| c.is_ascii_alphabetic()),
		"expected identifier to start with ASCII alphabetic character");
	let end = sql.bytes().skip(1)
		.position(|b| b != b'_' && !char::from(b).is_ascii_alphanumeric())
		.unwrap_or(sql.len() - 1) + 1;
	Ok(sql.split_at(end))
}

fn maybe_parse_sql_select_count(sql: &str) -> anyhow::Result<Option<(SqlSelectCount, &str)>> {
	if sql.len() < 5 || !sql[..5].eq_ignore_ascii_case("count") { return Ok(None) }
	let sql = sql[5..].trim_start_matches(|c: char| c.is_ascii_whitespace());
	let Some(sql) = sql.strip_prefix('(') else { return Ok(None) };
	let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	let sql = sql.strip_prefix('*').context("expected `*` (asterisk)")?;
	let sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
	let sql = sql.strip_prefix(')').context("expected `)` (right parenthesis)")?;
	Ok(Some((SqlSelectCount::All, sql)))
}


// Create

pub(crate) struct SqlCreateColumn<'a> {
	pub(crate) name: Cow<'a, str>,
	pub(crate) is_rowid_alias: bool,
	// TODO: Etc.
}

impl<'a> SqlCreateColumn<'a> {
	pub(crate) fn parse(table_name: &'a str, mut sql: &'a str)
	-> anyhow::Result<impl Iterator<Item = anyhow::Result<Self>>> {
		sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
		anyhow::ensure!(sql.len() >= 6 && sql[..6].eq_ignore_ascii_case("create"),
			"expected `CREATE` token");
		sql = sql[6..].trim_start_matches(|c: char| c.is_ascii_whitespace());
		anyhow::ensure!(sql.len() >= 5 && sql[..5].eq_ignore_ascii_case("table"),
			"expected `TABLE` token");
		sql = sql[5..].trim_start_matches(|c: char| c.is_ascii_whitespace());
		let (is_quoted, mut sql) = sql.strip_prefix('"')
			.map(|sql| (true, sql)).unwrap_or((false, sql));
		sql = sql.strip_prefix(table_name).context("expected table name token")?;
		if is_quoted { sql = sql.strip_prefix('"')
			.context("expected closing quotation mark '\"'")? }
		sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());
		sql = sql.strip_prefix('(').context("expected '(' (left parenthesis) token")?;
		sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace());

		Ok(std::iter::from_fn(move || {
			if sql.is_empty() { return None }

			fn inner<'a>(sql: &mut &'a str) -> anyhow::Result<SqlCreateColumn<'a>> {
				let (col_name, rest) = sql.split_once(|c: char| c.is_ascii_whitespace())
					.context("expected <column name> token followed by whitespace")?;
				let end = rest.find(|c: char| c == ',' || c == ')')
					.context("expected ',' (comma, column delimiter) \
						or ')' (right parenthesis) token")?;
				// TODO: More robust parsing
				let is_rowid = rest[..end].to_ascii_lowercase().contains("integer primary key");
				*sql = rest[end + 1..].trim_start_matches(|c: char| c.is_ascii_whitespace());
				Ok(SqlCreateColumn { name: col_name.into(), is_rowid_alias: is_rowid })
			}
			Some(inner(&mut sql))
		}))
	}
}
