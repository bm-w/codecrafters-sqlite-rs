[![progress-banner](https://backend.codecrafters.io/progress/sqlite/471ac5bb-48ef-472a-bc71-481654f7a695)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

My Rust solution to the [codecrafters.io SQLite challenge]((https://app.codecrafters.io/courses/sqlite)).

In this challenge, I built a barebones SQLite implementation that supports
basic SQL queries like `SELECT`. Along the way I learned about
[SQLite's file format](https://www.sqlite.org/fileformat.html), how indexed data
is
[stored in B-trees](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)
and more.

**Note**: **Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://app.codecrafters.io/courses/sqlite) to try the challenge.


# Sample Databases

To make it easy to test queries locally, Codecrafters added a sample database in the
root of this repository: `sample.db`.

This contains two tables: `apples` & `oranges`. You can use this to test your
implementation for the first 6 stages.

You can explore this database by running queries against it like this:

```sh
$ sqlite3 sample.db "select id, name from apples"
1|Granny Smith
2|Fuji
3|Honeycrisp
4|Golden Delicious
```

There are two other databases that you can use:

1. `superheroes.db`:
   - This is a small version of the test database used in the table-scan stage.
   - It contains one table: `superheroes`.
   - It is ~1MB in size.
1. `companies.db`:
   - This is a small version of the test database used in the index-scan stage.
   - It contains one table: `companies`, and one index: `idx_companies_country`
   - It is ~7MB in size.

These aren't included in the repository because they're large in size. You can
download them by running this script:

```sh
./download_sample_databases.sh
```

If the script doesn't work for some reason, you can download the databases
directly from
[codecrafters-io/sample-sqlite-databases](https://github.com/codecrafters-io/sample-sqlite-databases).
