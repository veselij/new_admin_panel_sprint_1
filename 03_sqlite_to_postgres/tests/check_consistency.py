"""Script to check consistency of data trasformation from sqlite to postgress."""
import argparse
import logging
import sqlite3
import uuid
from dataclasses import dataclass, fields
from datetime import date, datetime
from typing import Optional, Union

import psycopg2
from psycopg2 import sql as pg_sql
from psycopg2.extras import DictCursor

logging.basicConfig(level=logging.INFO, format='%(message)s')


@dataclass
class TableRowsStats:
    """Dataclass represents table results."""

    sqlite: int = 0
    pg: int = 0
    matched: int = 0


@dataclass(frozen=True)
class Filmwork:
    """Target table definition in Postgres for film_work."""

    id: uuid.UUID
    title: str
    description: Optional[str]
    creation_date: Optional[date]
    rating: Union[int, float, None]
    type: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


@dataclass(frozen=True)
class Genre:
    """Target table definition in Postgres for genre."""

    id: uuid.UUID
    name: str
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


@dataclass(frozen=True)
class Person:
    """Target table definition in Postgres for person."""

    id: uuid.UUID
    full_name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


@dataclass(frozen=True)
class GenreFilmwork:
    """Target table definition in Postgres for genre_film_work."""

    id: uuid.UUID
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    created_at: Optional[datetime]


@dataclass(frozen=True)
class PersonFilmwork:
    """Target table definition in Postgres for person_film_work."""

    id: uuid.UUID
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    role: Optional[str]
    created_at: Optional[datetime]


def get_row_count(connection, sql):
    """Get one iteration with one row in result.

    Args:
        connection: connections string
        sql: sql to execute

    Returns:
        int: number of rows

    """
    try:
        rows = next(select_from_db(connection, sql, n_rows=1))[0][0]
    except StopIteration:
        return 0
    return rows


def select_from_db(connection, sql, sql_params=None, n_rows=100):
    """Retrive data from sqlite or postgress db.

    Args:
        connection: connection string to database.
        sql: sql to execute in database
        sql_params: paramters for sql (default=None)
        n_rows: number of rows to fetch at one iteration (default=100)

    Yields:
        list: fetched rows.

    """
    cur = connection.cursor()
    if sql_params:
        cur.execute(sql, sql_params)
    else:
        cur.execute(sql)
    while True:
        sql_result = cur.fetchmany(n_rows)
        if not sql_result:
            cur.close()
            break
        yield sql_result


class TablesChecker:
    """Class to compare tables from two databases."""

    def __init__(self, sqlite_conn, pg_conn):
        """Initizlizaiton of table checker.

        Args:
            sqlite_conn: connection string to sqlite
            pg_conn: connection string to postgress

        """
        self._sqlite_conn = sqlite_conn
        self._pg_conn = pg_conn
        self._pg_rows = []
        self._table = ''
        self._sql_rows_count = 'select count(*) from {0}'
        self._sql_sqlite = 'select {0} from {1}'
        self.rows_stats = {}

    def calculate_tables_stats(self):
        """Calculate rows counts and matches in SQLite and Postgress tables."""
        for table, dc in table_registry.items():
            self._table = table
            self._count_rows_in_table()

            columns = [field.name for field in fields(dc)]
            for sqlite_row in select_from_db(self._sqlite_conn, self._sql_sqlite.format(','.join(columns), table)):
                self._get_rows_from_pg_by_ids(sqlite_row)
                self._compare_rows(sqlite_row)

    def _count_rows_in_table(self):
        """Count total rows per table."""
        sqlite_rows = get_row_count(self._sqlite_conn, self._sql_rows_count.format(self._table))
        pg_rows = get_row_count(self._pg_conn, self._sql_rows_count.format(self._table))
        self.rows_stats[self._table] = TableRowsStats(sqlite_rows, pg_rows, 0)

    def _get_rows_from_pg_by_ids(self, sqlite_rows):
        """Get number rows from Postgress table with in filter.

        Args:
            sqlite_rows: sqlite rows with ids to select from postgress

        """
        sql = pg_sql.SQL('select * from {0} where id in %(ids)s').format(pg_sql.Identifier(self._table))
        ids = tuple(row[0] for row in sqlite_rows)
        try:
            pg_rows = next(select_from_db(self._pg_conn, sql, {'ids': ids}))
        except StopIteration:
            pg_rows = []
        self._pg_rows = pg_rows

    def _compare_rows(self, sqlite_rows):
        """Compare sqlite and postgress rows.

        Args:
            sqlite_rows: list of rows from sqlite table
        """
        for sqlite_row in sqlite_rows:
            for pg_row in self._pg_rows:
                if sqlite_row == self._convert(pg_row):
                    self.rows_stats[self._table].matched += 1
                    break

    def _convert(self, row):
        """Convert dataime column in row to string.

        Args:
            row: row of table.

        Returns:
            tuple: tuple copy of row with dataime converted to string.

        """
        for index, column in enumerate(row):
            if isinstance(column, datetime):
                row[index] = '{0}+00'.format(datetime.strftime(column, '%Y-%m-%d %H:%M:%S.%f').rstrip('0'))
        return tuple(row)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", required=True, type=str, help="file path to sqlite")
    parser.add_argument("--pg_db", required=True, type=str, help="postgress database name")
    args = parser.parse_args()
    sqlite_path = args.sqlite
    pg_db = args.pg_db

    dsl = {'dbname': pg_db, 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}

    rows_stats = {}

    table_registry = {}
    table_registry['film_work'] = Filmwork
    table_registry['genre'] = Genre
    table_registry['person'] = Person
    table_registry['genre_film_work'] = GenreFilmwork
    table_registry['person_film_work'] = PersonFilmwork

    with sqlite3.connect(sqlite_path) as sqlite_conn:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            table_checker = TablesChecker(sqlite_conn, pg_conn)
            table_checker.calculate_tables_stats()
            for table, stat in table_checker.rows_stats.items():
                logging.info('{0} {1}/{2}/{3} sqlite/pg/matched'.format(table, stat.sqlite, stat.pg, stat.matched))
