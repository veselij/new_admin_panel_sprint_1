"""Script to migrate data from sqlite3 to Postgres database."""
import csv
import logging
import sqlite3
import uuid
from dataclasses import dataclass, fields
from datetime import date, datetime
from io import StringIO
from typing import Any, Callable, Generator, Optional, Type, Union, get_args, get_origin

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

file_logger = logging.getLogger('file_logger')
file_logger.setLevel(logging.INFO)
log = logging.FileHandler('skipped_rows.log', 'w', 'utf-8')
log.setLevel(logging.INFO)
file_logger.addHandler(log)

UniqueViolation = psycopg2.errors.lookup('23505')


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


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Maigrate from SQLite to Postgres.

    Args:
        connection: sqlite3 connection string
        pg_conn: Postgres connection string

    """
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    sqlite_data = sqlite_loader.load_movies()
    postgres_saver.save_all_data(sqlite_data)


def check_value_error(func: Callable[[Any], Any], arg: Any) -> bool:
    """Check if func call trigger ValueError. Used for types convertions.

    Args:
        func: function to test
        arg: function argument for testing

    Returns:
        bool: True if ValueError rised

    """
    try:
        func(arg)
    except ValueError:
        return True
    return False


def convert_to_datetime(datetime_value: str) -> Union[datetime, str]:
    """Convert datetime in string format to python datatime class or return value itself in case of ValueError.

    Args:
        datetime_value: datetime in string format

    Returns:
        ether a datetime object converted from string date or string itself in case of ValueError

    """
    try:
        converted = datetime.strptime('{0}00'.format(datetime_value), '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        return datetime_value
    return converted


class SQLiteLoader:
    """Loader of data from sqlite3 to csv tables."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Initialize of loader.

        Args:
            connection: sqlite3 connection string.

        """
        self._connection = connection
        self._dataclass = None
        self._table_name = ''
        self._uniq_ids = set()
        self._output = StringIO()

    def load_movies(self, n_rows: int = 1000) -> Generator[tuple[str, StringIO], None, None]:
        """Select data from sqlite3 and save to files.

        Args:
            n_rows: number of rows to fetch from sqlite at once. Default = 1000.

        Yields:
            tuple: table name and file-like object

        """
        for table_name, dc in table_registry.items():
            self._uniq_ids = set()
            self._dataclass = dc
            self._table_name = table_name
            cur = self._connection.cursor()
            cur.execute(self._sql_from_dataclass(dc))
            while True:
                rows = cur.fetchmany(size=n_rows)
                if not rows:
                    break
                dc_rows = [self._dataclass(*row) for row in rows]
                self._save_to_csv(dc_rows)
                yield (self._table_name, self._output)
            cur.close()

    def _sql_from_dataclass(self, dc: Type[Union[Filmwork, Genre, Person, GenreFilmwork, PersonFilmwork]]) -> str:
        select_fields = [field.name for field in fields(dc)]
        return "select {columns} from {table}".format(columns=','.join(select_fields), table=self._table_name)

    def _save_to_csv(self, dc_rows: list) -> None:
        """Save list of dataclasses instances to csv in memory writer.

        Args:
            dc_rows: dataclasses list of rows to save to csv

        """
        output = StringIO()
        csv_writer = csv.writer(output, delimiter='\t', escapechar='\\', quoting=csv.QUOTE_NONE)
        for row in dc_rows:
            row_values = [getattr(row, field.name) for field in fields(self._dataclass)]
            if self._validate_uniqs(row) and self._validate_types(row):
                csv_writer.writerow(row_values)
            else:
                file_logger.info(
                    'validation error\ttable:{0}\trow:{1}'.format(self._table_name, ','.join(map(str, row_values))),
                )
        output.seek(0)
        self._output = output

    def _validate_uniqs(self, dc: Union[Filmwork, Genre, Person, GenreFilmwork, PersonFilmwork]) -> bool:
        """Validate id fields for unique values.

        Args:
            dc: dataclass to validate

        Returns:
            bool: True if no dublicates of ids found, False if found

        """
        if dc.id in self._uniq_ids:
            return False
        self._uniq_ids.add(dc.id)
        return True

    def _validate_types(self, dc: Union[Filmwork, Genre, Person, GenreFilmwork, PersonFilmwork]) -> bool:
        """Validate sqlite rows values have required data type of Postgres tables columns.

        Args:
            dc: dataclass to check types

        Returns:
            bool: True if no type mismatches found, False if at least one mismatches

        """
        for field in fields(self._dataclass):
            row_value = getattr(dc, field.name)
            value_target_type = field.type
            if get_origin(value_target_type) is Union:
                optional_possible_types = get_args(value_target_type)
                if datetime in optional_possible_types:
                    row_value = convert_to_datetime(row_value)
                if not isinstance(row_value, optional_possible_types):
                    return False
            elif check_value_error(value_target_type, row_value):
                return False
        return True


class PostgresSaver:
    """Class to insert data from csv files to Postgres tables."""

    def __init__(self, pg_conn: _connection) -> None:
        """Initialize postgres saver.

        Args:
            pg_conn: Postgres connection string

        """
        self._pg_conn = pg_conn

    def save_all_data(self, sqlite_output: Generator[tuple[str, StringIO], None, None]) -> None:
        """Insert data from csv file to Postgres tables.

        Args:
            sqlite_output: generator of names of csv file to import to Postgres

        """
        for table, fl in sqlite_output:
            self._insert_in_pg(table, fl)

    def _insert_in_pg(self, table: str, fl: StringIO) -> None:
        with self._pg_conn.cursor() as cursor:
            try:
                cursor.copy_from(fl, table, sep='\t', null="")
            except UniqueViolation:
                self._pg_conn.rollback()
            else:
                self._pg_conn.commit()


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    table_registry = {}
    table_registry['film_work'] = Filmwork
    table_registry['genre'] = Genre
    table_registry['person'] = Person
    table_registry['genre_film_work'] = GenreFilmwork
    table_registry['person_film_work'] = PersonFilmwork
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
