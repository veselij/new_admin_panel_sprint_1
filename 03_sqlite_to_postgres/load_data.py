"""Script to migrate data from sqlite3 to Postgres database."""
import csv
import logging
import os
import sqlite3
import uuid
from collections import namedtuple
from dataclasses import dataclass, fields
from datetime import date, datetime
from typing import Optional, Union, get_args, get_origin

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

console_logger = logging.getLogger('console_logger')
console_logger.setLevel(logging.INFO)
stream = logging.StreamHandler()
stream.setLevel(logging.INFO)
console_logger.addHandler(stream)

file_logger = logging.getLogger('file_logger')
file_logger.setLevel(logging.INFO)
log = logging.FileHandler('skipped_rows.log', 'w', 'utf-8')
log.setLevel(logging.INFO)
file_logger.addHandler(log)

UniqueViolation = psycopg2.errors.lookup('23505')


@dataclass(frozen=True)
class Filmwork:
    """Target table definition in Postgres for film_work."""

    __slots__ = ('id', 'title', 'description', 'creation_date', 'rating', 'type', 'created', 'modified')
    id: uuid.UUID
    title: str
    description: Optional[str]
    creation_date: Optional[date]
    rating: Union[int, float, None]
    type: str
    created: Optional[datetime]
    modified: Optional[datetime]


@dataclass(frozen=True)
class Genre:
    """Target table definition in Postgres for genre."""

    __slots__ = ('id', 'name', 'description', 'created', 'modified')
    id: uuid.UUID
    name: str
    description: Optional[str]
    created: Optional[datetime]
    modified: Optional[datetime]


@dataclass(frozen=True)
class Person:
    """Target table definition in Postgres for person."""

    id: uuid.UUID
    full_name: str
    created: Optional[datetime]
    modified: Optional[datetime]


@dataclass(frozen=True)
class GenreFilmwork:
    """Target table definition in Postgres for genre_film_work."""

    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: Optional[datetime]


@dataclass(frozen=True)
class PersonFilmwork:
    """Target table definition in Postgres for person_film_work."""

    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: Optional[str]
    created: Optional[datetime]


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


def check_value_error(func, arg):
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


def convert_str_to_datetime(str_datetime):
    """Convert datetime in string format to python datatime class or return value itself in case of ValueError.

    Args:
        str_datetime: datetime in string format

    Returns:
        ether a datetime object converted from string date or string object itself in case of ValueError

    """
    try:
        str_datetime = datetime.strptime('{0}00'.format(str_datetime), '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        return str_datetime
    return str_datetime


class SQLiteLoader:
    """Loader of data from sqlite3 to csv tables."""

    def __init__(self, connection):
        """Initialize of loader.

        Args:
            connection: sqlite3 connection string.

        """
        self._connection = connection
        self._file_names = {}
        self._counter = 0
        self._dataclass = None
        self._table_name = ''
        self._uniq_ids = set()
        self._uniq_fk_ids = set()
        self._number_of_skipped_rows = 0
        self._number_of_rows_to_insert = 0

    def load_movies(self, n_rows=1000):
        """Select data from sqlite3 and save to files.

        Args:
            n_rows: number of rows to fetch from sqlite at once. Default = 1000.

        Returns:
            dict: Dictionary of csv file names extracted from sqlite.

        """
        console_logger.info('start select from sqlite3')
        for key, table_value in table_registry.items():
            self._uniq_ids = set()
            self._counter = 0
            self._number_of_skipped_rows = 0
            self._number_of_rows_to_insert = 0
            self._dataclass = table_value.data_class
            self._table_name = key
            cur = self._connection.cursor()
            cur.execute(table_value.sql)
            while True:
                rows = cur.fetchmany(size=n_rows)
                if not rows:
                    break
                dc_rows = [self._dataclass(*row) for row in rows]
                self._save_to_csv(dc_rows)
                self._counter += 1
            cur.close()
            console_logger.info(
                'Table {0}: {1}/{2} (skipped/to insert) rows from sqlite'.format(
                    self._table_name, self._number_of_skipped_rows, self._number_of_rows_to_insert,
                ),
            )
        console_logger.info('')
        return self._file_names

    def _save_to_csv(self, dc_rows):
        """Save list of dataclasses instances to csv files.

        Args:
            dc_rows: dataclasses list of rows to save to csv

        """
        file_name = '{0}_{1}.csv'.format(self._table_name, self._counter)
        with open(file_name, 'w') as fl:
            csv_writer = csv.writer(fl, delimiter='\t', escapechar='\\')
            for row in dc_rows:
                row_values = [getattr(row, field.name) for field in fields(self._dataclass)]
                if self._validate_uniqs(row) and self._validate_types(row):
                    csv_writer.writerow(row_values)
                    self._number_of_rows_to_insert += 1
                else:
                    self._number_of_skipped_rows += 1
                    file_logger.info(
                        'validation error\ttable:{0}\trow:{1}'.format(self._table_name, ','.join(map(str, row_values))),
                    )
        if self._table_name not in self._file_names.keys():
            self._file_names[self._table_name] = []
        self._file_names[self._table_name].append(file_name)

    def _validate_uniqs(self, dc):
        """Validate id fields and foring keys combinations of tables are unique.

        Args:
            dc: dataclass to validate

        Returns:
            bool: True if no dublicates of ids found, False if found

        """
        if dc.id in self._uniq_ids:
            return False
        self._uniq_ids.add(dc.id)
        fk_ids = [field.name for field in fields(self._dataclass) if field.name.endswith('_id')]
        uniq_fk_id = tuple(getattr(dc, fk).strip() for fk in fk_ids)
        if uniq_fk_id and uniq_fk_id in self._uniq_fk_ids:
            return False
        self._uniq_fk_ids.add(uniq_fk_id)
        return True

    def _validate_types(self, dc):
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
                    row_value = convert_str_to_datetime(row_value)
                if not isinstance(row_value, optional_possible_types):
                    return False
            elif check_value_error(value_target_type, row_value):
                return False
        return True


class PostgresSaver:
    """Class to insert data from csv files to Postgres tables."""

    def __init__(self, pg_conn):
        """Initialize postgres saver.

        Args:
            pg_conn: Postgres connection string

        """
        self._pg_conn = pg_conn
        self._number_of_inserted_rows = 0

    def save_all_data(self, sqlite_output):
        """Insert data from csv files to Postgres tables.

        Args:
            sqlite_output: dict output of csv files to import to Postgres

        """
        console_logger.info('start insert process to Postgres')
        for table, files in sqlite_output.items():
            self._number_of_inserted_rows = 0
            columns = [field.name for field in fields(table_registry[table].data_class)]
            for file_name in files:
                self._insert_in_pg(file_name, table, columns)
                os.remove(file_name)
            console_logger.info('Table {0} inserted in Postgres {1} rows'.format(table, self._number_of_inserted_rows))
            console_logger.info('')

    def _insert_in_pg(self, file_name, table, columns):
        with open(file_name, 'r') as fl:
            with self._pg_conn.cursor() as cursor:
                try:
                    cursor.copy_from(fl, table, sep='\t', columns=columns, null="")
                except UniqueViolation:
                    self._pg_conn.rollback()
                    console_logger.info('Table {0} already inserted in Postgres'.format(table))
                else:
                    self._pg_conn.commit()
                    self._number_of_inserted_rows += cursor.rowcount


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}

    Table = namedtuple('Table', ['data_class', 'sql'])

    film_work_table = Table(
        Filmwork, 'select id, title, description, creation_date, rating, type, created_at, updated_at from film_work',
    )
    genre_table = Table(Genre, 'select id, name, description, created_at, updated_at from genre')
    person_table = Table(Person, 'select id, full_name, created_at, updated_at from person')
    genre_film_work_table = Table(GenreFilmwork, 'select id, film_work_id, genre_id, created_at from genre_film_work')
    person_film_work_table = Table(
        PersonFilmwork, 'select id, film_work_id, person_id, role, created_at from person_film_work',
    )

    table_registry = {}
    table_registry['film_work'] = film_work_table
    table_registry['genre'] = genre_table
    table_registry['person'] = person_table
    table_registry['genre_film_work'] = genre_film_work_table
    table_registry['person_film_work'] = person_film_work_table

    with sqlite3.connect('db.sqlite') as sqlite_conn:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
