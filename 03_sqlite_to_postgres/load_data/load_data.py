"""Script to migrate data from sqlite3 to Postgres database."""
import sqlite3

import psycopg2
from dotenv import dotenv_values
from loaders import PostgresSaver, SQLiteLoader
from psycopg2.extras import DictCursor
from tables import table_registry

if __name__ == '__main__':
    config = dotenv_values(".env")
    with sqlite3.connect('db.sqlite') as sqlite_conn:
        with psycopg2.connect(**config, cursor_factory=DictCursor) as pg_conn:

            postgres_saver = PostgresSaver(pg_conn)
            sqlite_loader = SQLiteLoader(sqlite_conn)

            sqlite_data = sqlite_loader.load_movies(table_registry)
            postgres_saver.save_all_data(sqlite_data)
