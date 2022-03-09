"""File describes sqlite tables structure which needs to be migrated from sqlite to Postgres."""
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union


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


table_registry = {}
table_registry['film_work'] = Filmwork
table_registry['genre'] = Genre
table_registry['person'] = Person
table_registry['genre_film_work'] = GenreFilmwork
table_registry['person_film_work'] = PersonFilmwork
