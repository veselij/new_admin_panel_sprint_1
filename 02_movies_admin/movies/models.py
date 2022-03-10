"""Django app movies models definitions."""
import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    """Mixin to extend class models with date fields create and modified."""

    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    """Mixin to extend class models with id uuid field."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    """Film genre model."""

    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _("genre")
        verbose_name_plural = _("genres")

    def __str__(self) -> str:
        """Override default.

        Returns:
            genre name.
        """
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    """Model for filmwork persons."""

    full_name = models.CharField(_("full_name"), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _("person")
        verbose_name_plural = _("persons")


class Filmwork(UUIDMixin, TimeStampedMixin):
    """Class model represents model for cinematographic work."""

    class FilmType(models.TextChoices):
        """Choices for filmwork types."""

        movie = "movie", _("movie")
        tv_show = "tv_show", _("tv_show")

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"))
    creation_date = models.DateField(_("creation_date"), blank=True)
    rating = models.FloatField(_("rating"), blank=True, validators=[MinValueValidator(0), MaxValueValidator(100)])
    type = models.CharField("type", choices=FilmType.choices, max_length=7)
    genres = models.ManyToManyField(Genre, through="GenreFilmwork")
    persons = models.ManyToManyField(Person, through="PersonFilmwork")

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _("filmwork")
        verbose_name_plural = _("filmworks")
        indexes = [models.Index(fields=("creation_date", "rating"), name="film_work_creation_rating_idx")]

    def __str__(self) -> str:
        """Override default.

        Returns:
            filmwork title.
        """
        return self.title


class GenreFilmwork(UUIDMixin):
    """Class model represents relations many to many between genre and filmwork models."""

    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        constraints = [models.UniqueConstraint(fields=["film_work", "genre"], name="film_work_genre_idx")]
        indexes = [models.Index(fields=("film_work", "genre"), name="film_work_genre_idx")]


class PersonFilmwork(UUIDMixin):
    """Class model represents relations between person and filmwork models."""

    class Roles(models.TextChoices):
        """Choices for roles."""

        actor = "actor", _("actor")
        director = "director", _("director")
        writer = "writer", _("writer")

    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    role = models.TextField(_("role"), choices=Roles.choices, max_length=8, null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        constraints = [models.UniqueConstraint(fields=["film_work", "person", "role"], name="film_work_person_role_idx")]
        indexes = [models.Index(fields=("film_work", "person", "role"), name="film_work_person_role_idx")]
