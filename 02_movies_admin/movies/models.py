import uuid

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _("genre")
        verbose_name_plural = _("genres")


class Filmwork(UUIDMixin, TimeStampedMixin):

    class FilmType(models.TextChoices):
        MOVIE = "MV", _("movie")
        TV_SHOW = "TV", _("tv_show")

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"))
    creation_date = models.DateField(_("creation_date"), blank=True)
    rating = models.FloatField(_("rating"), blank=True,
                                validators=[MinValueValidator(0),
                                            MaxValueValidator(100)])
    type = models.CharField("type", choices=FilmType.choices, max_length=2)
    genres = models.ManyToManyField(Genre, through="GenreFilmwork")

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _("filmwork")
        verbose_name_plural = _("filmworks")

    def _str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_("full_name"), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _("person")
        verbose_name_plural = _("persons")


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    role = models.TextField(_("role"), null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
