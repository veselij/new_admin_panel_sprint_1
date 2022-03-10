"""Django admin definitions."""
from django.contrib import admin
from django.db.models import Prefetch
from django.utils.translation import gettext_lazy as _
from movies.models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    """Inline class for film genre changing."""

    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    """Inline class for person film changing."""

    model = PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    """Admin class for genre with list to display and search."""

    list_display = ("name", )
    search_fields = ("name", "description", "id")


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    """Admin class for filmwork with list to display, search and filters."""

    inlines = (GenreFilmworkInline, )
    list_display = ("title", "type", "creation_date", "rating", "film_genres", "film_directors", "film_writers")
    list_filter = ("type",)
    search_fields = ("title", "description", "id")

    @admin.display(description=_("FILM_GENRES"))
    def film_genres(self, genres):
        return ",".join([genre.name for genre in genres.genres.all()])

    @admin.display(description=_("FILM_DIRECTORS"))
    def film_directors(self, directors):
        return ",".join({director.full_name for director in directors.directors})

    @admin.display(description=_("FILM_WRITERS"))
    def film_writers(self, writers):
        return ",".join({writer.full_name for writer in writers.writers})

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        directors = Person.objects.filter(personfilmwork__role="director").order_by("full_name")
        writers = Person.objects.filter(personfilmwork__role="writer").order_by("full_name")
        return qs.prefetch_related(
            Prefetch("genres", queryset=Genre.objects.order_by('name')),
            Prefetch("persons", queryset=directors, to_attr="directors"),
            Prefetch("persons", queryset=writers, to_attr="writers"),
        )


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    """Admin class for person with list to display and search."""

    inlines = (PersonFilmworkInline, )
    list_display = ("full_name", )
    search_fields = ("full_name", "id")
