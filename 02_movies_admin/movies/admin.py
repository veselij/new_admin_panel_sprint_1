"""Django admin definitions."""
from django.contrib import admin
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
    list_display = ("title", "type", "creation_date", "rating")
    list_filter = ("type",)
    search_fields = ("title", "description", "id")


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    """Admin class for person with list to display and search."""

    inlines = (PersonFilmworkInline, )
    list_display = ("full_name", )
    search_fields = ("full_name", "id")
