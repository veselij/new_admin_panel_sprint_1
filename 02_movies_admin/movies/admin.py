from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, Person, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', )
    search_fields = ('name',  'description', 'id')


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, )

    list_display = ('title', 'type', 'creation_date', 'rating',)
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    inlines = (PersonFilmworkInline, )

    list_display = ('full_name', )
    search_fields = ('full_name', 'id')
