from utils.config import ESFilmWork, ESGenre


class Transformer:
    """Класс Transformer предназначен для обработки данных из PostgreSQL для загрузки в Elasticsearch."""

    def transform_filmworks(self, extracted_filmworks: dict) -> list[ESFilmWork]:
        transformed_filmworks = []
        for record in extracted_filmworks:
            filmwork = ESFilmWork(
                id=record['id'],
                imdb_rating=record['rating'],
                title=record['title'],
                description=record['description'],
                genre=record['genre'],
                director=record['director'],
                actors_names=record['actors_names'] or [],
                writers_names=record['writers_names'] or [],
                actors=record['actors'] or [],
                writers=record['writers'] or [],
                directors=record['directors'] or [],
            )
            transformed_filmworks.append(filmwork)

        return transformed_filmworks

    def transform_genres(self, extracted_genres: dict) -> list[ESGenre]:
        transformed_genres = []
        for record in extracted_genres:
            genre = ESGenre(
                id=record['id'],
                name=record['name'],
                description=record['description'],
            )
            transformed_genres.append(genre)

        return transformed_genres
