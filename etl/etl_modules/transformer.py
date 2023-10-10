from utils.config import ESFilmWork


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
            )
            transformed_filmworks.append(filmwork)

        return transformed_filmworks
