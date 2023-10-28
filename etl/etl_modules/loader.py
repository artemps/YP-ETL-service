from elasticsearch import helpers

from utils.config import ESFilmWork
from utils.connection import elastic_connection


class Loader:
    """Класс Loader предназначен для загрузки данных в Elasticsearch."""

    def __init__(self, elastic_config):
        self.elastic_url = elastic_config.get_elastic_url()
        self.movies_index = elastic_config.movies_index
        self.genres_index = elastic_config.genres_index

        self.settings = {
            'refresh_interval': '1s',
            'analysis': {
                'filter': {
                    'english_stop': {
                        'type': 'stop',
                        'stopwords': '_english_'
                    },
                    'english_stemmer': {
                        'type': 'stemmer',
                        'language': 'english'
                    },
                    'english_possessive_stemmer': {
                        'type': 'stemmer',
                        'language': 'possessive_english'
                    },
                    'russian_stop': {
                        'type': 'stop',
                        'stopwords': '_russian_'
                    },
                    'russian_stemmer': {
                        'type': 'stemmer',
                        'language': 'russian'
                    }
                },
                'analyzer': {
                    'ru_en': {
                        'tokenizer': 'standard',
                        'filter': [
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'english_possessive_stemmer',
                            'russian_stop',
                            'russian_stemmer'
                        ]
                    }
                }
            }
        }

        self.movies_mappings = {
            'dynamic': 'strict',
            'properties': {
                'id': {
                    'type': 'keyword'
                },
                'imdb_rating': {
                    'type': 'float'
                },
                'genre': {
                    'type': 'keyword'
                },
                'title': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'description': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'director': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'actors_names': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'writers_names': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
                'actors': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword'
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en'
                        }
                    }
                },
                'writers': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword'
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en'
                        }
                    }
                },
                'directors': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword'
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en'
                        }
                    }
                },
            },
        }

        self.genres_mappings = {
            'dynamic': 'strict',
            'properties': {
                'id': {
                    'type': 'keyword'
                },
                'name': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'description': {
                    'type': 'text',
                    'analyzer': 'ru_en'
                },
            },
        }

    def load_filmworks(self, transformed_data: list[ESFilmWork]) -> list[str]:
        """Метод создает индекс при отсутствии и сохраняет переданную пачку данных в Elastic"""

        with elastic_connection(self.elastic_url) as conn:
            if not conn.indices.exists(index=self.movies_index):
                conn.indices.create(index=self.movies_index, settings=self.settings, mappings=self.movies_mappings)

            data = [{'_index': 'movies', '_id': row.id, '_source': row.json()} for row in transformed_data]
            helpers.bulk(conn, data)
            return [x['_id'] for x in data]

    def load_genres(self, transformed_data: list[ESFilmWork]) -> list[str]:
        """Метод создает индекс при отсутствии и сохраняет переданную пачку данных в Elastic"""
        with elastic_connection(self.elastic_url) as conn:
            if not conn.indices.exists(index=self.genres_index):
                conn.indices.create(index=self.genres_index, settings=self.settings, mappings=self.genres_mappings)

            data = [{'_index': 'genres', '_id': row.id, '_source': row.json()} for row in transformed_data]
            helpers.bulk(conn, data)
            return [x['_id'] for x in data]
