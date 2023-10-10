from elasticsearch import helpers

from utils.config import ESFilmWork
from utils.connection import elastic_connection


class Loader:
    """Класс Loader предназначен для загрузки данных в Elasticsearch."""

    def __init__(self, elastic_config):
        self.elastic_url = elastic_config.get_elastic_url()
        self.index = elastic_config.index
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

        self.mappings = {
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
                }
            },
        }

    def load_filmworks(self, transformed_data: list[ESFilmWork]) -> list[str]:
        """Метод создает индекс при отсутствии и сохраняет переданную пачку данных в Elastic"""

        with elastic_connection(self.elastic_url) as conn:
            if not conn.indices.exists(index=self.index):
                conn.indices.create(index=self.index, settings=self.settings, mappings=self.mappings)

            data = [{'_index': 'movies', '_id': row.id, '_source': row.json()} for row in transformed_data]
            helpers.bulk(conn, data)
            return [x['_id'] for x in data]
