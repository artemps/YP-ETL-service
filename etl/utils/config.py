from typing import Optional

from dotenv import load_dotenv
from pydantic import Field, BaseModel, BaseSettings
from redis import Redis

load_dotenv()


class AppConfig(BaseSettings):
    chunk_size: int = Field(50, env='CHUNK_SIZE')
    sleep_time: float = Field(60.0, env='SLEEP_TIME')


class DataBaseConfig(BaseSettings):
    dbname: str = Field('db_name', env='DB_NAME')
    user: str = Field('user', env='DB_USER')
    password: str = Field('pass', env='DB_PASSWORD')
    host: str = Field('localhost', env='DB_HOST')
    port: int = Field(5432, env='DB_PORT')


class ElasticConfig(BaseSettings):
    elastic_host: str = Field('localhost', env='ELASTIC_HOST')
    elastic_port: str = Field(9200, env='ELASTIC_PORT')
    movies_index: str = Field('default', env='ELASTIC_MOVIES_INDEX')
    genres_index: str = Field('default', env='ELASTIC_GENRES_INDEX')

    def get_elastic_url(self):
        return 'http://{}:{}'.format(self.elastic_host, self.elastic_port)


class RedisConfig(BaseSettings):
    redis_host: str = Field('localhost', env='REDIS_HOST')
    redis_port: int = Field(6379, env='REDIS_PORT')

    def get_redis_client(self):
        redis_url = f'redis://{self.redis_host}:{self.redis_port}'
        return Redis.from_url(redis_url)


class ESPerson(BaseModel):
    id: str
    name: str


class ESFilmWork(BaseModel):
    id: str
    imdb_rating: Optional[float] = None
    genre: list[str]
    title: str
    description: Optional[str] = None
    director: list[str] = None
    actors_names: list[str]
    writers_names: list[str]
    actors: list[ESPerson]
    writers: list[ESPerson]
    directors: list[ESPerson]


class ESGenre(BaseModel):
    id: str
    name: str
    description: str
