import logging
from datetime import datetime
from typing import Generator

from utils.config import DataBaseConfig
from utils.connection import postgresql_connection
from .sql_queries import (
    SQL_FILMWORD_DATA, SQL_FILMWORK_IDS,
    SQL_FILMWORK_IDS_BY_PERSONS, SQL_FILMWORK_IDS_BY_GENRES
)

logger = logging.getLogger(__name__)


class Extractor:
    """Класс Extractor предназначен для извлечения данных из PostgreSQL"""

    def __init__(self, chunk_size: int, database_config: DataBaseConfig):
        self.chunk_size = chunk_size
        self.database_config = database_config

    def extract_filmworks(self, extract_timestamp: datetime, to_skip_list: list) -> Generator:
        """
        Метод позволяет чанками извлекать список фильмов для обновления
        Для начала выбираются все id фильмов которые были обновлены, обновлены их жанры или
        обновлены их участники.
        Далее выбираются данных, для всех нужных id, которые будут загружены в Elastic
        :param extract_timestamp: время, с которого нужно выбрать все обновленные фильмы
        :param to_skip_list: список id фильмов, которые уже были обработаны, но произошла какая то ошибка
        поэтому мы можем пропустить их
        """
        with postgresql_connection(self.database_config.dict()) as pg_conn:
            cur = pg_conn.cursor()
            filmworks_to_update = []
            filmworks_to_update.extend(self._extract_filmworks(extract_timestamp, cur))
            filmworks_to_update.extend(self._extract_filmworks_by_persons(extract_timestamp, cur))
            filmworks_to_update.extend(self._extract_filmworks_by_genres(extract_timestamp, cur))
            filmworks_to_update = list(set(filmworks_to_update))
            filmworks_to_update = tuple(set(filmworks_to_update) - set(to_skip_list))

            if not filmworks_to_update:
                logger.info('No data to update')
                return

            sql = SQL_FILMWORD_DATA
            cur.execute(sql, [filmworks_to_update])
            count = 0
            while True:
                count += self.chunk_size
                logger.info(f'{count}/{len(filmworks_to_update)}')
                rows = cur.fetchmany(self.chunk_size)
                columns = [col[0] for col in cur.description]
                if not rows:
                    cur.close()
                    break
                yield [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _extract_filmworks(date, cursor):
        sql = SQL_FILMWORK_IDS
        if date is None:
            sql = sql.format(filter=" ")
        else:
            sql = sql.format(filter="WHERE fw.modified >  %s ")
        cursor.execute(sql, [date])
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    @staticmethod
    def _extract_filmworks_by_persons(date, cursor):
        sql = SQL_FILMWORK_IDS_BY_PERSONS
        if date is None:
            sql = sql.format(filter=" ")
        else:
            sql = sql.format(filter="WHERE p.modified >  %s ")
        cursor.execute(sql, [date])
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    @staticmethod
    def _extract_filmworks_by_genres(date, cursor):
        sql = SQL_FILMWORK_IDS_BY_GENRES
        if date is None:
            sql = sql.format(filter=" ")
        else:
            sql = sql.format(filter="WHERE g.modified >  %s ")
        cursor.execute(sql, [date])
        rows = cursor.fetchall()
        return [row[0] for row in rows]
