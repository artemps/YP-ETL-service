import logging
from contextlib import contextmanager
from functools import wraps
from time import sleep

import psycopg2
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


@contextmanager
def postgresql_connection(dsl: dict):
    """Контекстный менеджер для управлени соединением с PostgreSQL"""
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    yield conn
    conn.close()


@contextmanager
def elastic_connection(database: str):
    """Контекстный менеджер для управлени соединением с Elasticsearch"""
    conn = Elasticsearch(database)
    yield conn
    conn.close()


def backoff(exceptions=None, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param exceptions: список ошибок при которых стоит делать повторные попытки
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if exceptions and type(e) not in exceptions:
                        raise e
                    logger.info(f'Connection error {e}Retrying...')
                    sleep(sleep_time)
                    sleep_time *= factor
                    sleep_time = min(sleep_time, border_sleep_time)

        return inner
    return func_wrapper
