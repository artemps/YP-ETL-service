import logging
import time
from datetime import datetime, timedelta

import psycopg2
import redis
import elasticsearch

from utils.config import (
    AppConfig, DataBaseConfig,
    ElasticConfig, RedisConfig,
)
from etl_modules.extractor import Extractor
from etl_modules.transformer import Transformer
from etl_modules.loader import Loader
from utils.state_storage import State, RedisStorage
from utils.connection import backoff

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@backoff(exceptions=[
    psycopg2.OperationalError,
    redis.exceptions.ConnectionError,
    elasticsearch.exceptions.ConnectionError
])
def run_etl(extractor: Extractor, transformer: Transformer, loader: Loader, state_storage: State) -> None:
    last_start_time = state_storage.get_state('last_start_time')
    is_running = state_storage.get_state('is_running')
    completed_ids = state_storage.get_state('completed_ids') or []

    if is_running:
        logger.info('ETL process is already running or not yet completed')
        return
    else:
        state_storage.set_state('is_running', True)

    logger.info(f'Start updating from {last_start_time}')
    for data in extractor.extract_filmworks(last_start_time, completed_ids):
        transformed_data = transformer.transform_filmworks(data)
        completed_ids = loader.load_filmworks(transformed_data)
        completed_ids.extend(completed_ids)
        state_storage.set_state('completed_ids', completed_ids)

    state_storage.set_state('last_start_time', datetime.now().strftime("%m-%d-%Y %H:%M:%S"))
    state_storage.set_state('completed_ids', [])
    state_storage.set_state('is_running', False)

    for data in extractor.extract_genres():
        transformed_data = transformer.transform_genres(data)
        loader.load_genres(transformed_data)


if __name__ == '__main__':
    app_config = AppConfig()

    state_storage = State(
        storage=RedisStorage(redis_adapter=RedisConfig().get_redis_client()))

    extractor = Extractor(
        chunk_size=app_config.chunk_size,
        database_config=DataBaseConfig()
    )

    transformer = Transformer()

    loader = Loader(
        elastic_config=ElasticConfig(),
    )

    while True:
        logger.info('ETL started...')
        run_etl(extractor, transformer, loader, state_storage)
        logger.info('ETL process is finished. Sleep...')
        time.sleep(app_config.sleep_time)
