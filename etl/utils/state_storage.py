import json

from typing import Any, Dict
from redis import Redis


class BaseStorage:
    """
    Интерфейс хранилища состояние.
    Позволяет сохранять и получать состояние.
    """

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        raise NotImplementedError()

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        raise NotImplementedError()


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        data = self.storage.retrieve_state()
        data.update({key: value})
        self.storage.save_state(data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        data = self.storage.retrieve_state()
        return data.get(key, None)


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: Dict[str, Any]) -> None:
        self.redis_adapter.set('data', json.dumps(state))

    def retrieve_state(self) -> Dict[str, Any]:
        data = self.redis_adapter.get('data')
        if data:
            return json.loads(data)
        return {}
