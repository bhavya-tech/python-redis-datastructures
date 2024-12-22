from redis import Redis
import redis
from typing import TypeVar, Iterable, Type, Any, Union

T = TypeVar('T')
VT = TypeVar('VT')
KT = TypeVar('KT')

class RedisDsInterface:
    def __init__(self, name: str, redis: Redis):
        self.name = name
        self.redis = redis

    def _fetch(self):
        raise NotImplementedError
    
    def _clear(self):
        self.redis.delete(self.name)

    def __str__(self) -> str:
        return str(self._fetch())
    
    def __repr__(self) -> str:
        return repr(self._fetch())
