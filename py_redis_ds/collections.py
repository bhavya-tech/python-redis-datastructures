from py_redis_ds.common import *
from py_redis_ds.builtins import Dict
from collections import deque, defaultdict

class Deque(RedisDsInterface, deque):
    maxlen: int | None = None

    def __init__(self, name: str, redis: Redis, maxlen: int = None):
        super().__init__(name, redis)
        self.maxlen = maxlen
    
    def append(self, object: T):
        self.redis.rpush(self.name, object)

    def appendleft(self, x: T):
        self.redis.lpush(self.name, x)

    def clear(self):
        self._clear()

    def count(self, x: T) -> int:
        return self.redis.lrange(self.name, 0, -1).count(x)
    
    def extend(self, iterable: Iterable[T]):
        self.redis.rpush(self.name, *iterable)

    def extendleft(self, iterable: Iterable[T]):
        self.redis.lpush(self.name, *iterable)

    def index(self, x, start = 0, stop = ...):
        return self.redis.lrange(self.name, start, stop).index(x)
    
    def insert(self, i: int, x: T):
        """
        Would not raise exception if maxlen is reached.
        """

        self.redis.linsert(self.name, 'BEFORE', self.redis.lindex(self.name, i), x)

    def pop(self):
        return self._pop(self.redis.rpop)

    def popleft(self):
        return self._pop(self.redis.lpop)    
        
    def remove(self, value: T):
        if not self.redis.lrem(self.name, 1, value):
            raise ValueError(f'{value} not in deque')
        
    def reverse(self):
        raise NotImplementedError
    
    def rotate(self, n: int):
        raise NotImplementedError
    
    def _fetch(self) -> list:
        return list(self.redis.lrange(self.name, 0, -1))

    def _pop(self, func):
        if (val := func(self.name)) is None:
            raise IndexError('pop from an empty deque')
        else:
            return val


class Defaultdict(Dict, defaultdict):

    def __init__(self, name, redis, default_factory=None):
        super().__init__(name, redis)
        self.default_factory = default_factory

    def __missing__(self, key):
        
        if self.default_factory == None:
            raise KeyError((key,))
        else:
            val = self.default_factory()
            self[key] = val
            return val
