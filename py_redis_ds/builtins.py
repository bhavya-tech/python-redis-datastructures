
from py_redis_ds.common import *
from typing import Set as SetType

class List(RedisDsInterface, list):

    def append(self, object: T):
        self.redis.rpush(self.name, object)

    def extend(self, iterable: Iterable[T]):
        self.redis.rpush(self.name, *iterable)

    def insert(self, index: int, value: T):
        self.redis.linsert(self.name, 'BEFORE', self.redis.lindex(self.name, index), value)

    def remove(self, value: T):
        self.redis.lrem(self.name, 0, value)

    def pop(self, index):
        raise NotImplementedError
    
    def clear(self):
        self.redis.delete(self.name)

    def index(self, value: T, start: int, stop: int) -> int:
        return self.redis.lrange(self.name, start, stop).index(value)
    
    def count(self, value: T) -> int:
        return self.redis.lrange(self.name, 0, -1).count(value)
    
    def sort(self, reverse: bool = False):
        """
        Func cannot be used by redis, can be done at python level if needed.
        """
        self.redis.sort(self.name, desc=reverse)
    
    def reverse(self):
        self.redis.lrange(self.name, 0, -1).reverse()
    
    def _fetch(self) -> list:
        return self.redis.lrange(self.name, 0, -1)

    def __len__(self) -> int:
        return self.redis.llen(self.name)
    
    def __iter__(self) -> iter:
        return iter(self.redis.lrange(self.name, 0, -1))

    def __getitem__(self, index: int) -> T:
        return self.redis.lindex(self.name, index)
    
    def __setitem__(self, index: int, value: T):
        self.redis.lset(self.name, index, value)

    def __delitem__(self, index):
        # self.redis.lrem(self.name, 0, self.redis.lindex(self.name, index))
        raise NotImplementedError
    
    def __contains__(self, value) -> bool:

        # TODO: Implement this in a better way
        return value in self.redis.lrange(self.name, 0, -1)
    

class Dict(RedisDsInterface, dict):

    def copy(self) -> dict:
        return self.redis.hgetall(self.name)

    def keys(self) -> [KT]:
        """
        ! The return type is different than builtin.
        in the builtin, it returns a dict_keys object.
        """
        return list(self.redis.hkeys(self.name))
    
    def values(self) -> [VT]:
        """
        ! The return type is different than builtin.
        in the builtin, it returns a dict_values object.
        """
        return list(self.redis.hvals(self.name))
    
    def items(self):
        return self._fetch().items()
    
    def get(self, key: KT, default: VT = None) -> Union[VT, None]:
        return self._fetch_item(key) or default
    
    def pop(self, key: KT, default: VT = None) -> Union[VT, None]:
        val = self._fetch_item(key) or default
        self.redis.hdel(self.name, key)
        return val
    
    def __len__(self) -> int:
        return self.redis.hlen(self.name)
    
    def __getitem__(self, key: KT) -> VT:
        val = self._fetch_item(key)

        if val is None:
            if hasattr(self, '__missing__'):
                return self.__missing__(key)
            else:
                raise KeyError(f'Key {key} not found')
        
        return val
    
    def __setitem__(self, key: KT, value: VT):
        self.redis.hset(self.name, key, value)

    def __delitem__(self, key: KT):
        return self.redis.hdel(self.name, key)
    
    def __iter__(self):
        return iter(self._fetch())
    
    def __reversed__(self):
        return reversed(self._fetch())

    def __delete__(self, key: KT):
        return self.redis.hdel(self.name, key)
    
    def __contains__(self, key: KT):
        return self.redis.hexists(self.name, key)
    
    def _fetch(self):
        return self.redis.hgetall(self.name)
    
    def _fetch_item(self, key: KT) -> VT:
        """
        ! The return type of hget is sus.
        """
        return self.redis.hget(self.name, key)
    

class Set(RedisDsInterface, set):
    """
    ! Needs thorough testing.
    ! Few implementations and func signature are wrong.
    """

    def add(self, element: T):
        self.redis.sadd(self.name, element)

    def copy(self) -> SetType[T]:
        return set(self.redis.smembers(self.name))
    
    def difference(self, *s: Iterable[Any]) -> SetType[T]:
        return self.redis.sdiff(self.name, list(s))
    
    def difference_update(self, *s: Iterable[Any]):
        self.redis.sdiffstore(self.name, list(s))

    def discard(self, element: T):
        self.redis.srem(self.name, element)

    def intersection(self, *s: Iterable[Any]) -> SetType[T]:
        return set(self.redis.sinter(self.name, list(s)))
    
    def intersection_update(self, *s: Iterable[Any]):
        self.redis.sinterstore(self.name, list(s))

    def isdisjoint(self, *s: Iterable[Any]) -> bool:
        return not bool(self.intersection(*s))
    
    def issubset(self, other):
        # return self.redis.sismember(self.name, list(other))
        raise NotImplementedError
    
    def issuperset(self, other):
        # return self.redis.sismember(list(other), self.name)
        raise NotImplementedError
    
    def remove(self, value: T):
        self.redis.srem(self.name, value)
    
    def symmetric_difference(self, s: Iterable[T]) -> SetType[T]:
        return self.union(s) - self.intersection(s)
    
    def symmetric_difference_update(self, s: Iterable[T]):
        # ! Can be improved
        # Might need pessimistic locking.

        # intersection = self.intersection(s)
        # self.update(s-intersection)

        raise NotImplementedError

    def union(self, *s: Iterable[Any]) -> SetType[T]:
        return set(self.redis.sunion(self.name, list(s)))
    
    def update(self, values: Iterable[T]):
        self.redis.sadd(self.name, *values)
    
    def clear(self):
        self.redis.delete(self.name)
    
    def discard(self, value: T):
        self.redis.srem(self.name, value)
    
    def __len__(self) -> int:
        return self.redis.scard(self.name)
    
    def __iter__(self) -> iter:
        return iter(self.redis.smembers(self.name))
    
    def __contains__(self, value: object) -> bool:
        return self.redis.sismember(self.name, value)