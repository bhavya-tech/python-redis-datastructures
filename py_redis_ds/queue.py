from py_redis_ds.common import *
from py_redis_ds.collections import Deque
from py_redis_ds.builtins import List
import queue as pyqueue
import uuid
import pickle

class Queue(RedisDsInterface, pyqueue.Queue):
    def __init__(self, name, redis: Redis, maxsize:int=0):
        super().__init__(name, redis)
        self.maxsize = maxsize
        self._init(maxsize)

    # Remove the unsupported methods.
    task_done = None
    join = None

    def qsize(self) -> int:
        return self._qsize()
    
    def empty(self) -> bool:
        return not self._qsize()
    
    def full(self) -> bool:
        return 0 < self.maxsize <= self._qsize()
    
    def put(self, item: T):
        if self.full():
            raise pyqueue.Full
        else:
            self._put(item)

    def get(self) -> T:
        if self.empty():
            raise pyqueue.Empty
        else:
            return self._get()

    # Since put and get here are no wait by default.
    put_nowait = put
    get_nowait = get

    def _put(self, item: T):
        self.queue.append(item)

    def _get(self):
        return self.queue.popleft()
    
    def _init(self, maxsize):
        self.queue = Deque(self.name, self.redis, maxsize)
    
    def _qsize(self) -> int:
        return len(self.queue)
    
    def _fetch(self) -> list:
        return self.queue._fetch()
    
    def _clear(self):
        self.queue._clear()


class PriorityQueue(Queue, pyqueue.PriorityQueue):
    """
    This implelentation is based on redis sorted set.
    So it is vastly different from the builtin PriorityQueue.

    This will assume that the items are tuples of (priority, item).    
    """

    def __init__(self, name, redis, maxsize = 0):
        super().__init__(name, redis, maxsize)

    def _init(self, maxsize):
        """
        Since we are going to use sorted set as priority queue,
        we don't need to define any proxy datastructure.
        """
        pass

    def put(self, item: tuple[int, T]):
        return super().put(item)

    def get(self) -> tuple[int, T]:
        return super().get()    

    def _put(self, item: tuple[int, T]):
        priority = item[0]
        item_to_add = (item[1], str(uuid.uuid4()))
        item_to_add = pickle.dumps(item_to_add)
        self.redis.zadd(self.name, {item_to_add: priority})

    def _fetch(self) -> list[tuple[int, T]]:
        rec = self.redis.zrange(self.name, 0, -1, withscores=True)
        return [(score, pickle.loads(item)[0]) for item, score in rec]

    def _get(self) -> tuple[int, T]:
        item_added_picklized, score = self.redis.zpopmin(self.name, 1)[0]
        item_added = pickle.loads(item_added_picklized)
        return (score, item_added[0])
    
    def _qsize(self) -> int:
        return self.redis.zcard(self.name)


class LifoQueue(Queue, pyqueue.LifoQueue):

    def _init(self, maxsize):
        pass

    def _fetch(self) -> list[T]:
        return self.redis.lrange(self.name, 0, -1)
    
    def _qsize(self):
        return self.redis.llen(self.name)

    def _get(self) -> T:
        return self.redis.lpop(self.name)

    def _put(self, item: T):
        self.redis.lpush(self.name, item)