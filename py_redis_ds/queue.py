from py_redis_ds.common import *
from py_redis_ds.collections import Deque
import queue as pyqueue

class Queue(RedisDsInterface, pyqueue.Queue):
    """
    ! Lot of incomplete stuff here.
    """
    def __init__(self, name, redis: Redis, maxsize:int=0):
        raise NotImplementedError
        super().__init__(name, redis)
        self.maxsize = maxsize
        self.queue = Deque(self.name, self.redis, maxsize)

        # self.mutex = redis.Lock(redis, self.name + '_mutex')