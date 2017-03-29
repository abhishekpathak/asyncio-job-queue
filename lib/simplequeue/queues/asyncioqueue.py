import asyncio
from .abstractqueue import AbstractQueue


class AsyncioQueue(AbstractQueue):

    def __init__(self, name="test", __name__=None, namespace="queue", broker=None, backend=None):
        self.name = name
        self.__name__ = __name__
        self.namespace = namespace
        self.broker = asyncio.Queue()
        self.backend = dict()

    def enqueue(self, item):
        self.broker.put_nowait(item)

    def dequeue(self):
        self.broker.get()

    def put_result(self, id, result):
        self.backend[id] = result

    def get_result(self, id):
        return self.backend.get(id, None)
