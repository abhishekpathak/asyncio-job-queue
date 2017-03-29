from .abstractqueue import AbstractQueue
import redis


class RedisQueue(AbstractQueue):

    def __init__(self, name="test", __name__=None, namespace="queue", broker="redis://localhost:6379:0", backend="redis://localhost:6379:1"):
        self.broker = broker
        self.backend = backend
        self.__name__ = __name__
        broker_host, broker_port, broker_db = self._parse_url(broker)
        backend_host, backend_port, backend_db = self._parse_url(backend)
        self._brokerdb = redis.Redis(host=broker_host, port=broker_port, db=broker_db)
        self._backenddb = redis.Redis(host=backend_host, port=backend_port, db=backend_db)
        self.queuekey = '%s:%s' % (namespace, name)

    def enqueue(self, item):
        self._brokerdb.rpush(self.queuekey, item)

    def dequeue(self, block=True, timeout=None):
        item = self._brokerdb.lpop(self.queuekey)
        return item

    def put_result(self, id, result):
        self._backenddb.set(id, result)

    def get_result(self, id):
        return str(self._backenddb.get(id))

    def _parse_url(self, url):
        """ Parse the url to obtain redis host, port and db information.

        Args : 
            url : the standard URL for a redis connection

        Returns : 
            host : the redis hostname
            port : redis port
            db : redis DB
        Raises : 
            ValueError, if the URL is not parsable.
        """
        try:
            assert url.split("//")[0].lower() == "redis:"
            redis_params = url.split("//")[1].split(":")
            host = redis_params[0]
            port = redis_params[1]
            db = redis_params[2]
            return host, port, db
        except Exception as e:
            raise ValueError("url not parsable.\n{}".format(e))
