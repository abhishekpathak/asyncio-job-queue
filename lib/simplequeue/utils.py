from simplequeue.queues.redisqueue import RedisQueue
from simplequeue.queues.asyncioqueue import AsyncioQueue


def deserialize(name, backend):
    """ Given a name and backend, get a queue object

    Args : 
        name (string) : the name of the queue
        backend (string) : the backend (persistance layer)

    Returns : 
        A queue instance with the given name on the given backend.

    Raises : 
        ValueError : when the method is called with an unknown backend.
    """
    if not backend or backend.lower() == "asyncio":
        return AsyncioQueue(name=name)
    elif backend.lower() == "redis":
        return RedisQueue(name=name)
    else:
        raise ValueError("no suitable backend provided.")
