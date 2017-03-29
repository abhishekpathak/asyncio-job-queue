from abc import ABC, abstractmethod


class AbstractQueue(ABC):
    """ An abstract queue class.

    Provides a consistent interface to all the different types of queues. This will ensure that any queue implementing this interface can be plugged into the system easily.

    Attributes :
        name : name of the queue.
        namespace : namespace as desired for the queue.
        broker : a string (preferably a URL) that can be parsed to get all the information about the system orchestrating as a broker to persist messages.
        backend : a string (preferably a URL) that can be parsed to get all the necessary information about the system in which results of successful job executions are stored.

    Notes :
    1. the intended usage of namespace.name is to uniquely identify a queue in a storage engine. 
    2. Parsing of these broker and backend strings is the responsibility of the implementors of this class.
    """
    def __init__(self, name=None, namespace=None, broker=None, backend=None):
        self.broker = broker
        self.backend = backend

    @abstractmethod
    def enqueue(self, item):
        """ Enqueue a job to the queue.
        Args : 
            item: a serialized job instance.

        Returns : 
            none

        Raises : 
            none
        """
        pass

    @abstractmethod
    def dequeue(self):
        """ Dequeue a job from the queue. Do not block if the queue is empty.
        Args : 
            none

        Returns : 
            a serialized job instance.

        Raises : 
            none
        """
        pass

    @abstractmethod
    def put_result(self, id, result):
        """ Store the result in results archive.

        Args : 
            id : the key against which the result is to be stored. Typically job id.
            result : result of the job execution (ideally to be stored as a string to provide uniformity across data types.).

        Returns : 
            none

        Raises : 
            none
        """
        pass

    @abstractmethod
    def get_result(self, id):
        """ Get the result from the results archive. Return None if not found.
        Args : 
            id : the job id.

        Returns : 
            the job result, ideally a string.

        Raises : 
            none
        """
        pass

