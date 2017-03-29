import asyncio
import multiprocessing as mp
import logging
import subprocess
import shlex
from simplequeue.worker import Worker
from simplequeue import utils


class SQ(object):
    """ Represents a simple queue.

    This class holds all the information necessary to correctly identify a
    queue on the system. It has the tools to instantiate/retrieve a DB connection. SQ is the one which spawns the workers as well.

    Attributes :
        name : the name of the queue
        backend :  the persistant storage that we want to use for the queue. As of now two values are supported : redis and asyncio. Asyncio is the default mode.
    """

    # TODO make backend values an enum instead of strings

    def __init__(self, name, backend=None):
        self.name = name.lower()
        if not backend:
            self.backend = "asyncio"
        else:
            self.backend = backend.lower()

    def spawn_workers(self):
        """ Externally available method to spawn workers.

        Spawns workers in the same process, or creates a new process depending upon the backend choice.

        Args : 
            none

        Returns : 
            none

        Raises : 
            ValueError

        """
        if self.backend == "asyncio":
            self._spawn_workers_in_same_process()
        elif self.backend == "redis":
            self._spawn_workers_in_new_process()
        else:
            raise ValueError("Spawning workers : backend {} is not supported".format(self.backend))

    @staticmethod
    def _run(queue, n):
        """ Creates a worker pool and registers it to an event loop.

        Args :
            queue : a queue object (any child instance of an Abstract Queue)
            n : the number of workers to spawn.

        Returns :
            none

        Raises :
            none
        """
        w = Worker(queue, n)
        worker_loop = asyncio.get_event_loop()
        worker_loop.run_until_complete(asyncio.gather(*w.run()))

    def _spawn_workers_in_same_process(self):
        """ Spawns workers in the same process.

        This method should somehow do an asyncio.gather of the producer and the worker coroutines and pass them to the application event loop.
        # TODO not working, temp stub.

        Args : 
            none

        Returns : 
            none

        Raises : 
            ValueError
        """
        SQ._run(self.queue, 3)

    def _spawn_workers_in_new_process(self):
        """ Spawns workers in a new process.

            Uses the multiprocessing module to create a new process.
            # TODO only fork mode is working, spawn mode is not.
            # TODO if spawn mode does not support, explore subprocess.Popen options.

        Args : 
            none

        Returns : 
            none

        Raises : 
            none
        """
        mp.set_start_method('fork')
        p = mp.Process(name="worker", target=SQ._run, args=(self.queue, 3), daemon=True)
        p.start()
        logging.debug("spawned a new process.")
        '''
        dir_path = os.path.dirname(os.path.realpath(__file__))
        command = "python app.py worker %s %s %d &" % (self.name, self.backend, 3)
        print(command)
        subprocess.run(shlex.split(command))
        '''

    @property
    def queue(self):
        """ get the queue object for this sq.

        Args : 
            none

        Returns : 
            a queue instance (AsyncioQueue, RedisQueue)

        Raises : 
            none
        """
        return utils.deserialize(self.name, self.backend)

    @property
    def queuename(self):
        """ little helper method to get the quantified queue name - a concatenation of the backend, and the actual queue name.

        Args : 
            none

        Returns : 
            String, concatenated backend and name

        Raises : 
            none
        """
        return "%s.%s" % (self.backend, self.name)
