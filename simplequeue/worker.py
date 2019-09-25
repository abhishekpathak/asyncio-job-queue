import asyncio
import uuid
import random
import json
import asyncio
import pdb
import sys
import logging
from simplequeue.job import Job
from simplequeue import utils


class Worker(object):
    """ Fetches and executes a job from the queue.

    A worker is responsible for actually executing a job.It knows how to pop a job from the queue, serialize it and then parse it to extract the actual task (function). It then executes the function and writes the result back to the backend for retreival by the job creator.
    Our workers leverage asyncio and work concurrently!

    Attributes:
        queue :  the queue object the worker listens on.
        n : the number of workers in a pool.
    """
    def __init__(self, queue, n):
        self.queue = queue
        self.poolsize = int(n)

    async def run_func(self, job):
        """ executes the actual task function of the job.

        This is intentionally made a coroutine - even if the actual task is blocking, it illustrates cooperative scheduling between workers.

        Args : 
            job : the job object containing the task and its metadata

        Returns : 
            the result of the task execution

        Raises : 
            none
        """
        await asyncio.sleep(2)
        return getattr(__import__(job.module), job.func)(*job.args, **job.kwargs)

    async def watch(self, i):
        """ Watches the queue for any new tasks.

        This again is a non-blocking method, so if the queue is empty, other workers can start working where they left off.
        If the queue contains a job, retrieve it, serialize it and pass it to the runner function. Then fetch the result and persist it in the result db.

        Args : 
            i : the worker id.

        Returns : 
            none

        Raises : 
            none
        """
        while True:
            next = self.queue.dequeue()
            if next:
                job = Job.deserialize_job(next)
                logging.info("executing job {} by worker {}".format(job.id, i))
                result = await self.run_func(job)
                self.queue.put_result(job.id, result)
            else:
                await asyncio.sleep(0.1)

    def run(self):
        """ Puts all the workers to work. Asks each one to start watching the queue.

        Args : 
            none

        Returns : 
            an array of watch coroutines.

        Raises : 
            none
        """
        workers = []
        for i in range(self.poolsize + 1):
            logging.debug("worker {} started. Listening for events.".format(i))
            workers.append(self.watch(i))
        return workers
