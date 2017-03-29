import time
import random
import sys
import logging
sys.path.append("/Users/abhishek.p/code/personal/job-queue/lib/")
from simplequeue.decorators import task
from simplequeue.sq import SQ
from simplequeue import settings

# the fully qualified queue name. Sufficient to accurately identify and get connection to a queue.
queue = '%s.%s' % (settings.backend, settings.name)

# configure the logging.
numeric_log_level = getattr(logging, settings.LOG_LEVEL.upper(), None)
if not isinstance(numeric_log_level, int):
    raise ValueError('Invalid log level: %s' % settings.LOG_LEVEL)
logging.basicConfig(filename=settings.LOG_FILE, level=numeric_log_level)


@task("app", queue)
def add(a, b):
    """ a sample task.
    """
    return a + b


@task("app", queue)
def multiply(a, b):
    """ a sample task.
    """
    return a * b


def test_add_jobs(n):
    """ a helper method to simulate pushing of jobs to queue.
    Args : 
        n : the number of jobs to create and push to the queue.

    Returns : 
        none

    Raises : 
        none
    """
    for i in range(n + 1):
        # randomly enqueue an add or a multiply job to the queue.
        option = random.randint(1, 3)
        if option == 1 or option == 3:
            job = add.delay(random.randint(0, 10), random.randint(11, 100))
        else:
            job = multiply.delay(random.randint(0, 10), random.randint(11, 100))
        time.sleep(10)  # wait for a few seconds before fetching the result
        logging.info(job.result)


def init():
    """ create the queue and spawn the workers
    """
    myqueue = SQ(name=settings.name, backend=settings.backend)
    myqueue.spawn_workers()


if sys.argv[1] == "producer":
    sys.argv[1] = None
    init()
    test_add_jobs(10)
