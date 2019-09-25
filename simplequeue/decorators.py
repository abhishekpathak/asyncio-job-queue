import uuid
import json
import logging
from simplequeue.job import Job
from simplequeue import utils


def task(module=None, queue=None):
    """ Decorates a normal function as a task.
    
    Adds a delay method to any function, which enables it to be pushed to a job queue for background processing.
    Accepts two params, module and queue, which are necessary to be manually told to this decorator. In addition to this, it has access to the decorated function, and its args and kwargs like usual.

    Args : 
        module : the module of the file where the function is located. This is necessary to be able to import when the function is run from the __main__ module.
        queue : the fully qualified queuename (backend.name).

    Returns : 
        the decorated function, with a delay function added as an attribute.

    Raises : 
        none
    """

    def wrapper(f):
        def delay(*args, **kwargs):
            job = Job(id=str(uuid.uuid4()), module=module, func=f, queue=queue, args=args, kwargs=kwargs)
            logging.info("putting job {} in the job queue".format(job.id))
            backend, name = queue.split('.')[0], queue.split('.')[1]
            qObj = utils.deserialize(name, backend)
            qObj.enqueue(json.dumps(job.__dict__))
            return job
        f.delay = delay
        return f
    return wrapper
