from simplequeue import utils
import json


class Job(object):

    def __init__(self, id, module, func, queue, args=None, kwargs=None):
        """ The job template that is pushed to the queue.

        Contains the task to run, and all the other necessary metadata to assist in persisting the task and subsequently recreating and running it.
        This class needs to be json serializable. Prefer to use string representations of python objects as much as possible.

        Attributes:
            id (string) : the identifier of the job, typically a uuid.
            func (string) : the name of the function that is to be executed as a task.
            module (string) : the name of the module in which func resides.
            queue : the fully qualified queue name (backend.name).
            args : the args to pass on to func.
            kwargs : the kwargs to pass on to func.
        """
        self.id = id
        self.func = func.__name__
        self.module = module
        self.queue = queue
        self.args = args
        self.kwargs = kwargs

    @property
    def result(self):
        """ Fetches the result of the job. Where the result resides is opaque to this method, it is encapsulated by the queue implementation.
        Args : 
            none

        Returns : 
            the result of the job (string).

        Raises : 
            none
        """
        backend, name = self.queue.split('.')[0], self.queue.split('.')[1]
        return utils.deserialize(name, backend).get_result(self.id)

    @staticmethod
    def deserialize_job(serialized_job):
        """ Helper method that creates a job object from its serialized form.
        Note : Avoid using Job(**jobdict), we want to have flexibility in recreating the job, as the Job's attributes change, and new dependent attributes come in.

        Args : 
            jobdict : the serialized representation of a job, in our case a json object. 

        Returns : 
            A job object.

        Raises : 
            none
        """

        jobdict = json.loads(serialized_job)
        return Job(
            id=jobdict["id"],
            module=jobdict["module"],
            func=getattr(__import__(jobdict["module"]), jobdict["func"]), 
            queue=jobdict["queue"], 
            args=list(jobdict["args"]), 
            kwargs=dict(jobdict["kwargs"])
        )
