
class TaskContext:
    """ This class contains information about the context the task is running in. """

    def __init__(self, task_name, dag_name, workflow_name, workflow_id, worker_hostname):
        """ Initialize the task context object.

        Args:
            task_name (str): The name of the task.
            dag_name (str): The name of the DAG the task was started from.
            workflow_name (str): The name of the workflow the task was started from.
            workflow_id (str): The id of the workflow this task is member of.
            worker_hostname (str): The name of the worker executing this task.
        """
        self.task_name = task_name
        self.dag_name = dag_name
        self.workflow_name = workflow_name
        self.workflow_id = workflow_id
        self.worker_hostname = worker_hostname

    def to_dict(self):
        """ Return the task context content as a dictionary. """
        return {
            'task_name': self.task_name,
            'dag_name': self.dag_name,
            'workflow_name': self.workflow_name,
            'workflow_id': self.workflow_id,
            'worker_hostname': self.worker_hostname
        }
