import copy
import time
import importlib

from .dag import Dag
from .exceptions import ImportWorkflowError
from lightflow.logger import get_logger
from lightflow.celery_tasks import dag_celery_task

logger = get_logger(__name__)


class Workflow:
    """ A workflow manages the execution and monitoring of dags.

    A workflow is a container for one or more dags. It is responsible for running and
    monitoring dags.

    Please note: this class has to be serialisable (e.g. by pickle)
    """
    def __init__(self, polling_time=0.5):
        """ Initialise the workflow.

        Args:
            polling_time (float): The waiting time between status checks of the running
                                  dags in seconds.
        """
        self._polling_time = polling_time

        self._dags = []
        self._workflow_id = None
        self._name = None

    @classmethod
    def from_name(cls, name, polling_time=0.5):
        """ Create a workflow object from a workflow script.

        Args:
            name (str): The name of the workflow script.
            polling_time (float): The waiting time between status checks of the running
                                  dags in seconds.

        Returns:
            Workflow: A fully initialised workflow object
        """
        new_workflow = cls(polling_time)
        new_workflow.load(name)
        return new_workflow

    @property
    def name(self):
        """ Returns the name of the workflow.

        Returns:
            str: The name of the workflow.
        """
        return self._name

    def load(self, name):
        """ Import the workflow script and load all dags.

        Args:
            name (str): The name of the workflow script.

        Raises:
            ImportWorkflowError: If the import of the workflow fails.
        """
        try:
            workflow_module = importlib.import_module(name)
            self._dags = [dag for key, dag in workflow_module.__dict__.items() if
                          isinstance(dag, Dag)]
            self._name = name
        except TypeError:
            logger.error('Cannot import workflow {}!'.format(name))
            raise ImportWorkflowError('Cannot import workflow {}!'.format(name))

    def run(self, data_store):
        """ Run all autostart dags in the workflow.

        Only the dags that are flagged as autostart are started. If a unique workflow id
        hasn't been assigned to this workflow yet, it is requested from the data store.

        Args:
            data_store (DataStore): A DataStore object that is fully initialised and
                        connected to the persistent data storage.
        """
        if data_store.exists(self._workflow_id):
            logger.info('Using existing workflow ID: {}'.format(self._workflow_id))
        else:
            self._workflow_id = data_store.add(self._name)
            logger.info('Created workflow ID: {}'.format(self._workflow_id))

        running = []
        for dag in self._dags:
            if not dag.autostart:
                continue

            # schedule a deep copy of the dag for execution in order to allow
            # multiple copies of the same dag to be run in parallel.
            running.append(dag_celery_task.delay(copy.deepcopy(dag),
                                                 workflow_id=self._workflow_id))

        while running:
            time.sleep(self._polling_time)

            for dag_result in reversed(running):
                if dag_result.ready():
                    running.remove(dag_result)
