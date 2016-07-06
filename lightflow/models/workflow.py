import copy
from time import sleep
import importlib

from .dag import Dag
from .exceptions import ImportWorkflowError, RequestActionUnknown, DagNameUnknown
from .signal import Server, Response
from lightflow.logger import get_logger
from lightflow.celery_tasks import dag_celery_task

logger = get_logger(__name__)


class Workflow:
    """ A workflow manages the execution and monitoring of dags.

    A workflow is a container for one or more dags. It is responsible for creating,
    running and monitoring dags.

    It is also the central server for the signal system, handling the incoming
    requests from dags and tasks.

    Please note: this class has to be serialisable (e.g. by pickle)
    """
    def __init__(self, polling_time=0.5):
        """ Initialise the workflow.

        Args:
            polling_time (float): The waiting time between status checks of the running
                                  dags in seconds.
        """
        self._polling_time = polling_time

        self._dags_blueprint = {}
        self._dags_running = []
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
            for key, dag in workflow_module.__dict__.items():
                if isinstance(dag, Dag):
                    self._dags_blueprint[dag.name] = dag
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

        # create the server for the signal service and start listening for requests
        signal_server = Server()
        signal_server.bind()

        # start all dags with the autostart flag set to True
        for name, dag in self._dags_blueprint.items():
            if dag.autostart:
                self._queue_dag(name, signal_server)

        # as long as there are dags in the list keep running
        while self._dags_running:
            sleep(self._polling_time)

            # handle new requests from dags and tasks
            break_counter = 0
            request = signal_server.receive()
            while (request is not None) and (break_counter < 10):
                signal_server.send(self._handle_request(request, signal_server))
                request = signal_server.receive()
                break_counter += 1

            # remove any dags that finished running
            for dag in reversed(self._dags_running):
                if dag.ready():
                    self._dags_running.remove(dag)

    def _queue_dag(self, name, signal_server, data=None):
        if name not in self._dags_blueprint:
            raise DagNameUnknown()

        self._dags_running.append(
            dag_celery_task.apply_async(
                (copy.deepcopy(self._dags_blueprint[name]),
                 self._workflow_id, signal_server.info(), data),
                queue='dag',
                routing_key='dag'
            )
        )

    def _handle_request(self, request, signal_server):
        if request is None:
            return Response(success=False)

        action_map = {
            'run_dag': self._handle_run_dag
        }

        if request.action in action_map:
            return action_map[request.action](request, signal_server)
        else:
            raise RequestActionUnknown()

    def _handle_run_dag(self, request, signal_server):
        self._queue_dag(request.payload['name'],
                        signal_server,
                        request.payload['data'])
        return Response(success=True)
