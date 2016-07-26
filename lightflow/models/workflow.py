import sys
import copy
from time import sleep
import importlib

from .dag import Dag
from .exceptions import ImportWorkflowError, RequestActionUnknown, RequestFailed,\
    DagNameUnknown
from .signal import Response
from lightflow.logger import get_logger
from lightflow.celery_tasks import dag_celery_task

MAX_SIGNAL_REQUESTS = 10

logger = get_logger(__name__)


class Workflow:
    """ A workflow manages the execution and monitoring of dags.

    A workflow is a container for one or more dags. It is responsible for creating,
    running and monitoring dags.

    It is also the central server for the signal system, handling the incoming
    requests from dags, tasks and the library frontend (e.g. cli, web).

    The workflow class hosts the current stop flag for itself and a list of dags
    that should be stopped.

    Please note: this class has to be serialisable (e.g. by pickle)
    """
    def __init__(self, clear_data_store=True, polling_time=0.5):
        """ Initialise the workflow.

        Args:
            clear_data_store (bool): Remove any documents created during the workflow
                                     run in the data store after the run.
            polling_time (float): The waiting time between status checks of the running
                                  dags in seconds.
        """
        self._clear_data_store = clear_data_store
        self._polling_time = polling_time

        self._dags_blueprint = {}
        self._dags_running = []
        self._workflow_id = None
        self._name = None

        self._stop_workflow = False
        self._stop_dags = []

    @classmethod
    def from_name(cls, name, clear_data_store=True, polling_time=0.5):
        """ Create a workflow object from a workflow script.

        Args:
            name (str): The name of the workflow script.
            clear_data_store (bool): Remove any documents created during the workflow
                                     run in the data store after the run.
            polling_time (float): The waiting time between status checks of the running
                                  dags in seconds.

        Returns:
            Workflow: A fully initialised workflow object
        """
        new_workflow = cls(clear_data_store, polling_time)
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
            del sys.modules[name]
        except TypeError:
            logger.error('Cannot import workflow {}!'.format(name))
            raise ImportWorkflowError('Cannot import workflow {}!'.format(name))

    def run(self, data_store, signal_server, workflow_id):
        """ Run all autostart dags in the workflow.

        Only the dags that are flagged as autostart are started. If a unique workflow id
        hasn't been assigned to this workflow yet, it is requested from the data store.

        Args:
            data_store (DataStore): A DataStore object that is fully initialised and
                        connected to the persistent data storage.
            signal_server (Server): A signal Server object that has been bound to a port
                                    number.
            workflow_id (str): A unique workflow id, that represents this workflow run
        """
        self._workflow_id = workflow_id

        # start all dags with the autostart flag set to True
        for name, dag in self._dags_blueprint.items():
            if dag.autostart:
                self._queue_dag(name, signal_server)

        # as long as there are dags in the list keep running
        while self._dags_running:
            sleep(self._polling_time)

            # handle new requests from dags, tasks and the library (e.g. cli, web)
            for i in range(MAX_SIGNAL_REQUESTS):
                request = signal_server.receive()
                if request is None:
                    break

                try:
                    response = self._handle_request(request, signal_server)
                    signal_server.send(response)
                except (RequestActionUnknown, RequestFailed):
                    signal_server.send(Response(success=False))

            # remove any dags that finished running
            for dag in reversed(self._dags_running):
                if dag.ready():
                    self._dags_running.remove(dag)

        if self._clear_data_store:
            data_store.remove(self._workflow_id)

    def _queue_dag(self, name, signal_server, data=None):
        """ Add a new dag to the queue.

        If the stop workflow flag is set, no new dag can be queued.

        Args:
            name (str): The name of the dag that should be queued.
            signal_server (Server): Reference to the main signal server object.
            data (MultiTaskData): The data that should be passed on to the new dag.

        Raises:
            DagNameUnknown: If the specified dag name does not exist

        Returns:
            bool: True if the dag was queued, otherwise False.
        """
        if self._stop_workflow:
            return False

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

        return True

    def _handle_request(self, request, signal_server):
        """ Handle an incoming request by forwarding it to the appropriate method.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request.
            signal_server (Server): Reference to the main signal server object.

        Raises:
            RequestActionUnknown: If the action specified in the request is not known.

        Returns:
            Response: A response object containing the response from the method handling
                      the request.
        """
        if request is None:
            return Response(success=False)

        action_map = {
            'run_dag': self._handle_run_dag,
            'stop_workflow': self._handle_stop_workflow,
            'stop_dag': self._handle_stop_dag,
            'is_dag_stopped': self._handle_is_dag_stopped
        }

        if request.action in action_map:
            return action_map[request.action](request, signal_server)
        else:
            raise RequestActionUnknown()

    def _handle_run_dag(self, request, signal_server):
        """ The handler for the run_dag request.

        The run_dag request creates a new dag and adds it to the queue.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request.
            signal_server (Server): Reference to the main signal server object.

        Returns:
            Response: A response object containing the following fields:
                          - success: True if a new dag was queued successfully.
        """
        result = self._queue_dag(request.payload['name'],
                                 signal_server,
                                 request.payload['data'])
        return Response(success=result)

    def _handle_stop_workflow(self, request, signal_server):
        self._stop_workflow = True
        for dag in self._dags_running:
            if dag.info['name'] not in self._stop_dags:
                self._stop_dags.append(dag.info['name'])
        return Response(success=True)

    def _handle_stop_dag(self, request, signal_server):
        if (request.payload['dag_name'] is not None) and \
           (request.payload['dag_name'] not in self._stop_dags):
            self._stop_dags.append(request.payload['dag_name'])
        return Response(success=True)

    def _handle_is_dag_stopped(self, request, signal_server):
        return Response(success=True,
                        payload={
                            'is_stopped': request.payload['dag_name'] in self._stop_dags
                        })
