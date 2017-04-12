import sys
import copy
import inspect
import importlib
from time import sleep

from .dag import Dag
from .exceptions import (WorkflowImportError, WorkflowArgumentError,
                         RequestActionUnknown, RequestFailed, DagNameUnknown)
from .signal import Response
from .arguments import Arguments
from lightflow.logger import get_logger
from lightflow.queue.app import create_app
from lightflow.queue.const import JobExecPath, JobType

MAX_SIGNAL_REQUESTS = 10

logger = get_logger(__name__)


class Workflow:
    """ A workflow manages the execution and monitoring of dags.

    A workflow is a container for one or more dags. It is responsible for creating,
    starting and monitoring dags.

    It is also the central server for the signal system, handling the incoming
    requests from dags, tasks and the library API.

    The workflow class hosts the current stop flag for itself and a list of dags
    that should be stopped.

    Please note: this class has to be serialisable (e.g. by pickle)
    """
    def __init__(self, config, *, clear_data_store=True):
        """ Initialise the workflow.

        Args:
            config (Config): Reference to the configuration object from which the
                             settings for the workflow are retrieved.
            clear_data_store (bool): Remove any documents created during the workflow
                                     run in the data store after the run.
        """
        self._config = config
        self._clear_data_store = clear_data_store

        self._dags_blueprint = {}
        self._dags_running = []
        self._workflow_id = None
        self._name = None
        self._arguments = Arguments()
        self._provided_arguments = {}

        self._celery_app = None

        self._stop_workflow = False
        self._stop_dags = []

        self._docstring = None

    @classmethod
    def from_name(cls, name, config, *, clear_data_store=True, arguments=None):
        """ Create a workflow object from a workflow script.

        Args:
            name (str): The name of the workflow script.
            config (Config): Reference to the configuration object from which the
                             settings for the workflow are retrieved.
            clear_data_store (bool): Remove any documents created during the workflow
                                     run in the data store after the run.
            arguments (dict): Dictionary of additional arguments that are ingested
                              into the data store prior to the execution of the workflow.

        Returns:
            Workflow: A fully initialised workflow object
        """
        new_workflow = cls(config, clear_data_store=clear_data_store)
        new_workflow.load(name, arguments=arguments)
        return new_workflow

    @property
    def name(self):
        """ Returns the name of the workflow. """
        return self._name

    @property
    def docstring(self):
        """ Returns the docstring of the workflow or None if empty. """
        return self._docstring

    @property
    def config(self):
        """ Returns the workflow configuration. """
        return self._config

    @property
    def arguments(self):
        """ Returns the workflow list of arguments. """
        return self._arguments

    def load(self, name, *, arguments=None, strict_dag=False):
        """ Import the workflow script and load all known objects.

        The workflow script is treated like a module and imported
        into the Python namespace. After the import, the method looks
        for instances of known classes and stores a reference for further
        use in the workflow object.

        Args:
            name (str): The name of the workflow script.
            arguments (dict): Dictionary of additional arguments that are ingested
                              into the data store prior to the execution of the workflow.
            strict_dag (bool): If true then the loaded workflow module must contain an
                               instance of Dag.

        Raises:
            WorkflowArgumentError: If the workflow requires arguments to be set that
                                   were not supplied to the workflow.
            WorkflowImportError: If the import of the workflow fails.
        """
        try:
            workflow_module = importlib.import_module(name)

            dag_present = False

            # extract objects of specific types from the workflow module
            for key, obj in workflow_module.__dict__.items():
                if isinstance(obj, Dag):
                    self._dags_blueprint[obj.name] = obj
                    dag_present = True
                elif isinstance(obj, Arguments):
                    self._arguments.extend(obj)

            self._name = name
            self._docstring = inspect.getdoc(workflow_module)
            del sys.modules[name]

            if strict_dag and not dag_present:
                raise WorkflowImportError(
                    'Workflow does not include a dag {}'.format(name))

            # check whether all arguments have been specified
            if arguments is not None:
                missing_arguments = self._arguments.check_missing(arguments)
                if len(missing_arguments) > 0:
                    raise WorkflowArgumentError(
                        'The following arguments are required ' +
                        'by the workflow, but are missing: {}'.format(
                            ', '.join(missing_arguments)))

                self._provided_arguments = arguments

        except (TypeError, ImportError):
            logger.error('Cannot import workflow {}'.format(name))
            raise WorkflowImportError('Cannot import workflow {}'.format(name))

    def run(self, data_store, signal_server, workflow_id):
        """ Run all autostart dags in the workflow.

        Only the dags that are flagged as autostart are started.

        Args:
            data_store (DataStore): A DataStore object that is fully initialised and
                        connected to the persistent data storage.
            signal_server (Server): A signal Server object that receives requests
                                    from dags and tasks.
            workflow_id (str): A unique workflow id that represents this workflow run
        """
        self._workflow_id = workflow_id
        self._celery_app = create_app(self._config)

        # pre-fill the data store with supplied arguments
        args = self._arguments.consolidate(self._provided_arguments)
        for key, value in args.items():
            data_store.get(self._workflow_id).set(key, value)

        # start all dags with the autostart flag set to True
        for name, dag in self._dags_blueprint.items():
            if dag.autostart:
                self._queue_dag(name)

        # as long as there are dags in the list keep running
        while self._dags_running:
            if self._config.workflow_polling_time > 0.0:
                sleep(self._config.workflow_polling_time)

            # handle new requests from dags, tasks and the library (e.g. cli, web)
            for i in range(MAX_SIGNAL_REQUESTS):
                request = signal_server.receive()
                if request is None:
                    break

                try:
                    response = self._handle_request(request)
                    signal_server.send(response)
                except (RequestActionUnknown, RequestFailed):
                    signal_server.send(Response(success=False, uid=request.uid))

            # remove any dags and their result data that finished running
            for dag in reversed(self._dags_running):
                if dag.ready():
                    dag.forget()
                    self._dags_running.remove(dag)

        # remove the signal entry
        signal_server.clear()

        # delete all entries in the data_store under this workflow id, if requested
        if self._clear_data_store:
            data_store.remove(self._workflow_id)

    def _queue_dag(self, name, *, data=None):
        """ Add a new dag to the queue.

        If the stop workflow flag is set, no new dag can be queued.

        Args:
            name (str): The name of the dag that should be queued.
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

        new_dag = copy.deepcopy(self._dags_blueprint[name])
        new_dag.config = self._config
        new_dag.workflow_name = self.name
        self._dags_running.append(
            self._celery_app.send_task(JobExecPath.Dag,
                                       args=(new_dag, self._workflow_id, data),
                                       queue=JobType.Dag,
                                       routing_key=JobType.Dag
                                       )
        )

        return True

    def _handle_request(self, request):
        """ Handle an incoming request by forwarding it to the appropriate method.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request.

        Raises:
            RequestActionUnknown: If the action specified in the request is not known.

        Returns:
            Response: A response object containing the response from the method handling
                      the request.
        """
        if request is None:
            return Response(success=False, uid=request.uid)

        action_map = {
            'start_dag': self._handle_start_dag,
            'stop_workflow': self._handle_stop_workflow,
            'stop_dag': self._handle_stop_dag,
            'is_dag_stopped': self._handle_is_dag_stopped
        }

        if request.action in action_map:
            return action_map[request.action](request)
        else:
            raise RequestActionUnknown()

    def _handle_start_dag(self, request):
        """ The handler for the start_dag request.

        The start_dag request creates a new dag and adds it to the queue.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request. The payload has to contain the
                               following fields:
                                'name': the name of the dag that should be started
                                'data': the data that is passed onto the start tasks

        Returns:
            Response: A response object containing the following fields:
                          - success: True if a new dag was queued successfully.
        """
        result = self._queue_dag(name=request.payload['name'],
                                 data=request.payload['data'])
        return Response(success=result, uid=request.uid)

    def _handle_stop_workflow(self, request):
        """ The handler for the stop_workflow request.

        The stop_workflow request adds all running dags to the list of dags
        that should be stopped and prevents new dags from being started. The dags will
        then stop queueing new tasks, which will terminate the dags and in turn the
        workflow.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request.

        Returns:
            Response: A response object containing the following fields:
                          - success: True if the dags were added successfully to the list
                                     of dags that should be stopped.
        """
        self._stop_workflow = True
        for dag in self._dags_running:
            if dag.info['name'] not in self._stop_dags:
                self._stop_dags.append(dag.info['name'])
        return Response(success=True, uid=request.uid)

    def _handle_stop_dag(self, request):
        """ The handler for the stop_dag request.

        The stop_dag request adds a dag to the list of dags that should be stopped.
        The dag will then stop queueing new tasks and will eventually stop running.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request. The payload has to contain the
                               following fields:
                                'dag_name': the name of the dag that should be stopped

        Returns:
            Response: A response object containing the following fields:
                          - success: True if the dag was added successfully to the list
                                     of dags that should be stopped.
        """
        if (request.payload['dag_name'] is not None) and \
           (request.payload['dag_name'] not in self._stop_dags):
            self._stop_dags.append(request.payload['dag_name'])
        return Response(success=True, uid=request.uid)

    def _handle_is_dag_stopped(self, request):
        """ The handler for the dag_stopped request.

        The dag_stopped request checks whether a dag is flagged to be terminated.

        Args:
            request (Request): Reference to a request object containing the
                               incoming request. The payload has to contain the
                               following fields:
                                'dag_name': the name of the dag that should be checked

        Returns:
            Response: A response object containing the following fields:
                          - is_stopped: True if the dag is flagged to be stopped.
        """
        return Response(success=True,
                        uid=request.uid,
                        payload={
                            'is_stopped': request.payload['dag_name'] in self._stop_dags
                        })
