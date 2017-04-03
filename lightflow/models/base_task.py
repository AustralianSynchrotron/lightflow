from .dag import Dag
from .action import Action
from .task_data import MultiTaskData
from .exceptions import TaskReturnActionInvalid
from .signal import Request


class TaskSignal:
    """ Class to wrap the construction and sending of signals into easy to use methods."""
    def __init__(self, client, dag_name):
        """ Initialise the task signal convenience class.

        Args:
            client (Client): A reference to a signal client object.
            dag_name (str): The name of the dag the task belongs to.
        """
        self._client = client
        self._dag_name = dag_name

    def start_dag(self, dag, *, data=None):
        """ Schedule the execution of a dag by sending a signal to the workflow.

        Args:
            dag (Dag, str): The dag object or the name of the dag that should be started.
            data (MultiTaskData): The data that should be passed on to the new dag.

        Returns:
            bool: True if the requested dag was started successfully.
        """
        return self._client.send(
            Request(
                action='start_dag',
                payload={'name': dag.name if isinstance(dag, Dag) else dag,
                         'data': data if isinstance(data, MultiTaskData) else None}
            )
        ).success

    def stop_dag(self):
        """ Send a stop signal to the dag that hosts this task.

        Upon receiving the stop signal, the dag will not queue any new tasks and wait
        for running tasks to terminate.

        Returns:
            bool: True if the signal was sent successfully.
        """
        return self._client.send(
            Request(
                action='stop_dag',
                payload={'dag_name': self._dag_name}
            )
        ).success

    def stop_workflow(self):
        """ Send a stop signal to the workflow.

        Upon receiving the stop signal, the workflow will not queue any new dags.
        Furthermore it will make the stop signal available to the dags, which will
        then stop queueing new tasks. As soon as all active tasks have finished
        processing, the workflow will terminate.

        Returns:
            bool: True if the signal was sent successfully.
        """
        return self._client.send(Request(action='stop_workflow')).success

    @property
    def is_stopped(self):
        """ Check whether the task received a stop signal from the workflow.

        Tasks can use the stop flag to gracefully terminate their work. This is
        particularly important for long running tasks and tasks that employ an
        infinite loop, such as trigger tasks.

        Returns:
            bool: True if the task should be stopped.
        """
        resp = self._client.send(
            Request(
                action='is_dag_stopped',
                payload={'dag_name': self._dag_name}
            )
        )
        return resp.payload['is_stopped']


class TaskParameters(dict):
    """ A class to store a mix of callable and native data type parameters for tasks.

    A single parameter can either be a callable returning a native data type or the
    native data type itself. This allows tasks do dynamically change their parameters
    based on the data flowing into the task or data in the data_store. The structure
    of the callable has to be either:

        my_method(data, data_store)
    or
        lambda data, data_store:

    Tasks that implement parameters create an object of the class in their __init__()
    method and populate it with the tasks attributes. In their run() method tasks then
    have to call the eval(data, data_store) method in order to evaluate any callables.
    """
    def __init__(self, *args, **kwargs):
        """ Initialise the class by passing any arguments down to the dict base type. """
        super().__init__(*args, **kwargs)
        self.update(*args, **kwargs)

    def __getattr__(self, key):
        """ Return the parameter value for a key using attribute-style dot notation.

        Args:
            key (str): The key that points to the parameter value that should be returned.

        Returns:
            str: The parameter value stored under the specified key.
        """
        if key in self:
            return self[key]
        else:
            raise AttributeError()

    def __setattr__(self, key, value):
        """ Assign a parameter value to a key using attribute-style dot notation.

        Args:
            key (str): The key to which the parameter value should be assigned.
            value: The parameter value that should be assigned to the key.
        """
        self[key] = value

    def __delattr__(self, key):
        """ Delete a parameter from the dictionary.

        Args:
            key (str): The key to the entry that should be deleted.

        Raise:
            AttributeError: if the key does not exist.
        """
        if key in self:
            del self[key]
        else:
            raise AttributeError()

    def eval(self, data, data_store):
        """ Return a new object in which callable parameters have been evaluated.

        Native types are not touched and simply returned, while callable methods are
        executed and their return value is returned.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.

        Returns:
            TaskParameters: A new TaskParameters object with the callable parameters
                            replaced by their return value.
        """
        result = {}
        for key, value in self.items():
            if value is not None and callable(value):
                result[key] = value(data, data_store)
            else:
                result[key] = value
        return TaskParameters(result)


class BaseTask:
    """ The base class for all tasks.

    Tasks should inherit from this class and implement the run() method.
    """
    def __init__(self, name, *, force_run=False, propagate_skip=True):
        """ Initialise the base task.

        The dag_name attribute is filled by the dag define method.

        Args:
            name (str): The name of the task.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        self._name = name
        self._force_run = force_run
        self.propagate_skip = propagate_skip

        self._config = None
        self.dag_name = None
        self.celery_result = None
        self._skip = False

    @property
    def name(self):
        """ Returns the name of the task. """
        return self._name

    @property
    def has_result(self):
        """ Returns whether the task has a result.

        This indicates that the task is either queued, running or finished.
        """
        return self.celery_result is not None

    @property
    def is_pending(self):
        """ Returns whether the task is queued. """
        if self.has_result:
            return self.celery_result.state == "PENDING"
        else:
            return False

    @property
    def is_finished(self):
        """ Returns whether the execution of the task has finished. """
        if self.has_result:
            return self.celery_result.ready()
        else:
            return False

    @property
    def is_failed(self):
        """ Returns whether the execution of the task failed. """
        if self.has_result:
            return self.celery_result.failed()
        else:
            return False

    @property
    def is_skipped(self):
        """ Returns whether the task has been flagged to be skipped. """
        return self._skip

    @property
    def state(self):
        """ Returns the current state of the task as a string. """
        if self.has_result:
            return self.celery_result.state
        else:
            return "NOT_QUEUED"

    @property
    def config(self):
        """ Returns the task configuration. """
        return self._config

    @config.setter
    def config(self, value):
        """ Sets the task configuration.

        Args:
            value (Config): A reference to a Config object.
        """
        self._config = value

    def skip(self):
        """ Flag the task to be skipped. """
        if not self._force_run:
            self._skip = True

    def _run(self, data, store, signal, *, start_callback=None, end_callback=None):
        """ The internal run method that decorates the public run method.

        This method makes sure data is being passed to and from the task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.
            start_callback: This function is called before the task is being run.
                            It takes no parameters.
            end_callback: This function is called after the task completed running.
                          It takes no parameters.

        Raises:
            TaskReturnActionInvalid: If the return value of the task is not
                                     an Action object.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        if data is None:
            data = MultiTaskData(self._name)

        if not self.is_skipped or self._force_run:
            if start_callback is not None:
                start_callback()

            result = self.run(data, store, signal)

            if end_callback is not None:
                end_callback()
        else:
            result = None

        if result is None:
            return Action(data.copy())
        else:
            if not isinstance(result, Action):
                raise TaskReturnActionInvalid()

            result.data.add_task_history(self.name)
            return result.copy()

    def run(self, data, store, signal, **kwargs):
        """ The main run method of a task.

        Implement this method in inherited classes.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        pass
