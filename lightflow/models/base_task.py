from .action import Action
from .task_data import MultiTaskData
from .exceptions import TaskReturnActionInvalid
from .signal import Request


class TaskSignal:
    """ Class to wrap the construction and sending of signals into easy to use methods."""
    def __init__(self, client):
        """ Initialise the task signal convenience class.

        Args:
            client (Client): A reference to a signal client object.
        """
        self._client = client

    def run_dag(self, name, data=None):
        """ Schedule the execution of a dag by sending a signal to the workflow.

        Args:
            name (str): The name of the dag that should be run.
            data (MultiTaskData): The data that should be passed on to the new dag.

        Returns:
            bool: True if the requested dag was started successfully.
        """
        return self._client.send(
            Request(
                action='run_dag',
                payload={'name': name,
                         'data': data if isinstance(data, MultiTaskData) else None}
            )
        ).success


class BaseTask:
    """ The base class for all tasks.

    Tasks should inherit from this class and implement the run() method.
    """
    def __init__(self, name, force_run=False, propagate_skip=True):
        """ Initialise the base task.

        Args:
            name (str): The name of the task.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        self._name = name
        self._force_run = force_run
        self.propagate_skip = propagate_skip

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

    def skip(self):
        """ Flag the task to be skipped. """
        if not self._force_run:
            self._skip = True

    def _run(self, data, data_store, signal):
        """ The internal run method that decorates  the public run method.

        This method makes sure data is being passed to and from the task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

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
            result = self.run(data, data_store, signal)
        else:
            result = None

        if result is None:
            return Action(data.copy())
        else:
            if not isinstance(result, Action):
                raise TaskReturnActionInvalid()

            result.data.add_task_history(self.name)
            return result.copy()

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of a task.

        Implement this method in inherited classes.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        pass
