from .action import Action
from .task_data import MultiTaskData
from .exceptions import TaskReturnActionInvalid, Abort, Stop
from lightflow.queue import JobType


class BaseTask:
    """ The base class for all tasks.

    Tasks should inherit from this class and implement the run() method.
    """
    def __init__(self, name, *, queue=JobType.Task, force_run=False, propagate_skip=True):
        """ Initialise the base task.

        The dag_name attribute is filled by the dag define method.

        Args:
            name (str): The name of the task.
            queue (str): Name of the queue the task should be scheduled to.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        self._name = name
        self._queue = queue
        self._force_run = force_run
        self.propagate_skip = propagate_skip

        self._skip = False

        self._config = None
        self._celery_result = None
        self._workflow_name = None
        self._dag_name = None

    @property
    def name(self):
        """ Returns the name of the task. """
        return self._name

    @property
    def queue(self):
        """ Returns the queue the task should be scheduled to. """
        return self._queue

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

    @property
    def celery_result(self):
        """ Returns the celery result object for this task. """
        return self._celery_result

    @celery_result.setter
    def celery_result(self, result):
        """ Sets the celery result object for this task.

        Args:
            result (AsyncResult): The result of the celery queuing call.
        """
        self._celery_result = result

    @property
    def workflow_name(self):
        """ Returns the name of the workflow this task belongs to. """
        return self._workflow_name

    @workflow_name.setter
    def workflow_name(self, name):
        """ Set the name of the workflow this task belongs to.

        Args:
            name (str): The name of the workflow.
        """
        self._workflow_name = name

    @property
    def dag_name(self):
        """ Returns the name of the dag this task belongs to. """
        return self._dag_name

    @dag_name.setter
    def dag_name(self, name):
        """ Set the name of the dag this task belongs to.

        Args:
            name (str): The name of the dag.
        """
        self._dag_name = name

    def skip(self):
        """ Flag the task to be skipped. """
        if not self._force_run:
            self._skip = True

    def _run(self, data, store, signal, context, *,
             start_callback=None, success_callback=None,
             stop_callback=None, abort_callback=None):
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
            context (TaskContext): The context in which the tasks runs.
            start_callback: This function is called before the task is being run.
            success_callback: This function is called when the task completed successfully
            stop_callback: This function is called when a Stop exception was raised.
            abort_callback: This function is called when an Abort exception was raised.

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

            try:
                result = self.run(data, store, signal, context)
                if success_callback is not None:
                    success_callback()

            except Stop as err:
                # the task should be stopped and optionally all successor tasks skipped
                if stop_callback is not None:
                    stop_callback(exc=err)

                result = Action(data, limit=[]) if err.skip_successors else None

            except Abort as err:
                # the workflow should be stopped immediately
                if abort_callback is not None:
                    abort_callback(exc=err)

                result = None
                signal.stop_workflow()

        else:
            result = None

        if result is None:
            return Action(data)
        else:
            if not isinstance(result, Action):
                raise TaskReturnActionInvalid()

            result.data.add_task_history(self.name)
            return result

    def run(self, data, store, signal, context, **kwargs):
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
            context (TaskContext): The context in which the tasks runs.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        pass
