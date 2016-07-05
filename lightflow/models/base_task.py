from .action import Action
from .task_data import MultiTaskData
from .exceptions import TaskReturnActionInvalid


class BaseTask:
    """ The base class for all tasks.

    Tasks should inherit from this class and implement the run() method.
    """
    def __init__(self, name, force_run=False):
        """ Initialise the base task.

        Args:
            name (str): The name of the task.
            force_run (bool): Run the task even if it is flagged to be skipped.
        """
        self._name = name
        self._force_run = force_run

        self.celery_result = None
        self._skip = False

    @property
    def name(self):
        """ Returns the name of the task. """
        return self._name

    @property
    def is_queued(self):
        """ Returns whether the task has been queued for execution. """
        return self.celery_result is not None

    @property
    def is_finished(self):
        """ Returns whether the execution of the task has finished. """
        if self.is_queued:
            return self.celery_result.ready()
        else:
            return False

    @property
    def is_skipped(self):
        """ Returns whether the task has been flagged to be skipped. """
        return self._skip

    @property
    def state(self):
        """ Returns the current state of the task as a string. """
        if self.is_queued:
            return self.celery_result.state
        else:
            return "NOT_QUEUED"

    def skip(self):
        """ Flag the task to be skipped. """
        if not self._force_run:
            self._skip = True

    def _run(self, data=None, data_store=None):
        """ The internal run method that decorates  the public run method.

        This method makes sure data is being passed to and from the task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.

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
            result = self.run(data, data_store)
        else:
            result = None

        if result is None:
            return Action(data.copy())
        else:
            if not isinstance(result, Action):
                raise TaskReturnActionInvalid()

            result.data.add_task_history(self.name)
            return result.copy()

    def run(self, data, data_store, **kwargs):
        """ The main run method of a task.

        Implement this method in inherited classes.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        pass
