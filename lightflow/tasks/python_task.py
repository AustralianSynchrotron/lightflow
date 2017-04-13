from lightflow.models import BaseTask


class PythonTask(BaseTask):
    """ The Python task executes a user-defined python method. """
    def __init__(self, name, *, callback=None, force_run=False, propagate_skip=True):
        """ Initialize the Python task.

        Args:
            name (str): The name of the task.
            callback: A reference to the Python method that should be called by
                      the task as soon as it is run.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run=force_run, propagate_skip=propagate_skip)
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the Python task.

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
        if self._callback is not None:
            return self._callback(data, store, signal, context, **kwargs)
