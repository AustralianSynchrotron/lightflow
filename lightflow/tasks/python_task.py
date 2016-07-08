from lightflow.models import BaseTask


class PythonTask(BaseTask):
    """ The Python task executes a user-defined python method. """
    def __init__(self, name, python_callable=None, force_run=False, propagate_skip=True):
        """ Initialise the Python task.

        Args:
            name (str): The name of the task.
            python_callable: A reference to the Python method that should be called by
                             the task as soon as it is run.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._python_callable = python_callable

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the Python task.

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
        if self._python_callable is not None:
            return self._python_callable(self.name, data, data_store, signal, **kwargs)
