from lightflow.models import BaseTask


class PythonTask(BaseTask):
    """ The Python task executes a user-defined python method. """
    def __init__(self, name, force_run=False, python_callable=None):
        """ Initialise the Python task.

        Args:
            name (str): The name of the task.
            force_run (bool): Run the task even if it is flagged to be skipped.
            python_callable: A reference to the Python method that should be called by
                             the task as soon as it is run.
        """
        super().__init__(name, force_run)
        self._python_callable = python_callable

    def run(self, data, data_store, **kwargs):
        """ The main run method of the Python task.

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
        if self._python_callable is not None:
            return self._python_callable(self.name, data, data_store, **kwargs)
