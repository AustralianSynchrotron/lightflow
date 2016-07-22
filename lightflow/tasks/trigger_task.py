from lightflow.models import BaseTask


class TriggerTask(BaseTask):
    """ The base trigger task that defines a common interface for triggers. """
    def __init__(self, name, dag_name, force_run=False, propagate_skip=True):
        """ Initialise the Trigger task.

        Args:
            name (str): The name of the task.
            dag_name (str): The name of the dag that should be executed once one or more
                            trigger events occurred.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._dag_name = dag_name

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the Trigger task.

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
