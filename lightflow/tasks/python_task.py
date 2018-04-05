from lightflow.models import BaseTask, Action
from lightflow.queue import DefaultJobQueueName


class PythonTask(BaseTask):
    """ The Python task executes a user-defined python method.

    Args:
        name (str): The name of the task.
        callback (callable): A reference to the Python method that should be called by
            the task as soon as it is run. It has to have the following definition::

                (data, store, signal, context) -> None, Action

            with the parameters:

                - **data** (:class:`.MultiTaskData`): The data object that has been passed\
                    from the predecessor task.
                - **store** (:class:`.DataStoreDocument`): The persistent data store object\
                    that allows the task to store data for access across the current\
                    workflow run.
                - **signal** (*TaskSignal*): The signal object for tasks. It wraps\
                    the construction and sending of signals into easy to use methods.
                - **context** (*TaskContext*): The context in which the tasks runs.

        queue (str): Name of the queue the task should be scheduled to. Defaults to
            the general task queue.
        callback_init (callable): An optional callable that is called shortly
            before the task is run. The definition is::

                (data, store, signal, context) -> None

            with the parameters:

                - **data** (:class:`.MultiTaskData`): The data object that has been passed\
                    from the predecessor task.
                - **store** (:class:`.DataStoreDocument`): The persistent data store object\
                    that allows the task to store data for access across the current\
                    workflow run.
                - **signal** (*TaskSignal*): The signal object for tasks. It wraps\
                    the construction and sending of signals into easy to use methods.
                - **context** (*TaskContext*): The context in which the tasks runs.

        callback_finally (callable): An optional callable that is always called
            at the end of a task, regardless whether it completed successfully,
            was stopped or was aborted. The definition is::

                (status, data, store, signal, context) -> None

            with the parameters:

                - **status** (*TaskStatus*): The current status of the task. It can\
                    be one of the following:

                        - ``TaskStatus.Success`` -- task was successful
                        - ``TaskStatus.Stopped`` -- task was stopped
                        - ``TaskStatus.Aborted`` -- task was aborted
                        - ``TaskStatus.Error`` -- task raised an exception

                - **data** (:class:`.MultiTaskData`): The data object that has been passed\
                    from the predecessor task.
                - **store** (:class:`.DataStoreDocument`): The persistent data store object\
                    that allows the task to store data for access across the current\
                    workflow run.
                - **signal** (*TaskSignal*): The signal object for tasks. It wraps\
                    the construction and sending of signals into easy to use methods.
                - **context** (*TaskContext*): The context in which the tasks runs.

        force_run (bool): Run the task even if it is flagged to be skipped.
        propagate_skip (bool): Propagate the skip flag to the next task.
    """
    def __init__(self, name, callback=None, *, queue=DefaultJobQueueName.Task,
                 callback_init=None, callback_finally=None,
                 force_run=False, propagate_skip=True):
        super().__init__(name, queue=queue,
                         callback_init=callback_init, callback_finally=callback_finally,
                         force_run=force_run, propagate_skip=propagate_skip)
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the Python task.

        Args:
            data (:class:`.MultiTaskData`): The data object that has been passed from the
                predecessor task.
            store (:class:`.DataStoreDocument`): The persistent data store object that allows the
                task to store data for access across the current workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                and sending of signals into easy to use methods.
            context (TaskContext): The context in which the tasks runs.

        Returns:
            Action: An Action object containing the data that should be passed on
                to the next task and optionally a list of successor tasks that
                should be executed.
        """
        if self._callback is not None:
            result = self._callback(data, store, signal, context, **kwargs)
            return result if result is not None else Action(data)
