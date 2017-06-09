from .dag import Dag
from .signal import Request
from .task_data import MultiTaskData


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
            str: The name of the successfully started dag.
        """
        return self._client.send(
            Request(
                action='start_dag',
                payload={'name': dag.name if isinstance(dag, Dag) else dag,
                         'data': data if isinstance(data, MultiTaskData) else None}
            )
        ).payload['dag_name']

    def join_dags(self, names=None):
        """ Wait for the specified dags to terminate.

        This function blocks until the specified dags terminate. If no dags are specified
        wait for all dags of the workflow, except the dag of the task calling this signal,
        to terminate.

        Args:
            names (list): The names of the dags that have to terminate.

        Returns:
            bool: True if all the signal was sent successfully.
        """
        return self._client.send(
            Request(
                action='join_dags',
                payload={'names': names}
            )
        ).success

    def stop_dag(self, name=None):
        """ Send a stop signal to the specified dag or the dag that hosts this task.

        Args:
            name str: The name of the dag that should be stopped. If no name is given the
                      dag that hosts this task is stopped.

        Upon receiving the stop signal, the dag will not queue any new tasks and wait
        for running tasks to terminate.

        Returns:
            bool: True if the signal was sent successfully.
        """
        return self._client.send(
            Request(
                action='stop_dag',
                payload={'name': name if name is not None else self._dag_name}
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
