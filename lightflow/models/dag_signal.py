from .signal import Request


class DagSignal:
    """ Class to wrap the construction and sending of signals into easy to use methods """
    def __init__(self, client, dag_name):
        """ Initialise the dag signal convenience class.

        Args:
            client (Client): A reference to a signal client object.
            dag_name (str): The name of the dag sending this signal.
        """
        self._client = client
        self._dag_name = dag_name

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
        """ Check whether the dag received a stop signal from the workflow.

        As soon as the dag receives a stop signal, no new tasks will be queued
        and the dag will wait for the active tasks to terminate.

        Returns:
            bool: True if the dag should be stopped.
        """
        resp = self._client.send(
            Request(
                action='is_dag_stopped',
                payload={'dag_name': self._dag_name}
            )
        )
        return resp.payload['is_stopped']
