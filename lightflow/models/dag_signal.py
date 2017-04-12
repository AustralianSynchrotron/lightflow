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
