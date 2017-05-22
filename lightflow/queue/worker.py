from celery.result import AsyncResult
from celery.bootsteps import StartStopStep

from lightflow.models.signal import Client, Request, SignalConnection


class WorkerLifecycle(StartStopStep):
    """ Class that manages the lifecycle of a worker. """

    def stop(self, consumer):
        """ This function is called when the worker received a request to terminate.

        Upon the termination of the worker, the workflows for all running jobs are
        stopped gracefully.

        Args:
            consumer (Consumer): Reference to the consumer object that handles messages
                                 from the broker.
        """
        stopped_workflows = []
        for request in [r for r in consumer.controller.state.active_requests]:
            job = AsyncResult(request.id)

            workflow_id = job.result['workflow_id']
            if workflow_id not in stopped_workflows:
                client = Client(
                    SignalConnection(**consumer.app.user_options['config'].signal,
                                     auto_connect=True),
                    request_key=workflow_id)
                client.send(Request(action='stop_workflow'))

                stopped_workflows.append(workflow_id)
