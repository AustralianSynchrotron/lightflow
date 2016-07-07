from uuid import uuid4

from .models import Workflow
from .celery_tasks import celery_app, workflow_celery_task


def run_workflow(name, clear_data_store=True):
    """ Run a single workflow by sending it to the workflow queue.

    Args:
        name (str): The name of the workflow that should be run.
        clear_data_store (bool): Remove any documents created during the workflow
                                 run in the data store after the run.
    """
    wf = Workflow.from_name(name, clear_data_store)
    workflow_celery_task.apply_async((wf,),
                                     queue='workflow',
                                     routing_key='workflow')


def run_worker(queues=None):
    """ Run a worker process.

    """
    queues = queues if queues is not None else ['workflow', 'dag', 'task']

    argv = [
        'worker',
        '-n={}'.format(uuid4(), ),
        '--queues={}'.format(','.join(queues))
    ]
    celery_app.worker_main(argv)
