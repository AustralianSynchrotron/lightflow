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
    workflow_celery_task.delay(wf)


def run_worker():
    """ Run a worker process.
    """
    argv = [
        'worker',
        '-n={}'.format(uuid4(), ),
    ]
    celery_app.worker_main(argv)
