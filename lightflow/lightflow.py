from uuid import uuid4

from lightflow.models import Workflow
from lightflow.celery_tasks import celery_app, workflow_celery_task


def run_workflow(name):
    """ Run a single workflow by sending it to the workflow queue.

    Args:
        name (str): The name of the workflow that should be run.
    """
    wf = Workflow.from_name(name)
    workflow_celery_task.delay(wf)


def run_worker():
    """ Run a worker process.
    """
    argv = [
        'worker',
        '-n={}'.format(uuid4(), ),
    ]
    celery_app.worker_main(argv)
