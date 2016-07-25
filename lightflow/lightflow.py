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


def get_workers():
    """ Return a dictionary with basic information about the workers.

    Returns:
        dict: A dictionary of all workers, with the unique worker name as key and
              the fields as follows:
              'broker': the broker the worker is using
                'transport': the transport protocol of the broker
                'hostname': the broker hostname
                'port': the broker port
                'virtual_host': the virtual host, e.g. the database number in redis.
              'proc': the worker process
                'pid': the PID of the worker
                'processes': the PIDs of the concurrent task processes

    """
    workers = {}
    for name, stats in celery_app.control.inspect().stats().items():
        if name not in workers:
            workers[name] = {}

        broker = stats['broker']
        workers[name]['broker'] = {
            'transport': broker['transport'],
            'hostname': broker['hostname'],
            'port': broker['port'],
            'virtual_host': broker['virtual_host']
        }

        workers[name]['proc'] = {
            'pid': stats['pid'],
            'processes': stats['pool']['processes']
        }
    return workers


def get_queues(worker_name):
    """ Return the queues for the specified worker.

    Args:
        worker_name (str): the unique name of the worker.

    Returns:
        list: a list of the queue names this worker serves.
    """
    inspect = celery_app.control.inspect(destination=[worker_name])
    return [q['name'] for q in inspect.active_queues()[worker_name]]


def get_tasks(worker_name, task_status='active'):
    """ Return the active and scheduled tasks for the specified worker.

    Args:
        worker_name (str): the unique name of the worker.
        task_status (str): the status of the tasks.
                           Allowed values are 'active' and 'scheduled'

    Returns:
        list: A list of the active or scheduled tasks. Each list item is a dictionary
              with the following fields:
              'id': the unique id of the task
              'name': the name of the task
              'worker_pid': the PID of the worker executing the task
              'routing_key': the queue the task is in
    """
    inspect = celery_app.control.inspect(destination=[worker_name])
    if task_status == 'active':
        tasks = inspect.active()[worker_name]
    else:
        tasks = inspect.scheduled()[worker_name]

    return [{
        'id': task['id'],
        'name': task['name'],
        'worker_pid': task['worker_pid'],
        'routing_key': task['delivery_info']['routing_key']
    } for task in tasks]
