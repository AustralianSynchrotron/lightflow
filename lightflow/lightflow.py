from uuid import uuid4
from celery.result import AsyncResult
from celery.signals import worker_process_shutdown

from .models import Workflow
from .models.signal import Client, Request
from .celery_tasks import celery_app, workflow_celery_task, create_signal_connection


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

    Args:
        queues (list): List of queue names this worker accepts tasks from.
    """
    queues = queues if queues is not None else ['workflow', 'dag', 'task']

    argv = [
        'worker',
        '-n={}'.format(uuid4(), ),
        '--queues={}'.format(','.join(queues))
    ]
    celery_app.worker_main(argv)


@worker_process_shutdown.connect
def worker_shutdown(**kwargs):
    """ Celery hook that is executed when a worker process receives a term signal. """
    stop_all_workflows()


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
    stats = celery_app.control.inspect().stats()
    if stats is not None:
        for name, stats in stats.items():
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
    """ Return the active or scheduled tasks for the specified worker.

    Args:
        worker_name (str): the unique name of the worker.
        task_status (str): the status of the tasks.
                           Allowed values are 'active' and 'scheduled'.

    Returns:
        list: A list of the active or scheduled tasks. Each list item is a dictionary
              with the fields as specified in the _build_task_response() helper method.
    """
    inspect = celery_app.control.inspect(destination=[worker_name])
    if task_status == 'active':
        tasks = inspect.active()[worker_name]
    else:
        tasks = inspect.scheduled()[worker_name]
    return [_build_task_response(task) for task in tasks]


def find_task(task_id):
    """ Find a task by its celery task id or workflow id.

    Args:
        task_id (str): The celery task id or workflow id of the task.

    Returns:
        dict: If the task was found a dictionary representing the task with the fields
              as specified in the _build_task_response() helper method.
              Otherwise, None is returned.
    """
    inspect = celery_app.control.inspect()
    workers = inspect.active()

    task_found = None
    for worker, tasks in workers.items():
        if task_found is not None:
            break

        for task in tasks:
            task = _build_task_response(task)

            if task['id'] == task_id:
                task_found = task
                break

            if task['type'] == 'workflow':
                if task['workflow_id'] == task_id:
                    task_found = task
                    break

    return task_found


def stop_tasks(task_ids):
    """ Send a stop signal to the specified tasks.

    This method only supports tasks of the type 'workflow' and 'dag'.

    Args:
        task_ids (list): A list of celery task ids or workflow ids.

    Returns:
        list: A list of all tasks that received a stop signal. Each list item is a
              dictionary with the fields as specified in the _build_task_response()
              helper method.
    """
    stopped_tasks = []

    for task_id in task_ids:
        task = find_task(task_id)

        if task is not None:
            client = Client(create_signal_connection(), task['workflow_id'])

            req = {
                'workflow': Request(action='stop_workflow'),
                'dag': Request(action='stop_dag',
                               payload={'dag_name': task['name']}),
            }

            if task['type'] in req:
                if client.send(req[task['type']]).success:
                    stopped_tasks.append(task)

    return stopped_tasks


def stop_all_workflows():
    """ Send a stop signal to all running workflows.

    Returns:
        list: A list of all workflows that received a stop signal. Each list item is a
              dictionary with the fields as specified in the _build_task_response()
              helper method.
    """
    stopped_tasks = []
    inspect = celery_app.control.inspect()
    workers = inspect.active()

    if workers is not None:
        for worker, tasks in workers.items():
            for task in tasks:
                task = _build_task_response(task)
                if task['type'] == 'workflow':
                    client = Client(create_signal_connection(), task['workflow_id'])

                    if client.send(Request(action='stop_workflow')).success:
                        stopped_tasks.append(task)

    return stopped_tasks


def _build_task_response(task):
    """ Helper function to enrich a task dict as returned by celery with additional data.

    Args:
        task (dict): A dictionary representing a task as returned by celery inspect()

    Returns:
        dict: A dictionary with the following fields:
            'id': the unique id of the task as created by celery
            'name': the name of the task
            'type': the type of the task ('workflow', 'dag', 'task')
            'class_name': the Python class name of the task
            'worker_pid': the PID of the worker executing the task
            'routing_key': the queue the task is in
            'workflow_id': The id of the workflow, only exists for workflow tasks
    """
    async_result = AsyncResult(id=task['id'], app=celery_app)
    task_response = {
        'id': task['id'],
        'name': async_result.info.get('name', '') if async_result.info is not None else '',
        'type': async_result.info.get('type', '') if async_result.info is not None else '',
        'class_name': task['name'],
        'worker_pid': task['worker_pid'],
        'routing_key': task['delivery_info']['routing_key']
    }

    if task_response['type'] == 'workflow':
        task_response['workflow_id'] = async_result.info.get('workflow_id', '')\
            if async_result.info is not None else ''

    return task_response
