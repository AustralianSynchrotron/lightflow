from uuid import uuid4

from .celery.app import create_app
from .models.const import TaskStatus
from .celery.control import WorkerStats, QueueStats, TaskStats


def start_worker(queues, config, *, celery_args=None):
    """ Start a worker process.

    Args:
        queues (list): List of queue names this worker accepts tasks from.
        config (Config): Reference to the configuration object from which the
                         settings for the worker are retrieved.
        celery_args (list): List of additional Celery worker command line arguments.
                            Please note that this depends on the version of Celery used
                            and might change. Use with caution.
    """
    celery_app = create_app(config)

    argv = [
        'worker',
        '-n={}'.format(uuid4()),
        '--queues={}'.format(','.join(queues))
    ]

    argv.extend(celery_args or [])

    celery_app.worker_main(argv)


def list_workers(config, *, filter_by_queues=None):
    """

    Args:
        config:
        filter_by_queues: OR

    Returns:

    """
    celery_app = create_app(config)
    worker_stats = celery_app.control.inspect().stats()
    queue_stats = celery_app.control.inspect().active_queues()

    workers = []
    for name, w_stat in worker_stats.items():
        queues = [QueueStats.from_celery(q_stat) for q_stat in queue_stats[name]]

        add_worker = filter_by_queues is None
        if not add_worker:
            for queue in queues:
                if queue.name in filter_by_queues:
                    add_worker = True
                    break

        if add_worker:
            workers.append(WorkerStats.from_celery(name, w_stat, queues))

    return workers


def list_tasks(config, *, status=TaskStatus.Active, filter_by_worker=None):
    """

    filter_by_worker improves performance

    Args:
        config:
        status:
        filter_by_worker:

    Returns:

    """
    celery_app = create_app(config)

    # option to filter by the worker (improves performance)
    if filter_by_worker is not None:
        inspect = celery_app.control.inspect(destination=[filter_by_worker])
    else:
        inspect = celery_app.control.inspect()

    # get active or scheduled tasks
    if status == TaskStatus.Active:
        task_map = inspect.active()
    else:
        task_map = inspect.scheduled()

    result = []
    for worker_name, tasks in task_map.items():
        for task in tasks:
            result.append(TaskStats.from_celery(worker_name, task, celery_app))

    return result
