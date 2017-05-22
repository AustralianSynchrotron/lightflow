from uuid import uuid4

from .queue.app import create_app
from .queue.worker import WorkerLifecycle
from .queue.models import WorkerStats, QueueStats


def start_worker(queues, config, *, name=None, celery_args=None):
    """ Start a worker process.

    Args:
        queues (list): List of queue names this worker accepts jobs from.
        config (Config): Reference to the configuration object from which the
                         settings for the worker are retrieved.
        name (string): Unique name for the worker. The hostname template variables from
                       Celery can be used. If not given, a unique name is created.
        celery_args (list): List of additional Celery worker command line arguments.
                            Please note that this depends on the version of Celery used
                            and might change. Use with caution.
    """
    celery_app = create_app(config)

    argv = [
        'worker',
        '-n={}'.format(uuid4() if name is None else name),
        '--queues={}'.format(','.join(queues))
    ]

    argv.extend(celery_args or [])

    celery_app.steps['consumer'].add(WorkerLifecycle)
    celery_app.user_options['config'] = config
    celery_app.worker_main(argv)


def stop_worker(config, *, worker_ids=None):
    """ Stop a worker process.

    Args:
        config (Config): Reference to the configuration object from which the
                         settings for the worker are retrieved.
        worker_ids (list): An optional list of ids for the worker that should be stopped.

    """
    if worker_ids is not None and not isinstance(worker_ids, list):
        worker_ids = [worker_ids]

    celery_app = create_app(config)
    celery_app.control.shutdown(destination=worker_ids)


def list_workers(config, *, filter_by_queues=None):
    """ Return a list of all available workers.

    Args:
        config (Config): Reference to the configuration object from which the
                         settings are retrieved.
        filter_by_queues (list): Restrict the returned workers to workers that listen to
                                 at least one of the queue names in this list.

    Returns:
        list: A list of WorkerStats objects.
    """
    celery_app = create_app(config)
    worker_stats = celery_app.control.inspect().stats()
    queue_stats = celery_app.control.inspect().active_queues()

    if worker_stats is None:
        return []

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
