from uuid import uuid4

from .celery.app import create_app


def run_worker(queues, config, *, detach=False, celery_args=None):
    """ Run a worker process.

    Args:
        queues (list): List of queue names this worker accepts tasks from.
        config (Config): Reference to the configuration object from which the
                         settings for the worker are retrieved.
        detach (bool): Start worker as a background process.
        celery_args (list): List of additional Celery worker command line arguments.
                            Please note that this depends on the version of Celery used
                            and might change. Use with caution.
    """
    celery = create_app(config)

    argv = [
        'worker',
        '-n={}'.format(uuid4()),
        '--queues={}'.format(','.join(queues))
    ]

    if detach:
        argv.append('--detach')

    argv.extend(celery_args or [])

    celery.worker_main(argv)
