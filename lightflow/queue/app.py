import logging.config
from kombu import Queue
from celery import Celery
from celery.result import AsyncResult
from celery.signals import setup_logging, task_postrun
from functools import partial

from lightflow.queue.const import DefaultJobQueueName
from lightflow.queue.pickle import patch_celery
from lightflow.models.exceptions import ConfigOverwriteError


LIGHTFLOW_INCLUDE = ['lightflow.queue.jobs', 'lightflow.models']


def create_app(config):
    """ Create a fully configured Celery application object.

    Args:
        config (Config): A reference to a lightflow configuration object.

    Returns:
        Celery: A fully configured Celery application object.
    """

    # configure the celery logging system with the lightflow settings
    setup_logging.connect(partial(_initialize_logging, config), weak=False)
    task_postrun.connect(partial(_cleanup_workflow, config), weak=False)

    # patch Celery to use cloudpickle instead of pickle for serialisation
    patch_celery()

    # create the main celery app and load the configuration
    app = Celery('lightflow')
    app.conf.update(**config.celery)

    # overwrite user supplied settings to make sure celery works with lightflow
    app.conf.update(
        task_serializer='pickle',
        accept_content=['pickle'],
        result_serializer='pickle',
        task_default_queue=DefaultJobQueueName.Task
    )

    if isinstance(app.conf.include, list):
        app.conf.include.extend(LIGHTFLOW_INCLUDE)
    else:
        if len(app.conf.include) > 0:
            raise ConfigOverwriteError(
                'The content in the include config will be overwritten')
        app.conf.include = LIGHTFLOW_INCLUDE

    return app


def _initialize_logging(config, **kwargs):
    """ Hook into the logging system of celery.

    Connects the local logging system to the celery logging system such that both systems
    can coexist next to each other.

    Args:
        config (Config): Reference to the configuration object from which the
                         logging settings are retrieved.
        **kwargs: Keyword arguments from the hook.
    """
    logging.config.dictConfig(config.logging)


def _cleanup_workflow(config, task_id, args, **kwargs):
    """ Cleanup the results of a workflow when it finished.

    Connects to the postrun signal of Celery. If the signal was sent by a workflow,
    remove the result from the result backend.

    Args:
        task_id (str): The id of the task.
        args (tuple): The arguments the task was started with.
        **kwargs: Keyword arguments from the hook.
    """
    from lightflow.models import Workflow
    if isinstance(args[0], Workflow):
        if config.celery['result_expires'] == 0:
            AsyncResult(task_id).forget()
