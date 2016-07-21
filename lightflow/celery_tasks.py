from celery import Celery
from kombu import Queue

from .logger import get_logger
from .config import Config
from .models.base_task import TaskSignal
from .models.dag import DagSignal
from .models.datastore import DataStore
from .models.signal import Client
from .celery_pickle import patch_celery

logger = get_logger(__name__)

# patch Celery to use cloudpickle instead of pickle for serialisation
patch_celery()

# configure Celery and create the main celery app
conf = Config().get('celery')
celery_app = Celery('lightflow',
                    broker=conf['broker'],
                    backend=conf['backend'],
                    include=['lightflow.celery_tasks'])

celery_app.conf.update(
        CELERY_TASK_SERIALIZER='pickle',
        CELERY_ACCEPT_CONTENT=['pickle'],  # Ignore other content
        CELERY_RESULT_SERIALIZER='pickle',
        CELERY_TIMEZONE='Australia/Melbourne',
        CELERY_ENABLE_UTC=True,
        CELERYD_CONCURRENCY=8,
        CELERY_DEFAULT_QUEUE='task',
        CELERY_QUEUES=(
            Queue('task', routing_key='task'),
            Queue('workflow', routing_key='workflow'),
            Queue('dag', routing_key='dag'),
        )
)


# ----------------------------------------------------------------------------------------
# Celery tasks

def connect_data_store():
    data_store_conf = Config().get('datastore')
    data_store = DataStore(host=data_store_conf['host'],
                           port=data_store_conf['port'],
                           database=data_store_conf['database'])
    data_store.connect()
    return data_store


@celery_app.task
def workflow_celery_task(workflow):
    logger.info('Running workflow <{}>'.format(workflow.name))
    workflow.run(connect_data_store())
    logger.info('Finished workflow <{}>'.format(workflow.name))


@celery_app.task
def dag_celery_task(dag, workflow_id, signal_connection, data=None):
    logger.info('Running DAG <{}>'.format(dag.name))
    dag.run(workflow_id,
            DagSignal(Client.from_connection(signal_connection)),
            data)
    logger.info('Finished DAG <{}>'.format(dag.name))


@celery_app.task
def task_celery_task(task, workflow_id, signal_connection, data=None):
    logger.info('Running task <{}>'.format(task.name))
    result = task._run(data,
                       connect_data_store().get(workflow_id),
                       TaskSignal(Client.from_connection(signal_connection)))
    logger.info('Finished task <{}>'.format(task.name))
    return result
