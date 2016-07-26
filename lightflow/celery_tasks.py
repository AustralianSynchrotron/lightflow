from celery import Celery
from kombu import Queue

from .logger import get_logger
from .config import Config
from .models.base_task import TaskSignal
from .models.dag import DagSignal
from .models.datastore import DataStore
from .models.signal import Server, Client
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

def create_data_store_connection():
    data_store_conf = Config().get('datastore')
    data_store = DataStore(host=data_store_conf['host'],
                           port=data_store_conf['port'],
                           database=data_store_conf['database'])
    data_store.connect()
    return data_store


@celery_app.task(bind=True)
def workflow_celery_task(self, workflow, workflow_id=None):
    logger.info('Running workflow <{}>'.format(workflow.name))

    # create the server for the signal service and start listening for requests
    signal_server = Server()
    signal_server.bind()

    # create the data store connection
    data_store = create_data_store_connection()

    # create a unique workflow id for this run
    if data_store.exists(workflow_id):
        logger.info('Using existing workflow ID: {}'.format(workflow_id))
    else:
        workflow_id = data_store.add(workflow.name)
        logger.info('Created workflow ID: {}'.format(workflow_id))

    # store task specific meta information wth the task
    self.update_state(meta={'name': workflow.name, 'type': 'workflow',
                            'workflow_id': workflow_id,
                            'signal_connection': signal_server.info().to_dict()})

    # run the DAGs in the workflow
    workflow.run(data_store=data_store,
                 signal_server=signal_server,
                 workflow_id=workflow_id)

    logger.info('Finished workflow <{}>'.format(workflow.name))


@celery_app.task(bind=True)
def dag_celery_task(self, dag, workflow_id, signal_connection, data=None):
    logger.info('Running DAG <{}>'.format(dag.name))

    # store task specific meta information wth the task
    self.update_state(meta={'name': dag.name, 'type': 'dag',
                            'signal_connection': signal_connection.to_dict()})

    # run the tasks in the DAG
    dag.run(workflow_id,
            DagSignal(Client.from_connection(signal_connection), dag.name),
            data)

    logger.info('Finished DAG <{}>'.format(dag.name))


@celery_app.task(bind=True)
def task_celery_task(self, task, workflow_id, signal_connection, data=None):
    logger.info('Running task <{}>'.format(task.name))

    # store task specific meta information wth the task
    self.update_state(meta={'name': task.name,  'type': 'task',
                            'signal_connection': signal_connection.to_dict()})

    # run the task and capture the result
    result = task._run(data,
                       create_data_store_connection().get(workflow_id),
                       TaskSignal(Client.from_connection(signal_connection),
                                  task.dag_name))

    logger.info('Finished task <{}>'.format(task.name))
    return result
