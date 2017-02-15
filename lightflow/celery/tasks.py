from redis import StrictRedis
from celery import Celery
from kombu import Queue
from datetime import datetime

from lightflow.logger import get_logger
from lightflow.config import config
from lightflow.models.base_task import TaskSignal
from lightflow.models.dag import DagSignal
from lightflow.models.datastore import DataStore
from lightflow.models.signal import Server, Client
from lightflow.celery.pickle import patch_celery

LIGHTFLOW_INCLUDE = ['lightflow.celery_tasks', 'lightflow.models']

logger = get_logger(__name__)

# patch Celery to use cloudpickle instead of pickle for serialisation
patch_celery()

# configure Celery and create the main celery app
conf = config.get('celery')
celery_app = Celery('lightflow')
celery_app.conf.update(**conf)

# overwrite user supplied settings to make sure celery works with lightflow
celery_app.conf.update(
    task_serializer='pickle',
    accept_content=['pickle'],
    result_serializer='pickle',
    task_default_queue='task',
    task_queues=(
        Queue('task', routing_key='task'),
        Queue('workflow', routing_key='workflow'),
        Queue('dag', routing_key='dag'),
    )
)

if isinstance(celery_app.conf.include, list):
    celery_app.conf.include.extend(LIGHTFLOW_INCLUDE)
else:
    celery_app.conf.include = LIGHTFLOW_INCLUDE


# ----------------------------------------------------------------------------------------
# Celery tasks

def create_data_store_connection():
    data_store_conf = config.get('datastore')
    data_store = DataStore(host=data_store_conf['host'],
                           port=data_store_conf['port'],
                           database=data_store_conf['database'])
    data_store.connect()
    return data_store


def create_signal_connection():
    signal_conf = config.get('signal')
    return StrictRedis(host=signal_conf['host'], port=signal_conf['port'],
                       db=signal_conf['db'])


@celery_app.task(bind=True)
def workflow_celery_task(self, workflow, workflow_id=None):
    logger.info('Running workflow <{}>'.format(workflow.name))

    # create the data store connection
    data_store = create_data_store_connection()

    # create a unique workflow id for this run
    if data_store.exists(workflow_id):
        logger.info('Using existing workflow ID: {}'.format(workflow_id))
    else:
        workflow_id = data_store.add(workflow.name,
                                     meta_payload={
                                         'name': workflow.name,
                                         'start_time': datetime.utcnow(),
                                         'config': config.to_dict()
                                     })
        logger.info('Created workflow ID: {}'.format(workflow_id))

    # create the server for the signal service
    signal_server = Server(create_signal_connection(), request_key=workflow_id)

    # store task specific meta information wth the task
    self.update_state(meta={'name': workflow.name, 'type': 'workflow',
                            'workflow_id': workflow_id})

    # run the DAGs in the workflow
    workflow.run(data_store=data_store,
                 signal_server=signal_server,
                 workflow_id=workflow_id,
                 polling_time=config.get('graph').get('workflow_polling_time'))

    logger.info('Finished workflow <{}>'.format(workflow.name))


@celery_app.task(bind=True)
def dag_celery_task(self, dag, workflow_id, data=None):
    logger.info('Running DAG <{}>'.format(dag.name))
    conf_signal = config.get('signal')

    # store task specific meta information wth the task
    self.update_state(meta={'name': dag.name, 'type': 'dag',
                            'workflow_id': workflow_id})

    # run the tasks in the DAG
    dag.run(workflow_id,
            DagSignal(
                Client(create_signal_connection(),
                       request_key=workflow_id,
                       response_polling_time=conf_signal.get('response_polling_time')),
                dag.name),
            data,
            polling_time=config.get('graph').get('dag_polling_time'))

    logger.info('Finished DAG <{}>'.format(dag.name))


@celery_app.task(bind=True)
def task_celery_task(self, task, workflow_id, data=None):
    logger.info('Running task <{}>'.format(task.name))
    conf_signal = config.get('signal')

    # store task specific meta information wth the task
    self.update_state(meta={'name': task.name,  'type': 'task',
                            'workflow_id': workflow_id})

    # run the task and capture the result
    result = task._run(
        data,
        create_data_store_connection().get(workflow_id),
        TaskSignal(Client(create_signal_connection(),
                          request_key=workflow_id,
                          response_polling_time=conf_signal.get('response_polling_time')),
                   task.dag_name))

    logger.info('Finished task <{}>'.format(task.name))
    return result
