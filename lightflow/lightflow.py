import logging
import importlib

from lightflow.config import Config
from lightflow.models import Dag, DataStore
from lightflow.models.exceptions import ImportWorkflowError

logger = logging.getLogger(__name__)


class Lightflow:

    def __init__(self):
        pass

    def run_workflow(self, workflow_name, workflow_id):
        # create unique workflow id if it doesn't exist
        data_store = self.create_datastore_connection()
        if data_store.check_workflow_id(workflow_id):
            logger.info('Using existing workflow ID: {}'.format(workflow_id))
        else:
            workflow_id = data_store.create_workflow_id(workflow_id)
            logger.info('Created workflow ID: {}'.format(workflow_id))

        # run all autostart dags in the workflow specified by the workflow name
        for dag in self.get_dags(workflow_name):
            if not dag.autostart:
                continue

            from lightflow.celery_tasks import dag_celery_task
            dag_celery_task.delay(dag, workflow_id=workflow_id)

        return workflow_id

    def status_workflow(self, workflow_id):
        pass

    def status(self):
        pass

    def create_datastore_connection(self):
        data_store_conf = Config().get('datastore')
        data_store = DataStore.create_connection(
                host=data_store_conf['host'],
                port=data_store_conf['port'],
                database_name=data_store_conf['database_name'],
                username=data_store_conf['username'],
                password=data_store_conf['password'])
        return data_store

    def get_dags(self, workflow_name):
        try:
            workflow_module = importlib.import_module(workflow_name)
            return [dag for key, dag in workflow_module.__dict__.items() if isinstance(dag, Dag)]
        except TypeError as e:
            logger.error('Cannot import workflow {}!'.format(workflow_name))
            raise ImportWorkflowError('Cannot import workflow {}!'.format(workflow_name))
