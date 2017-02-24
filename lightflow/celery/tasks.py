import celery
from datetime import datetime

from lightflow.logger import get_logger
from lightflow.models.base_task import TaskSignal
from lightflow.models.dag import DagSignal
from lightflow.models.signal import Server, Client

logger = get_logger(__name__)


@celery.task(bind=True)
def execute_workflow(self, workflow, workflow_id=None):
    """ Celery task that runs a workflow on a worker.

    This task starts, manages and monitors the dags that make up a workflow.

    Args:
        self (Task): Reference to itself, the celery task object.
        workflow (Workflow): Reference to the workflow object that is being used to
                             start, manage and monitor dags.
        workflow_id (string): If a workflow ID is provided the workflow run will use
                              this ID, if not a new ID will be auto generated.
    """
    logger.info('Running workflow <{}>'.format(workflow.name))
    data_store = workflow.config.create_data_store_connection()

    # create a unique workflow id for this run
    if data_store.exists(workflow_id):
        logger.info('Using existing workflow ID: {}'.format(workflow_id))
    else:
        workflow_id = data_store.add(payload={
                                         'name': workflow.name,
                                         'start_time': datetime.utcnow()
                                     })
        logger.info('Created workflow ID: {}'.format(workflow_id))

    signal_server = Server(workflow.config.create_signal_connection(),
                           request_key=workflow_id)

    # store task specific meta information wth the task
    self.update_state(meta={'name': workflow.name,
                            'type': 'workflow',
                            'workflow_id': workflow_id})

    # run the DAGs in the workflow
    workflow.run(data_store=data_store,
                 signal_server=signal_server,
                 workflow_id=workflow_id)

    logger.info('Finished workflow <{}>'.format(workflow.name))


@celery.task(bind=True)
def execute_dag(self, dag, workflow_id, data=None):
    """ Celery task that runs a single dag on a worker.

    This task starts, manages and monitors the individual tasks of a dag.

    Args:
        self (Task): Reference to itself, the celery task object.
        dag (Dag): Reference to a Dag object that is being used to start, manage and
                   monitor tasks.
        workflow_id (string): The unique ID of the workflow run that started this dag.
        data (MultiTaskData): An optional MultiTaskData object that is being passed to
                              the first tasks in the dag. This allows the transfer of
                              data from dag to dag.
    """
    logger.info('Running DAG <{}>'.format(dag.name))

    # store task specific meta information wth the task
    self.update_state(meta={'name': dag.name,
                            'type': 'dag',
                            'workflow_id': workflow_id})

    # run the tasks in the DAG
    dag.run(workflow_id=workflow_id,
            signal=DagSignal(Client(dag.config.create_signal_connection(),
                                    request_key=workflow_id),
                             dag.name),
            data=data)

    logger.info('Finished DAG <{}>'.format(dag.name))


@celery.task(bind=True)
def execute_task(self, task, workflow_id, data=None):
    """ Celery task that runs a single task on a worker.

    Args:
        self (Task): Reference to itself, the celery task object.
        task (BaseTask): Reference to the task object that performs the work
                         in its run() method.
        workflow_id (string): The unique ID of the workflow run that started this task.
        data (MultiTaskData): An optional MultiTaskData object that contains the data
                              that has been passed down from upstream tasks.
    """
    logger.info('Running task <{}>'.format(task.name))

    # store task specific meta information wth the task
    self.update_state(meta={'name': task.name,
                            'type': 'task',
                            'workflow_id': workflow_id})

    # run the task and capture the result
    result = task._run(data=data,
                       data_store=task.config.create_data_store_connection(),
                       signal=TaskSignal(Client(task.config.create_signal_connection(),
                                                request_key=workflow_id),
                                         task.dag_name))

    logger.info('Finished task <{}>'.format(task.name))
    return result
