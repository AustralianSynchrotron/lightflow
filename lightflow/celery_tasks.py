from celery import Celery
from lightflow.config import Config
from uuid import uuid4


conf = Config().get('celery')
app = Celery('lightflow',
             broker=conf['broker'],
             backend=conf['backend'],
             include=['lightflow.celery_tasks'])
app.conf.update(
        CELERY_TASK_SERIALIZER='pickle',
        CELERY_ACCEPT_CONTENT=['pickle'],  # Ignore other content
        CELERY_RESULT_SERIALIZER='pickle',
        CELERY_TIMEZONE='Australia/Melbourne',
        CELERY_ENABLE_UTC=True,
        CELERYD_CONCURRENCY=3,
)


@app.task
def dag_celery_task(dag, workflow_id=None):
    print('Running workflow')
    dag.run(workflow_id)


@app.task
def task_celery_task(task):
    print('Running task')
    task.run()


def run_worker():
    argv = [
        'worker',
        '-n={}'.format(uuid4(),),
    ]
    app.worker_main(argv)
