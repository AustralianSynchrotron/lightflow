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
        CELERYD_CONCURRENCY=8,
)


@app.task
def dag_celery_task(dag, workflow_id=None):
    print('Running workflow')
    dag.run(workflow_id)


@app.task
def task_celery_task(task, *args, **kwargs):
    print('Running task:'.format(args))
    return task._run(*args, **kwargs)


def run_worker():
    argv = [
        'worker',
        '-n={}'.format(uuid4(),),
    ]
    app.worker_main(argv)
