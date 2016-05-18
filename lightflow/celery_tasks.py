from celery import Celery
from lightflow.config import Config


conf = Config()['celery']
app = Celery('lightflow',
             broker=conf['broker'],
             backend=conf['backend'],
             include=['lightflow.celery_tasks'])
#app.conf.update(
#        CELERY_TASK_SERIALIZER='pickle',
#        CELERY_ACCEPT_CONTENT=['pickle'],  # Ignore other content
#        CELERY_RESULT_SERIALIZER='pickle',
#        CELERY_TIMEZONE='Australia/Melbourne',
#        CELERY_ENABLE_UTC=True,
#)


@app.task
def dag_celery_task(dag, start_task=None, workflow_id=None):
    dag.run(start_task, workflow_id)


@app.task
def task_celery_task(task):
    task.run()
