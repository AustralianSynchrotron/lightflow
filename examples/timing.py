""" Retrieve the duration for all tasks from the store meta section

The last task in a list of tasks interrogates the log in the persistent data store
in order to print the run time for each task.

"""
from time import sleep
from random import random

from lightflow.models import Dag, DataStoreDocumentSection
from lightflow.tasks import PythonTask


# the callback for all sleep tasks
def random_sleep(data, store, signal, context):
    sleep(random() * 4)


# the callback function for the task that prints the run times
def print_times(data, store, signal, context):
    dag_log = store.get(key='log.{}.tasks'.format(context.dag_name),
                        section=DataStoreDocumentSection.Meta)
    for task, fields in dag_log.items():
        # The print task has not finished yet, so there is no duration available
        if task != context.task_name:
            print(task, 'on', fields['worker'], 'took', fields['duration'], 'seconds')
        else:
            print(task, 'on', fields['worker'], 'is still running')


# create the main DAG
d = Dag('main_dag')

# create the sleep tasks
sleep_task_1 = PythonTask(name='sleep_task_1', callback=random_sleep)
sleep_task_2 = PythonTask(name='sleep_task_2', callback=random_sleep)
sleep_task_3 = PythonTask(name='sleep_task_3', callback=random_sleep)

# create the print task
print_task = PythonTask(name='print_task', callback=print_times)

# set up the DAG
d.define({
    sleep_task_1: sleep_task_2,
    sleep_task_2: sleep_task_3,
    sleep_task_3: print_task
})
