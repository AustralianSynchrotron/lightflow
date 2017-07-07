""" A sequence of tasks incrementing a number

This workflow arranges 3 tasks in a row. Each task calls the same callable,
which increments a number and prints the current number and some information.

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# the callback function for all tasks
def inc_number(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id}) '
          'on {worker_hostname}'.format(**context.to_dict()))

    if 'value' not in data:
        data['value'] = 0

    data['value'] = data['value'] + 1
    print('This is task #{}'.format(data['value']))


# create the main DAG
d = Dag('main_dag')

# create the 3 tasks that increment a number
task_1 = PythonTask(name='task_1',
                    callback=inc_number)

task_2 = PythonTask(name='task_2',
                    callback=inc_number)

task_3 = PythonTask(name='task_3',
                    callback=inc_number)


# set up the graph of the DAG as a linear sequence of tasks
d.define({
    task_1: task_2,
    task_2: task_3
})
