""" Simple workflow of two tasks exchanging data

The first task (put_task) stores the value 5 in the key 'value', that is then read
and displayed by the second task (print_task).

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# the callback function for the task that stores the value 5
def put_data(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id}) '
          'on {worker_hostname}'.format(**context.to_dict()))

    data['value'] = 5


# the callback function for the task that prints the data
def print_value(data, store, signal, context):
    print('The value is: {}'.format(data['value']))


# create the main DAG
d = Dag('main_dag')

# create the two tasks for storing and retrieving data
put_task = PythonTask(name='put_task',
                      callback=put_data)

print_task = PythonTask(name='print_task',
                        callback=print_value)

# set up the graph of the DAG, in which the put_task has to be executed first,
# followed by the print_task.
d.define({
    put_task: print_task
})
