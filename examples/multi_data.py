""" Manage multiple datasets in the task input using indices and aliases

The put_task stores the value 5 into the data and passes this value on to
the print_task_1, square_task, multiply_task and subtract_task. The square_task will
square the value (now 25), print it and pass it on to the multiply_task. The input to the
multiply_task are now two datasets. One from the put_task with ['value']==5 and one from
the square_task with ['value']==25. Since multiplication is a commutative operation,
the multiply_task does not care about the order of the datasets and will simply multiply
both datasets regardless of their order. The result (['value']==125) of the multiplication
is passed on to the subtract_task. Again, the subtract task gets two datasets as input.
The dataset from the put_task with ['value'==5] and the dataset from the multiply_task
with ['value']==125. This time the order in which the subtraction is executed matters.
Thus, the data passed from the put_task to the subtract_task is given the alias 'first',
while the data from the multiply_task is labelled 'second'. The desired result is -120,
so the sutract_task accesses the first dataset by its alias 'first' and subtracts the
value from the second dataset, accessed via its alias 'second'.


The schematic for the graph is as follows:

          /-> print_task_1
          |
put_task -|-> square_task -> print_task_2
          |       |
          |       v
          |-> multiply_task -> print_task_3
          |       |
          |       v
          \-> subtract_task -> print_task_4
"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# store the value 5 under the key 'value'
def put_data(data, store, signal, context):
    data['value'] = 5


# print the name of the task and the current value
def print_data(data, store, signal, context):
    print(context.task_name, 'The value is:', data['value'])


# square the current value
def square_data(data, store, signal, context):
    data['value'] = data['value']**2


# multiply the value from the first dataset and the second dataset. Since the default
# dataset has never been changed, the default dataset is still the first (index==0)
# dataset in the list of all datasets. The second dataset is referenced by its index==1.
def multiply_data(data, store, signal, context):
    data['value'] = data['value'] * data.get_by_index(1)['value']


# subtract two values by using the aliases of the two datasets and different functions
# for illustration purposes: get_by_alias() and the shorthand notation ([alias])
def subtract_data(data, store, signal, context):
    data['value'] = data.get_by_alias('first')['value'] - data('second')['value']


# create the main DAG based on the diagram above
d = Dag('main_dag')

put_task = PythonTask(name='put_task', callback=put_data)
square_task = PythonTask(name='square_task', callback=square_data)
multiply_task = PythonTask(name='multiply_task', callback=multiply_data)
subtract_task = PythonTask(name='subtract_task', callback=subtract_data)

print_task_1 = PythonTask(name='print_task_1', callback=print_data)
print_task_2 = PythonTask(name='print_task_2', callback=print_data)
print_task_3 = PythonTask(name='print_task_3', callback=print_data)
print_task_4 = PythonTask(name='print_task_4', callback=print_data)


d.define({put_task: {print_task_1: None,
                     square_task: None,
                     multiply_task: None,
                     subtract_task: 'first'},
          square_task: [print_task_2, multiply_task],
          multiply_task: {print_task_3: None,
                          subtract_task: 'second'},
          subtract_task: print_task_4})
