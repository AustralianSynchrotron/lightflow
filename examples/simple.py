""" Very simple workflow with two consecutive tasks. 

"""

from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask


def put_data_me(data, store, signal, context):
    print(context.name)
    data['value'] = 5
    return Action(data)


def print_value(data, store, signal, context):
    print(context.name)
    print(data['value'])


d = Dag('myDag')

put_me = PythonTask(name='put_me',
                    callable=put_data_me)

print_me = PythonTask(name='print_me',
                      callable=print_value)

d.define({
    put_me: print_me
})
