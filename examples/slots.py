from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask


def square_value(data, store, signal, context):
    print(context.task_name)
    data['value'] = data['value']*data['value']
    return Action(data)


def print_value(data, store, signal, context):
    print(context.task_name)
    print(data['value'])


def put_data_me(data, store, signal, context):
    print(context.task_name)
    data['value'] = 5
    return Action(data)


def mult_data(data, store, signal, context):
    print(context.task_name)
    data['value'] = data['value']*data.dataset_from_index(1)['value']
    return Action(data)


def sub_data(data, store, signal, context):
    print(context.task_name)
    data['value'] = data.dataset_from_alias('first')['value']-data.dataset_from_alias('second')['value']
    return Action(data)


d = Dag('myDag')

put_me = PythonTask(name='put_me',
                    callable=put_data_me)

print_me = PythonTask(name='print_me',
                      callable=print_value)

square_me = PythonTask(name='square_me',
                       callable=square_value)

print_me2 = PythonTask(name='print_me2',
                       callable=print_value)

mult_me = PythonTask(name='mult_me',
                     callable=mult_data)

print_me3 = PythonTask(name='print_me3',
                       callable=print_value)

sub_me = PythonTask(name='sub_me',
                    callable=sub_data)

print_me4 = PythonTask(name='print_me4',
                       callable=print_value)


d.define({put_me: {print_me: '', square_me: '', mult_me: '', sub_me: 'first'},
          square_me: {print_me2: '', mult_me: ''},
          mult_me: {print_me3: '', sub_me: 'second'},
          sub_me: [print_me4]})
