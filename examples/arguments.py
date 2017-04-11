from lightflow.models import Arguments, Option, Dag
from lightflow.tasks import PythonTask


arguments = Arguments([
    Option('filepath', help='Specify a file path', type=str),
    Option('recursive', default=True, help='Run recursively', type=bool),
    Option('iterations', default=1, help='The number of iterations', type=int),
    Option('threshold', default=0.4, help='The threshold value', type=float)
])


def print_filename(data, store, signal, context):
    print('The filepath is:', store.get('filepath'))


def print_iterations(data, store, signal, context):
    print('Number of iterations:', store.get('iterations'))



d = Dag('myDag')

print_1 = PythonTask(name='print_filename',
                     callable=print_filename)

print2 = PythonTask(name='print_number',
                    callable=print_iterations)

d.define({
    print_1: print2
})
