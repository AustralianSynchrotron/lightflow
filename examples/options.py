from lightflow.models import Arguments, Option, Dag, Action
from lightflow.tasks import PythonTask


arguments = Arguments([
    Option('filepath', help='Specify a file path'),
    Option('number', default=1, help='The number of iterations', type=int)
])


def print_filename(name, data, data_store, signal):
    print(data_store.get('filepath'))

def print_number(name, data, data_store, signal):
    print(data_store.get('number'))



d = Dag('myDag')

print_1 = PythonTask(name='print_filename',
                     python_callable=print_filename)

print2 = PythonTask(name='print_number',
                    python_callable=print_number)

d.define({
    print_1: print2
})
