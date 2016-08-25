from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask, ChunkingTask


def make_list(name, data, data_store, signal):
    print(name)
    data['my_list'] = ['asdf_0001.dat', 'asdf_0002.dat', 'sdfa_0001.dat', 'sdfa_0002.dat', 'sdfa_0003.dat',
                       'blah_0001.dat', '|', 'blah_0002.dat', 'blah2_0001.dat', '|']
    return Action(data)


def print_list(name, data, data_store, signal):
    print(name)
    print('==================================')
    print(data['my_list'])
    print('==================================')
    return Action(data)


print_dag = Dag('print_dag', autostart=False)

print_list_task = PythonTask(name='print_list',
                             python_callable=print_list)

print_dag.define({print_list_task: []})


chunk_dag = Dag('chunk_dag')
make_list_task = PythonTask(name='make_list',
                            python_callable=make_list)
chunk_task = ChunkingTask(name='print_me', dag_name='print_dag',
                          pattern='(?P<match>[0-9A-Za-z]*)_', key='my_list')

chunk_dag.define({make_list_task: chunk_task})
