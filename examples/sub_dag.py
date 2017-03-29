from lightflow.models import Dag
from lightflow.tasks import PythonTask
from time import sleep
import numpy as np


def print_name(name, data, store, signal):
    print('>>>>>>>>>>> {}'.format(name))


def start_sub_dag(name, data, store, signal):
    for i in range(5):
        sleep(1)
        data['image'] = np.ones((100, 100))
        signal.start_dag('subDag', data=data)


def sub_dag_print(name, data, store, signal):
    print('<<<<<<<<<<< {}'.format(data['image'].shape))


md_one = PythonTask(name='md_one',
                    callable=print_name)

md_two = PythonTask(name='md_two',
                    callable=start_sub_dag)

main_dag = Dag('mainDag')
main_dag.define({md_one: [md_two]})


sd_one = PythonTask(name='sd_one',
                    callable=sub_dag_print)

sd_two = PythonTask(name='sd_two',
                    callable=sub_dag_print)

sub_dag = Dag('subDag', autostart=False)
sub_dag.define({sd_one: [sd_two]})
