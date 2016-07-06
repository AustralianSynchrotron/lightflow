from lightflow.models import Dag
from lightflow.tasks import PythonTask


def print_name(name, data, data_store, signal):
    print('>>>>>>>>>>> {}'.format(name))


def start_sub_dag(name, data, data_store, signal):
    signal.run_dag('subDag')


md_one = PythonTask(name='md_one',
                    python_callable=print_name)

md_two = PythonTask(name='md_two',
                    python_callable=start_sub_dag)

main_dag = Dag('mainDag')
main_dag.define_workflow({md_one: [md_two]})


sd_one = PythonTask(name='sd_one',
                    python_callable=print_name)

sd_two = PythonTask(name='sd_two',
                    python_callable=print_name)

sub_dag = Dag('subDag')
sub_dag.define_workflow({sd_one: [sd_two]})
