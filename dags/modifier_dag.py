from socro.models import Dag
from socro.tasks import ModifierTask, PythonTask, JoinTask


def print_me(name):
    print(name)


def modify_me(dag):
    print('modify')
    for l in ['a', 'b', 'c', 'd']:
        pt1 = PythonTask(name='{}1'.format(l),
                         python_callable=print_me,
                         dag=dag)
        pt2 = PythonTask(name='{}2'.format(l),
                         python_callable=print_me,
                         dag=dag)

        pt1.add_downstream(dag.get_task_by_name('mod'))
        pt2.add_downstream(pt1)
        pt2.add_upstream(dag.get_task_by_name('join'))


d = Dag('myDag')

start = PythonTask(name='start',
                   python_callable=print_me,
                   dag=d)

mod = ModifierTask(name='mod',
                   python_callable=modify_me,
                   dag=d)
mod.add_downstream(start)

join = JoinTask(name='join',
                dag=d)
