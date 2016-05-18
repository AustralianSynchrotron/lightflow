from socro.models import Dag
from socro.tasks import PythonTask, BranchingTask, JoinTask


def branch_me():
    return ['b1', 'c1'] # creates an infinite loop because a1 is never started
    #return None


def print_me(name):
    print(name)


d = Dag('myDag')

start = BranchingTask(name='start',
                      python_callable=branch_me,
                      dag=d)

a1 = PythonTask(name='a1',
                python_callable=print_me,
                dag=d)
a2 = PythonTask(name='a2',
                python_callable=print_me,
                dag=d)
a1.add_downstream(start)
a2.add_downstream(a1)

b1 = PythonTask(name='b1',
                python_callable=print_me,
                dag=d)
b2 = PythonTask(name='b2',
                python_callable=print_me,
                dag=d)
b1.add_downstream(start)
b2.add_downstream(b1)
b2.add_downstream(a1)

c1 = PythonTask(name='c1',
                python_callable=print_me,
                dag=d)
c2 = PythonTask(name='c2',
                python_callable=print_me,
                dag=d)
c1.add_downstream(start)
c2.add_downstream(c1)

join = JoinTask(name='join',
                dag=d)
join.add_downstream(a2)
join.add_downstream(b2)
join.add_downstream(c2)
