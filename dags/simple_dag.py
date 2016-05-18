from lightflow.models import Dag
from lightflow.tasks import PythonTask


def print_me(name):
    print(name)


d = Dag('myDag')

b1 = PythonTask(name='b1',
                python_callable=print_me,
                dag=d)
b2 = PythonTask(name='b2',
                python_callable=print_me,
                dag=d)
b3 = PythonTask(name='b3',
                python_callable=print_me,
                dag=d)

b2.add_downstream(b1)
b3.add_downstream(b2)
