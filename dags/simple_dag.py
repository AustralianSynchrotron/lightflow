from time import sleep
from lightflow.models import Dag
from lightflow.tasks import PythonTask


def print_me(name):
    print(name)


def sleep_me(name):
    print("Sleeping: {}".format(name,))
    sleep(10)
    print("Awoken: {}".format(name,))


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

b4 = PythonTask(name='b4',
                python_callable=print_me,
                dag=d)

b5 = PythonTask(name='b5',
                python_callable=sleep_me,
                dag=d)


b2.add_downstream(b1)
b2.add_downstream(b4)
b2.add_downstream(b5)
b3.add_downstream(b2)
