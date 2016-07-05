from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask


def put_data_me(name, data, data_store):
    print(name)
    data['value'] = 5
    return Action(data)


def branch_me(name, data, data_store):
    return Action(data, ['t_lane1_print_me'])


def print_value(name, data, data_store):
    print(name)
    print(data['value'])


d = Dag('myDag')

t_put_me = PythonTask(name='t_put_me',
                      python_callable=put_data_me)

t_branch_me = PythonTask(name='t_branch_me',
                         python_callable=branch_me)

t_lane1_print_me = PythonTask(name='t_lane1_print_me',
                              python_callable=print_value)

t_lane2_print_me = PythonTask(name='t_lane2_print_me',
                              python_callable=print_value)

t_lane3_print_me = PythonTask(name='t_lane3_print_me',
                              python_callable=print_value)

t_join_me = PythonTask(name='t_join_me',
                       python_callable=print_value)

d.define_workflow({t_put_me: [t_branch_me],
                   t_branch_me: [t_lane1_print_me, t_lane2_print_me, t_lane3_print_me],
                   t_lane1_print_me: [t_join_me],
                   t_lane2_print_me: [t_join_me],
                   t_lane3_print_me: [t_join_me]})
