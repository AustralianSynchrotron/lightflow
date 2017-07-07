""" Select task branches dynamically and wait for their completion

The workflow also demonstrates the use of the 'limit' parameter in the returned
Action of the branch_task to select which successor task, and thus which lane,
will be processed. In the example below lane 1 and lane 2 will run in parallel,
while lane 3 is skipped.


The graph is as following:

                        /-> lane1_print_task \
put_task -> branch_task --> lane2_print_task  --> join_task
                        \-> lane3_print_task /

"""

from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask


# the callback function for the task that stores the value 5
def put_data(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id})'.format(**context.to_dict()))

    data['value'] = 5


# the callback function for the branch task that limits the successor tasks to the
# print tasks in lane 1 and lane 2. The successor tasks can be specified by either their
# name or the task object itself. Both methods are shown here.
def branch_with_limit(data, store, signal, context):
    return Action(data, limit=[lane1_print_task, 'lane2_print_task'])


# the callback function for tasks that print the data
def print_value(data, store, signal, context):
    print('Task {} and value {}'.format(context.task_name, data['value']))


# create the main DAG
d = Dag('main_dag')

# task for storing the data
put_task = PythonTask(name='put_task',
                      callback=put_data)

# task that limits the branching to certain successor tasks
branch_task = PythonTask(name='branch_task',
                         callback=branch_with_limit)

# first task, first lane, simply prints the value stored in the put_task
lane1_print_task = PythonTask(name='lane1_print_task',
                              callback=print_value)

# first task, second lane, simply prints the value stored in the put_task
lane2_print_task = PythonTask(name='lane2_print_task',
                              callback=print_value)

# first task, third lane, simply prints the value stored in the put_task
lane3_print_task = PythonTask(name='lane3_print_task',
                              callback=print_value)

# joins all three lanes together and waits for the predecessor tasks to finish processing
join_task = PythonTask(name='t_join_me',
                       callback=print_value)

# set up the graph of the DAG as illustrated above. Please note how a list of tasks
# defines tasks that are run in parallel (branched out).
d.define({put_task: branch_task,
          branch_task: [lane1_print_task, lane2_print_task, lane3_print_task],
          lane1_print_task: join_task,
          lane2_print_task: join_task,
          lane3_print_task: join_task})
