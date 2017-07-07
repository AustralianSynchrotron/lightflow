""" Process tasks in parallel with branches and wait for their completion

This workflow shows how to run tasks in parallel by branching into multiple lanes. A join
task waits for the tasks in the lanes to finish.


The graph is as following:

            /-> lane1_print_task \
branch_task --> lane2_print_task  --> join_task
            \-> lane3_print_task /

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# the callback function for the tasks
def print_info(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id})'.format(**context.to_dict()))


# create the main DAG
d = Dag('main_dag')

# task that limits the branching to certain successor tasks
branch_task = PythonTask(name='branch_task',
                         callback=print_info)

# first task, first lane
lane1_print_task = PythonTask(name='lane1_print_task',
                              callback=print_info)

# first task, second lane
lane2_print_task = PythonTask(name='lane2_print_task',
                              callback=print_info)

# first task, third lane
lane3_print_task = PythonTask(name='lane3_print_task',
                              callback=print_info)

# joins all three lanes together and waits for the predecessor tasks to finish processing
join_task = PythonTask(name='t_join_me',
                       callback=print_info)

# set up the graph of the DAG as illustrated above. Please note how a list of tasks
# defines tasks that are run in parallel (branched out).
d.define({branch_task: [lane1_print_task, lane2_print_task, lane3_print_task],
          lane1_print_task: join_task,
          lane2_print_task: join_task,
          lane3_print_task: join_task})
