""" Workflow of two tasks where the second task is executed on a custom queue

Make sure there are two workers available. The first one should consume all three standard
queues: 'workflow', 'dag' and 'task'. For example with:

    lightflow worker start

The second worker should consume only from the 'special' queue. The command line looks
like this:

    lightflow worker start -q special

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# the callback function for the tasks that simply prints the context
def print_text(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id})'.format(**context.to_dict()))


# create the main DAG
d = Dag('main_dag')

# create the two task, where the first task is executed on the 'task' queue and the
# second task on the 'special' queue
print_task = PythonTask(name='print_task',
                        callback=print_text)

print_special = PythonTask(name='print_special',
                           callback=print_text,
                           queue='special')

# set up the graph of the DAG, in which the print_task has to be executed first,
# followed by the print_special.
d.define({
    print_task: print_special
})
