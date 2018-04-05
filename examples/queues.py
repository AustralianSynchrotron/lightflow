""" Workflow of two tasks showing custom queues for workflows, dags and tasks

By default a workflow is scheduled to a queue with the name 'workflow', a DAG to a
queue with the name 'dag' and a task to a queue with the name 'task'. In order to have
a worker consume workflows, DAGs and tasks, start it with:

    lightflow worker start

If you like to specify the queue a worker takes jobs from, use the -q argument.
For instance a worker started with:

    lightflow worker start -q workflow

will only run workflows. However you are not restricted to the three default queue names.
By changing the queue names a workflow, dag or task is scheduled to allows you to route
jobs to specific workers.

For this example workflow, start three workers as following:

    lightflow worker start -q main

    lightflow worker start -q graph,task

    lightflow worker start -q high_memory

The first worker only consumes jobs from the 'main' queue, which we will use to run
workflows. The second worker consumes jobs sent to the 'graph' and 'task' queues. We will
route all DAGs to the 'graph' queue, while all tasks without a custom queue name will end
up in the default 'task' queue. For the third worker we assume it runs on a special host
for memory demanding tasks. So we will route our large memory consuming tasks to this
worker.

Start the workflow with:

    lightflow workflow start -q main queues

In the output of the workers you will see how the workflow is being processed on the
worker consuming the 'main' queue, the DAG and the print_task on the second worker, and
the print_memory task on the third worker.

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# the callback function for the tasks that simply prints the context
def print_text(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id})'.format(**context.to_dict()))


# create the main DAG and have it scheduled on the 'graph' queue
d = Dag('main_dag', queue='graph')

# create the two task, where the first task is executed on the default 'task' queue
# while the second task is processed on the 'high_memory' queue
print_task = PythonTask(name='print_task',
                        callback=print_text)

print_memory = PythonTask(name='print_memory',
                          callback=print_text,
                          queue='high_memory')

# set up the graph of the DAG, in which the print_task has to be executed first,
# followed by the print_memory task.
d.define({
    print_task: print_memory
})
