""" Stop the execution of a task from callback functions

This workflow shows how to stop a task using the Stop exception from any callback. In the
example below, 'start_task' is executed first and then branches into three paths. The
first path consists of a bash task that enumerates the numbers 1 to 10. As soon as
number 5 is reached it raises the Stop exception from its stdout callback function. This
leads to an immediate stop of the bash process and skips the successor 'print_task_1'.
The second path, running in parallel, executes 'stop_noskip_task', which raises a
Stop exception but with the 'skip_successors' flag set to False, meaning that the task
is stopped immediately but 'print_task_2' will be executed. The third path is similar
to the second path but will skip 'print_task_3'.
"""

from lightflow.models import Dag, StopTask
from lightflow.tasks import PythonTask, BashTask


# callback function for the start task
def start_all(data, store, signal, context):
    print('Starting DAG {}'.format(context.dag_name))


# callback function that is called for each new line of the stdout of the bash process
def bash_stdout(line, data, store, signal, context):
    if int(line) == 5:
        raise StopTask('Reached line number 5')
    else:
        print('Content of current line is {}'.format(line))


# callback function for a task that immediately stops but will not affect successor tasks
def stop_noskip(data, store, signal, context):
    raise StopTask('Stop task {} but not successor tasks'.format(context.task_name),
                   skip_successors=False)


# callback function for a task that immediately stops and also skips its successor tasks
def stop(data, store, signal, context):
    raise StopTask('Stop task {} and all successor tasks'.format(context.task_name))


# callback for printing the current task context
def print_context(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id})'.format(**context.to_dict()))


# create the main DAG
d = Dag('main_dag')


start_task = PythonTask(name='start_task',
                        callback=start_all)

bash_task = BashTask(name='bash_task',
                     command='for i in `seq 1 10`; do echo "$i"; done',
                     callback_stdout=bash_stdout)

stop_noskip_task = PythonTask(name='stop_noskip_task',
                              callback=stop_noskip)

stop_task = PythonTask(name='stop_task',
                       callback=stop)

print_task_1 = PythonTask(name='print_task_1',
                          callback=print_context)

print_task_2 = PythonTask(name='print_task_2',
                          callback=print_context)

print_task_3 = PythonTask(name='print_task_3',
                          callback=print_context)


# set up the graph of the DAG with a start task and three paths with different stop
# conditions.
d.define({
    start_task: [bash_task, stop_noskip_task, stop_task],
    bash_task: print_task_1,
    stop_noskip_task: print_task_2,
    stop_task: print_task_3
})
