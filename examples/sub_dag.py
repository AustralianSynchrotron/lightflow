""" Implement dynamic workflows by calling other dags from within a task

In order to change the workflow at runtime, a task can request the execution of another
dag via the start_dag function of the signal system.

This example requires the numpy module to be installed and available to the workers
as well as to the workflow.

"""

from time import sleep
import numpy as np

from lightflow.models import Dag
from lightflow.tasks import PythonTask


# the callback function for the init task
def print_name(data, store, signal, context):
    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id})'.format(**context.to_dict()))


# this callback function starts five dags. For each dag the function waits a second,
# then creates a numpy array and stores it into the data that is then passed to the
# sub dag. The dag that should be started can either be given by its name or the dag
# object itself. The names of the created dags are recorded and the task waits for
# all created dags to be completed.
def start_sub_dag(data, store, signal, context):
    dag_names = []
    for i in range(5):
        sleep(1)
        data['image'] = np.ones((100, 100))
        started_dag = signal.start_dag(sub_dag, data=data)
        dag_names.append(started_dag)

    signal.join_dags(dag_names)


# this callback function prints the dimensions of the received numpy array
def sub_dag_print(data, store, signal, context):
    print('Received an image with dimensions: {}'.format(data['image'].shape))


init_task = PythonTask(name='init_task',
                       callback=print_name)

call_dag_task = PythonTask(name='call_dag_task',
                           callback=start_sub_dag)

# create the main dag that runs the init task first, followed by the call_dag task.
main_dag = Dag('main_dag')
main_dag.define({
    init_task: call_dag_task
})


# create the tasks for the sub dag that simply prints the shape of the numpy array
# passed down from the main dag.
print_task = PythonTask(name='print_task',
                        callback=sub_dag_print)

# create the sub dag that is being called by the main dag. In order to stop the
# system from automatically starting the dag when the workflow is run, set the autostart
# parameter to false.
sub_dag = Dag('sub_dag', autostart=False)

sub_dag.define({
    print_task: None
})
