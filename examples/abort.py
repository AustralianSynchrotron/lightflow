""" Abort the running workflow upon an error in a task

The following workflow stores the file names of three images into an array by the
first task. The second task checks whether there are at least 5 image file names in the
array, and as this is not the case aborts the workflow gracefully. The abort is
accomplished by using the Abort exception.

"""

from lightflow.models import Dag, AbortWorkflow
from lightflow.tasks import PythonTask


# the callback function for the task that stores the array of three image file names
def collect_data(data, store, signal, context):
    data['images'] = ['img_001.tif', 'img_002.tif', 'img_003.tif']


# the callback function for the task that checks the number of stored file names
def check_data(data, store, signal, context):
    if len(data['images']) < 5:
        raise AbortWorkflow('At least 5 images are required')


# create the main DAG
d = Dag('main_dag')

# create the two tasks for storing and checking data
collect_task = PythonTask(name='collect_task',
                          callback=collect_data)

check_task = PythonTask(name='check_task',
                        callback=check_data)

# set up the graph of the DAG
d.define({
    collect_task: check_task
})
