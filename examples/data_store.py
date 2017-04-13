""" Keep data during a workflow run in the persistent data store

Data that should be kept during a workflow run can be saved into the persistent
data store. This data is deleted as soon as the workflow run ends, but is available
to all tasks during the lifetime of the workflow.

The data store provides methods to store and retrieve single values or append values
to a list. This can even be done asynchronously from different tasks at the same time.

The key under which the data is being stored supports a hierarchical structure using
the dot notation.

This workflow stores different types of data in the persistent data store and modifies
them.

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask

import numpy as np


# the callback function to store data in the persistent data store. It stores a single
# integer value in 'number', a single integer value into the hierarchical key
# 'buffer' -> 'observable' and a numpy array into 'image'. Additionally it adds an integer
# value to a list in 'sample' -> 'spectra'.
def store_data(data, store, signal, context):
    store.set('number', 5)
    store.set('buffer.observable', 20)
    store.set('image', np.ones((100, 100)))
    store.push('sample.spectra', 7)


# the callback function for the task that retrieves and prints the 'number' and 'image'
# values then modifies the 'number' value and creates a new list of 'filenames'.
def modify_data(data, store, signal, context):
    number = store.get('number')
    print('The number is: {}'.format(number))

    img = store.get('image')
    print('The dimension of the image is: {}'.format(img.shape))

    store.set('number', number * 10)
    store.push('filenames', 'file_a.spec')


# the callback function for the task that adds another filename to the list.
def add_filename(data, store, signal, context):
    store.push('filenames', 'file_b.spec')


# the callback function for the task that adds a nested list to the list of filenames and
# then extends the list of filenames with two more entries.
def add_more_filenames(data, store, signal, context):
    store.push('filenames', ['nested_a', 'nested_b'])
    store.extend('filenames', ['file_c.spec', 'file_d.spec'])


# create the main DAG
d = Dag('main_dag')

# create the tasks that call the functions above
store_task = PythonTask(name='store_task',
                        callback=store_data)

modify_task = PythonTask(name='modify_task',
                         callback=modify_data)

add_filename_task = PythonTask(name='add_filename_task',
                               callback=add_filename)

add_more_filename_task = PythonTask(name='add_more_filename_task',
                                    callback=add_more_filenames)

# set up the graph of the DAG, in which the store_task and modify_task are called
# in sequence while the add_filename_task and add_more_filename_task are run in parallel.
d.define({
    store_task: modify_task,
    modify_task: [add_filename_task, add_more_filename_task]
})
