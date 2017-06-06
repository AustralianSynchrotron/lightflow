""" Demonstration of user provided workflow parameters

Parameters allow a workflow to ingest data upon its execution and thus allow
the customization of the workflow by users without changes to the workflow code.

On the command line, parameters are specified as argname=value pairs.

Workflow parameters are stored into the persistent data store and can be retrieved from
there as the code below shows.

"""

from lightflow.models import Parameters, Option, Dag
from lightflow.tasks import PythonTask


# This workflow takes four parameters, three optional and one mandatory. All parameters
# without a default value are considered mandatory. In the example below, if the
# 'filepath' parameter is not specified the workflow will not start and an error message
# will be printed on the command line. Additionally, each parameter can have a help text
# and a type. If a type is given, the user provided value is automatically converted
# to this type.
parameters = Parameters([
    Option('filepath', help='Specify a file path', type=str),
    Option('recursive', default=True, help='Run recursively', type=bool),
    Option('iterations', default=1, help='The number of iterations', type=int),
    Option('threshold', default=0.4, help='The threshold value', type=float)
])


# the callback function that prints the value of the filepath parameter
def print_filepath(data, store, signal, context):
    print('The filepath is:', store.get('filepath'))


# the callback function that prints the value of the iterations parameter
def print_iterations(data, store, signal, context):
    print('Number of iterations:', store.get('iterations'))


# create the main DAG
d = Dag('main_dag')

# task for printing the value of the filepath parameter
print_filepath_task = PythonTask(name='print_filepath_task',
                                 callback=print_filepath)

# task for printing the value of the iterations parameter
print_iterations_task = PythonTask(name='print_iterations_task',
                                   callback=print_iterations)

# set up the graph of the DAG, in which the print_filepath_task has to be executed first,
# followed by the print_iterations_task.
d.define({
    print_filepath_task: print_iterations_task
})
