.. _tutorial-simple:

Step 1: A simple workflow
=========================
In this section of the tutorial we will write our first workflow. It will consist of two tasks that are executed in order. Each task will print
a message so you can track the execution of the tasks. At the end of this section you will have learned how to create tasks, arrange their execution
order and run a workflow.


Workflow file
-------------
In Lightflow, workflows are defined using Python. This means you don't have to learn another language and you can use your favorite Python libraries and modules.
Typically you would have a single Python file describing the entire workflow, but for complex workflows you can, of course, split the workflow definition
into multiple files. For this tutorial, we will only have a single workflow file.

Change into the ``tutorial`` directory and create an empty file called ``tutorial01.py``. This file will contain the workflow for this step of the tutorial.
Your directory structure should look like this::

    /lightflow_tutorial
        lightflow.cfg
        /tutorial
            tutorial01.py


Create two tasks
------------------
Let's get started with our workflow. First, we will create the two tasks for our small workflow. Open the workflow file you just created with your editor of choice.
At the top of the file import the PythonTask class::

    from lightflow.tasks import PythonTask

Lightflow is shipped with two task classes: the ``PythonTask`` and the ``BashTask``. The ``PythonTask`` allows you to execute Python code in your task, while
the ``BashTask`` provides an easy to use task for executing bash commands. In this tutorial we will use the ``PythonTask`` for all our tasks as it is the most
flexible type of task. You can pretty much do whatever you like during the execution of a ``PythonTask``.

Next, create the two tasks for our workflow. We are going to be boring here and call the first task ``first_task`` and the second task ``second_task``::

    first_task = PythonTask(name='first_task',
                            callback=print_first)

    second_task = PythonTask(name='second_task',
                             callback=print_second)

The first argument ``name`` defines a name for the task so you can track the task more easily. We are using the name of the object here, but you can name the
task whatever you think is appropriate. The second argument ``callback`` is a callable that is being run when the task is executed. This is the 'body' of the task
and you are free to execute your own Python code here. In the spirit of boring names for our tutorial, we have named the callables: ``print_first`` and
``print_second``. Of course, we haven't defined the callables yet, so let's do this next.


Implement the callables
-----------------------
We will use functions as the callables for our ``PythonTask`` objects. The functions take a specific form and look like this:: 

    def print_first(data, store, signal, context):
        print('This is the first task')

Add this code above your task instantiations. A callable for a ``PythonTask`` has four arguments. We will cover all four arguments in more detail
in the following tutorial steps. So for now, you can safely ignore them. All we do in the body of the function is to print a simple string.

The callable for the second task is pretty much the same, we only change the name and the string that is printed::

    def print_second(data, store, signal, context):
        print('This is the second task')

At this point we have the task objects that should be run and the code that should be executed for each task. We haven't defined the order in which
we want the tasks to be run yet. This will happen in the next step.


Arrange the tasks in a sequence
-------------------------------
In Lightflow tasks are arranged in a Directed Acyclic Graph, or 'DAG' for short. While this might sound complicated, what it means is that all you do is to
define the dependencies between the tasks, thereby building a network (also called graph) of tasks. The 'directed' captures the fact that the dependencies impose
a direction on the graph. In our case, we want the ``first_task`` to be run before the ``second_task``. Lightflow does not allow for loops in the task graph,
represented by the word 'acyclic'. For example, you are not allowed to set up a graph in which you start with ``first_task`` then run ``second_task`` followed
by running ``first_task`` again.

In Lightflow the ``Dag`` class takes care of running the tasks in the correct order. Import the ``Dag`` class at the top of your workflow file with::

    from lightflow.models import Dag

Next, below your task object instantiations at the bottom of your workflow, create an object of the ``Dag`` class::

    d = Dag('main_dag')

You have to provide a single argument, which is the name you would like to give to the Dag.

The ``Dag`` class provides the function ``define()`` for setting up the task graph. This is where the magic happens. Lightflow uses a Python dictionary
in order to specify the arrangement of the tasks. The **key:value** relationship of a dictionary is mapped to a **parent:child** relationship for tasks,
thereby defining the dependencies between tasks. For our simple, two task workflow the graph definition looks like this::

    d.define({
        first_task: second_task
    })

That's it! You have defined our first workflow and are now ready to run it.


The complete workflow
---------------------
Here is the complete workflow for this tutorial including a few comments::

    from lightflow.models import Dag
    from lightflow.tasks import PythonTask

    
    # the callback functions for the task
    def print_first(data, store, signal, context):
        print('This is the first task')

    def print_second(data, store, signal, context):
        print('This is the second task')


    # create the two task objects
    first_task = PythonTask(name='first_task',
                            callback=print_first)

    second_task = PythonTask(name='second_task',
                             callback=print_second)

    # create the main DAG
    d = Dag('main_dag')
    
    # set up the graph of the DAG, in which the first_task has
    # to be executed first, followed by the second_task.
    d.define({
        first_task: second_task
    })


Document the workflow
---------------------
This step is optional, but highly recommended as it will help you remembering what the workflow does. We will add a title and a short description
to the workflow. At the top of your workflow file add the following docstring::

   """ Tutorial 1: a sequence of two tasks

   This workflow uses two tasks in order to demonstrate
   the basics of a workflow definition in Lightflow.
   """

Lightflow uses the first line of the docstring when listing all available workflows. Give it a go by changing to the directory where the configuration
file is located and enter::

   $ lightflow workflow list
   tutorial01      Tutorial 1: a sequence of two tasks

Lightflow will list your workflow together with the short description you gave it.


Start a worker
--------------
Lightflow uses a worker based scheme. This means a workflow adds jobs onto a central queue from which a number of workers consume jobs and execute them.
In order for Lightflow to run our workflow, it needs at least one running worker. Start a worker with::

    $ lightflow worker start

This will start a worker, which then waits for the first job to be added to the queue. You can start as many workers as you like, but for now one worker
is enough.


Run the workflow
----------------
With at least one worker running, we are ready to run our first workflow. You might need a second terminal in order to run the workflow as the first one
is occupied running our worker. In your second terminal enter::

    $ lightflow workflow start tutorial01

This will send our workflow to the queue. The worker will pick up the workflow and run it. The default logging level is very verbose so you
will see the worker print out a lot of information as it executes the workflow.

You will see how the ``first_task`` is being executed first and prints the string "This is the first task", then followed by the ``second_task`` and the
string "This is the second task".

Congratulations! You completed the first tutorial successfully.
