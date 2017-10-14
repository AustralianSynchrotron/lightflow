Quickstart
==========

You can't wait to start using Lightflow or have no time to follow the tutorial? No problem, just spend a few minutes with
this quickstart guide and you are on your way to using Lightflow.

This quickstart guide assumes that you have a redis database running on ``localhost`` and port ``6379``,
a MongoDB database running on ``localhost`` and port ``27017`` as well as Lightflow installed on your system. If you haven't
installed the database systems and Lightflow yet, no problem, just follow the installation guide.


Configuration and examples
--------------------------

Create an empty directory in your preferred location. We will use this directory in the following to store the configuration file and
the example workflows. Lightflow has no restrictions on where this directory should be located and what its name should be.

The first step is to create the global configuration file for Lightflow. This file contains, among other settings, the connection
information for redis and MongoDB. The quickest and easiest way to generate a default configuration file is to use the Lightflow command line.
Make sure you are located in the directory you created earlier and simply enter::

    lightflow config default .

This will create a configuration file called ``lightflow.cfg`` containing a default configuration. If you were running redis and MongoDB on
different hosts than ``localhost`` or the default port, simply edit the appropriate settings in the configuration file. You can find more
information about the configuration file in the section Configuration.

Lightflow ships with a number of examples that demonstrate various features of the system. We will copy these examples into a subfolder called ``examples``
inside your current directory. This will allow you to modify the examples as you see fit or use them as a starting point for your own workflows.
The command line tool offers a quick and easy way to copy the examples::

    lightflow config examples .

Now you will find a subfolder ``examples`` in your directory containing all example workflows. If you like, you can list all available example workflows
together with a short description, Make sure you are located in the folder containing the configuration file, then enter::

    lightflow workflow list


Start the workers
-----------------

Lightflow uses a worker based scheme. This means a workflow adds jobs onto a central queue from which a number of workers consume jobs and execute them.
In order for Lightflow to run a workflow, it needs at least one running worker (obviously). You can start a worker with::

    lightflow worker start

This will start a worker, which then waits for the first job to be added to the queue. You can start as many workers as you like, but for the quickstart
guide one worker is enough.

.. admonition:: A recommended setup for multiple workers 

   What is special about Lightflow, in comparison with other workflow systems, is that it also uses workers for running the workflow itself. This means, there
   is no central daemon and thus no single point of failure. Lightflow uses three queues for running a workflow. Two queues, labelled ``workflow`` and ``dag``, for
   managing the workflows and one queue, labelled ``task``, for executing the individual tasks of a workflow. A typical setup of workers would consist of one worker
   dealing with workflow related jobs, thus consuming jobs only from the ``workflow`` and ``dag`` queues, and one or more workers executing the actual tasks.

   You can use the ``-q`` argument in the command line in order to restrict the queues a worker consumes jobs from. For the recommended setup discussed above you would start one worker
   with::

       lightflow worker start -q workflow,dag
    
   and at least one more worker with::

       lightflow worker start -q task


Run a workflow
--------------

With at least one worker running, we are ready to run our first workflow. You can pick any example workflow you like and run it. In the following we
will run the most basic of all workflows, the ``simple`` workflow. You might need a second terminal in order to run the workflow as the first one
is occupied running our worker. In your second terminal enter::

    lightflow workflow start simple

This will send the workflow ``simple`` to the queue. Our worker will pick up the workflow and run it. The default logging level is very verbose so you
will see the worker print out a lot of information as it executes the workflow.


Where to go from here
---------------------

Congratulations, you have finished the quickstart guide. A good place to continue is to have a look at the documented example workflows. They are a great
starting point for exploring the features of Lightflow. Alternatively, head over to the tutorial section for a more structured introduction to Lightflow.
