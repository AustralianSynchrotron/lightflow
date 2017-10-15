.. _tutorial-setup:

Step 0: Setup
=============
In this step we will set up the environment for the tutorial and create an empty workflow. We assume that you followed the :ref:`installation` guide
and have a redis database running on ``localhost`` and port ``6379``, a MongoDB database running on ``localhost`` and port ``27017``
as well as Lightflow installed on your system.

To test whether you have installed Lightflow correctly, enter the following into a terminal::

    $ lightflow

This calls the command line interface of Lightflow and will print the available commands and options.


Configuration file
------------------
Start by creating an empty directory with your preferred name (e.g. ``lightflow_tutorial``) in a location of your choice (e.g. your home directory). 
This will be our working directory for the tutorial containing the lightflow configuration file and our workflow file. Lightflow has no restrictions on where
this directory should be located and what it is called.

Next, we will create a configuration file. Lightflow uses the configuration file for storing settings such as the connection information
for redis and MongoDB, and the location of your workflow files. To make things easier, we equipped the command line interface with a command to generate
a default configuration file for you. Make sure you are located in the directory you created earlier, then enter::

    $ lightflow config default .

This creates a configuration file called ``lightflow.cfg`` containing the default settings into the current directory.

Let's have a look into the configuration file. Open ``lightflow.cfg`` with your editor of choice. The configuration file uses the YAML format
and is broken up into several sections.

.. admonition:: Non-default redis or MongoDB

   If you are running redis on a different host or port from the default mentioned above, change the host
   and port settings in the ``celery`` as well as ``signal`` sections in the configuration file.
   If your MongoDB configuration deviates from the default, edit the host and port fields in the ``store`` section.

We will focus on the first field labelled ``workflows``. This field contains a list of paths where Lightflow should look for workflow files.
The paths can either be relative to the configuration file or absolute paths. By default, Lightflow expects to find workflow files in a sub-directory
called ``examples``, located in the same directory as your configuration file. However, we would like our tutorial workflow file to
live in its own directory called ``tutorial``. Therefore, edit the configuration file by changing ``examples`` to ``tutorial``::

    workflows:
      - ./tutorial

Save the file and exit your editor.


Tutorial folder
---------------

Before we can move on we have to create the ``tutorial`` folder for our tutorial workflow file of course. In the same
directory as your configuration file, create a sub-directory called ``tutorial``::

    $ mkdir tutorial

Now you are ready to write your first workflow! Head over to :ref:`tutorial-simple` in our tutorial and learn how to write your first workflow.
