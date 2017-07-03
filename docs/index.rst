Welcome to Lightflow!
=====================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Lightflow is a Python 3.5+ library and command-line tool for executing workflows,
composed of individual tasks, in a distributed fashion. It is based on Celery and
provides task dependencies, data exchange between tasks and an intuitive description of workflows. 


Getting Started
---------------

The following getting started guide assumes a redis database running on ``localhost`` and port ``6379``
as well as a MongoDB database running on ``localhost`` and port ``27017``.

Install Lightflow from PyPi::

    pip install lightflow


Create a default configuration file and copy the provided example workflows to a local directory of your choice::

    lightflow config default .
    lightflow config examples .


If you like, list all available example workflows::

    lightflow workflow list


In order to execute a workflow, start a worker that consumes jobs from the workflow, dag and task queues.
Then start a workflow from the list of available examples. The following example starts the workflow ``simple``::

    lightflow worker start
    lightflow workflow start simple


Guide
-----
 
* :doc:`/overview`
* :doc:`/install`
* :doc:`/configuration`
* :doc:`/worker`
* :doc:`/workflow`
* :doc:`/monitor`
* :doc:`/integration`
* :doc:`/api`



Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
