Lightflow - a lightweight, distributed workflow system
======================================================

.. image:: https://travis-ci.org/AustralianSynchrotron/Lightflow.svg?branch=master
    :target: https://travis-ci.org/AustralianSynchrotron/Lightflow

.. image:: https://readthedocs.org/projects/lightflow/badge/?version=latest
    :target: http://lightflow.readthedocs.io/en/latest
    :alt: Documentation Status

Lightflow is a Python 3.5+ library and command-line tool for executing workflows,
composed of individual tasks, in a distributed fashion. It is based on Celery and
provides task dependencies, data exchange between tasks and an intuitive description of workflows.


Dependencies
------------

Python
^^^^^^
Lightflow is written in Python 3 and requires Python 3.5 or higher.

Operating system
^^^^^^^^^^^^^^^^
Lightflow is being developed and tested on Linux, with Debian and RedHat being the main platforms.

redis
^^^^^
The redis database is required by Lightflow as a communication broker between tasks.
It is also used as the default broker for the Celery queuing system, but could be replaced
with any other supported Celery broker.

MongoDB
^^^^^^^
Lightflow makes use of MongoDB for storing persistent data during a workflow run that can be accessed
by all tasks.


Getting started
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
