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

Lightflow is written in Python 3 and requires Python 3.5 or higher. At the time being only Linux is supported.




Getting started
---------------

Install Lightflow from PyPi::

    pip install lightflow


Create a default configuration file::

    lightflow config default .


Copy the provided example workflows to a local directory::

    lightflow config examples .


List all example workflows::

    lightflow workflow list


In order to execute a workflow, start a worker that consumes jobs from the workflow, dag and task queues::

    lightflow worker start


Send a workflow to the queue. In this example the simple workflow is queued::

    lightflow workflow start simple



