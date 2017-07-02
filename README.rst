Lightflow
=========

Lightflow is a lightweight, distributed workflow system written in Python.

.. image:: https://travis-ci.org/AustralianSynchrotron/Lightflow.svg?branch=master
    :target: https://travis-ci.org/AustralianSynchrotron/Lightflow

.. image:: https://readthedocs.org/projects/lightflow/badge/?version=latest
    :target: http://lightflow.readthedocs.io/en/latest
    :alt: Documentation Status

Lightflow models a workflow as a set of individual tasks arranged as a directed acyclic graph (DAG).
This specification encodes the direction that data flows as well as dependencies between tasks.
Each workflow consists of one or more DAGs. Lightflow employs a worker-based queuing system, in which
workers consume individual tasks. In order to avoid single points of failure, such as a central daemon
often found in other workflow tools, the queuing system is also used to manage and monitor workflows and DAGs.


Dependencies
------------

Lightflow is written in Python 3 and requires Python 3.5 or higher. At the time being only Linux is supported.




Getting started
---------------

Install Lightflow from PyPi::

    pip install lightflow


Create a default configuration file:

    lightflow config default > lightflow.cfg


