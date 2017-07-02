Overview
========

Introduction
------------

Concepts
--------

Lightflow models a workflow as a set of individual tasks arranged as a directed acyclic graph (DAG).
This specification encodes the direction that data flows as well as dependencies between tasks.
Each workflow consists of one or more DAGs. Lightflow employs a worker-based queuing system, in which
workers consume individual tasks. In order to avoid single points of failure, such as a central daemon
often found in other workflow tools, the queuing system is also used to manage and monitor workflows and DAGs.