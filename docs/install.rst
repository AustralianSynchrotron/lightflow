Installation
============

Lightflow has only a few dependencies.


Requirements
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


Library
------

The latest version of Lightflow can be installed from PyPi with::

    pip install lightflow
