.. _installation:

Installation
============

One of the key goals when developing Lightflow was to keep the infrastructure dependencies as small as possible.
Lightflow does not require special file systems, job scheduler systems or special hardware. It runs on most Linux distributions
and is known to work on MacOSX as well as on the Ubuntu subsystem of Windows 10. Apart from Python 3.5+, the only dependencies
of Lightflow are a running redis and MongoDB database.


Python
------
Lightflow requires Python 3.5 or higher. It has been developed and tested with both a native Python installation as well as Miniconda/Anaconda.

Lightflow's main Python dependencies are:

- `Celery <http://www.celeryproject.org>`_ - for queuing and managing jobs and running workers
- `NetworkX <http://networkx.github.io>`_ - for building and interrogating the directed acyclic graphs of a workflow
- `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ - for exchanging data between tasks running on distributed hosts
- `Click <http://click.pocoo.org/5>`_ - for the command line client
- `ruamel.yaml <http://yaml.readthedocs.io/en/latest>`_ - for reading the configuration file

These dependencies are installed during the installation of Lightflow automatically.


redis
-----
Redis is an in-memory key-value database and is required by Lightflow as a communication broker between tasks. It is also used as the default
broker for the Celery queuing system, but could be replaced with any other supported Celery broker.

You can either download redis from the `offical redis website <https://redis.io/download>`_ or install it via the package
manager of your distribution. By default, the redis server runs on ``localhost`` and port ``6379``. The :ref:`quickstart` as well as the :ref:`tutorial`
assume you are running redis using these defaults.


MongoDB
-------
MongoDB is a popular document-oriented database and is used by Lightflow for storing data that should persist during a workflow run.

You can either download MongoDB from the `official MongoDB website <https://www.mongodb.com/download-center#community>`_ or install it via the package
manager of your distribution:

- `RedHat <https://docs.mongodb.com/master/tutorial/install-mongodb-on-red-hat>`_
- `Debian <https://docs.mongodb.com/master/tutorial/install-mongodb-on-debian>`_
- `Ubuntu <https://docs.mongodb.com/master/tutorial/install-mongodb-on-ubuntu>`_

By default, MongoDB runs on ``localhost`` and port ``27017``. The :ref:`quickstart` as well as the :ref:`tutorial`
assume you are running MongoDB using these defaults.


Lightflow
---------
After having redis and MongoDB running, installing Lightflow is a breeze. It is available from PyPI and can be installed with::

    pip install lightflow

This will install the Lightflow libraries, command line client and example workflows.
