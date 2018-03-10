


Workers
-------
The ``workers`` module provides the API functions for starting, stopping and managing workers.

Methods
^^^^^^^
.. automodule:: lightflow.workers
   :members:

Return Classes
^^^^^^^^^^^^^^
.. autoclass:: lightflow.queue.models.WorkerStats
   :members:

.. autoclass:: lightflow.queue.models.QueueStats
   :members:


Workflows
---------
The ``workflows`` module provides the API functions for starting, stopping and monitoring workflows.

Methods
^^^^^^^
.. automodule:: lightflow.workflows
   :members:

Return Classes
^^^^^^^^^^^^^^
.. autoclass:: lightflow.queue.models.JobStats
   :members:


Config
------
The configuration of Lightflow is passed to the API functions via an instance of the ``Config`` class. The configuration is described as
a YAML structure and can be loaded from a file. The ``Config`` class contains a default configuration, which means that you only need
to specify the settings in the config file that you would like to change.

.. autoclass:: lightflow.Config
   :members:
   :inherited-members:


Task Data
---------
.. autoclass:: lightflow.models.MultiTaskData
   :members:


Persistent Data Store
---------------------
.. autoclass:: lightflow.models.DataStoreDocument
   :members:
