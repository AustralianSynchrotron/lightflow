
Python task
-----------

The ``PythonTask`` is the most basic and most flexible task in Lightflow. It allows you to execute almost arbitrary Python code in your task.
The only requirement is that the Python code can be serialised and deserialised safely. 

.. autoclass:: lightflow.tasks.PythonTask


Bash task
----------

The ``BashTask`` provides an easy to use task for executing bash commands. It allows you to capture and process the standard and error output
of the bash command either in 'real-time' or once the process has completed as a file object.

.. autoclass:: lightflow.tasks.BashTask
