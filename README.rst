Lightflow
=========

A lightweight, high performance pipeline system for synchrotrons.


Running
-------

::

   docker run -it -d -p 6379:6379 redis
   docker run -it -d -p 27017:27017 mongo


Testing
-------

To set up a development environment::

   pip3 install -r requirements-dev.txt
   pip3 install -e .

To run the unit tests, flake8 and generate test coverage::

   tox

To run just the unit tests::

   pytest

To generate test coverage::

   tox -e coverage
