import time
from itertools import count
import pymongo
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
import gridfs
from gridfs import GridFS

from lightflow.logger import get_logger

logger = get_logger(__name__)


# if the connection to MongoDB got lost, try reconnecting for 5 minutes
WAIT_TIME = 300


def get_methods(*objs):
    """ Return the names of all callable attributes of an object"""
    return set(
        attr
        for obj in objs
        for attr in dir(obj)
        if not attr.startswith('_') and callable(getattr(obj, attr))
    )


class MongoExecutable:
    """ Wrapper class for catching and handling reconnect exceptions in pymongo calls.

    The provided callable is executed and if the pymongo library raises an AutoReconnect
    exception, another call is attempted. This is repeated until WAIT_TIME is reached.
    """
    def __init__(self, method):
        """ Initialize the MongoExecutable.

        Args:
            method (callable): The function that should be called and for which
                               reconnection attempts should be tried.
        """
        self._method = method

    def __call__(self, *args, **kwargs):
        """ Call the method and handle the AutoReconnect exception gracefully """
        start_time = time.time()

        for attempt in count():
            try:
                return self._method(*args, **kwargs)
            except AutoReconnect:
                duration = time.time() - start_time

                if duration >= WAIT_TIME:
                    break

                logger.warning(
                    'Reconnecting to MongoDB, attempt {} ({:.3f} seconds elapsed)'.
                    format(attempt, duration))

                time.sleep(self.calc_sleep(attempt))

        return self._method(*args, **kwargs)

    def calc_sleep(self, attempt):
        """ Calculate the sleep time based on the number of past attempts.

        The sleep time grows exponentially with the attempts up to a maximum
        of 10 seconds.

        Args:
            attempt (int): The number of reconnection attempts.

        Returns:
            int: The number of seconds to sleep before trying the next attempt.
        """
        return min(10, pow(2, attempt))

    def __dir__(self):
        return dir(self._method)

    def __str__(self):
        return str(self._method)

    def __repr__(self):
        return repr(self._method)


class MongoReconnectProxy:
    """ Proxy for catching AutoReconnect exceptions in function calls of another class """

    def __init__(self, obj, methods):
        """ Initialize the MongoReconnectProxy.

        Args:
            obj: The object for which all calls should be wrapped in the AutoReconnect
                 exception handling block.
            methods (set): The list of method names that should be wrapped.
        """
        self._unproxied_object = obj
        self._methods = methods

    @property
    def unproxied_object(self):
        """ Return the unproxied object """
        return self._unproxied_object

    def __getitem__(self, key):
        """ Return proxy for the object method named 'key'. """
        item = self._unproxied_object[key]
        if callable(item):
            return MongoReconnectProxy(item, self._methods)
        return item

    def __getattr__(self, key):
        """ Depending on the type of attribute return an Executable or Proxy object. """
        attr = getattr(self._unproxied_object, key)
        if callable(attr):
            if key in self._methods:
                return MongoExecutable(attr)
            else:
                return MongoReconnectProxy(attr, self._methods)
        return attr

    def __call__(self, *args, **kwargs):
        return self._unproxied_object(*args, **kwargs)

    def __dir__(self):
        return dir(self._unproxied_object)

    def __str__(self):
        return str(self._unproxied_object)

    def __repr__(self):
        return repr(self._unproxied_object)


class MongoClientProxy(MongoReconnectProxy):
    """ Proxy for catching AutoReconnect exceptions in function calls of the MongoClient

    Specialization of the MongoReconnectProxy class for the MongoClient class.
    """
    def __init__(self, obj):
        super().__init__(obj,
                         get_methods(pymongo.collection.Collection,
                                     pymongo.database.Database,
                                     MongoClient,
                                     pymongo))


class GridFSProxy(MongoReconnectProxy):
    """ Proxy for catching AutoReconnect exceptions in function calls of the GridFS class

    Specialization of the MongoReconnectProxy class for the GridFS class.
    """
    def __init__(self, obj):
        super().__init__(obj,
                         get_methods(gridfs, GridFS))
