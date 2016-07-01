from copy import copy


class Action:
    """ The class for the action object that is returned by each task.

    The action object encapsulates the information that is returned by a task to the
    system. It contains the data that should be passed on to the successor tasks and
    a list of immediate successor tasks that should be executed. The latter allows
    to limit the execution of successor tasks.
    """
    def __init__(self, data, limit=None):
        """ Initialise the Action object.

        Args:
            data (MultiTaskData): The processed data from the task that should be passed
                                  on to successor tasks.
            limit (list): A list of names of all immediate successor tasks that
                          should be executed.
        """
        self._data = data
        self._limit = limit

    @property
    def data(self):
        """ Returns the data object. """
        return self._data

    @property
    def limit(self):
        """ Returns the list of tasks that should be executed. """
        return self._limit

    def copy(self):
        """ Return a copy of the Action object. """
        return copy(self)
