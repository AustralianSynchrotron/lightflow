
class TaskParameters(dict):
    """ A class to store a mix of callable and native data type parameters for tasks.

    A single parameter can either be a callable returning a native data type or the
    native data type itself. This allows tasks do dynamically change their parameters
    based on the data flowing into the task or data in the data_store. The structure
    of the callable has to be either:

        my_method(data, data_store)
    or
        lambda data, data_store:

    Tasks that implement parameters create an object of the class in their __init__()
    method and populate it with the tasks attributes. In their run() method tasks then
    have to call the eval(data, data_store) method in order to evaluate any callables.
    """
    def __init__(self, *args, **kwargs):
        """ Initialise the class by passing any arguments down to the dict base type. """
        super().__init__(*args, **kwargs)
        self.update(*args, **kwargs)

    def __getattr__(self, key):
        """ Return the parameter value for a key using attribute-style dot notation.

        Args:
            key (str): The key that points to the parameter value that should be returned.

        Returns:
            str: The parameter value stored under the specified key.
        """
        if key in self:
            return self[key]
        else:
            raise AttributeError()

    def __setattr__(self, key, value):
        """ Assign a parameter value to a key using attribute-style dot notation.

        Args:
            key (str): The key to which the parameter value should be assigned.
            value: The parameter value that should be assigned to the key.
        """
        self[key] = value

    def __delattr__(self, key):
        """ Delete a parameter from the dictionary.

        Args:
            key (str): The key to the entry that should be deleted.

        Raise:
            AttributeError: if the key does not exist.
        """
        if key in self:
            del self[key]
        else:
            raise AttributeError()

    def eval(self, data, data_store):
        """ Return a new object in which callable parameters have been evaluated.

        Native types are not touched and simply returned, while callable methods are
        executed and their return value is returned.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.

        Returns:
            TaskParameters: A new TaskParameters object with the callable parameters
                            replaced by their return value.
        """
        result = {}
        for key, value in self.items():
            if value is not None and callable(value):
                result[key] = value(data, data_store)
            else:
                result[key] = value
        return TaskParameters(result)
