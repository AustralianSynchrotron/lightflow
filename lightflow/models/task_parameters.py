
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

    def eval(self, data, data_store, *, exclude=None):
        """ Return a new object in which callable parameters have been evaluated.

        Native types are not touched and simply returned, while callable methods are
        executed and their return value is returned.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            exclude (list): List of key names as strings that should be excluded from
                            the evaluation.

        Returns:
            TaskParameters: A new TaskParameters object with the callable parameters
                            replaced by their return value.
        """
        exclude = [] if exclude is None else exclude

        result = {}
        for key, value in self.items():
            if key in exclude:
                continue

            if value is not None and callable(value):
                result[key] = value(data, data_store)
            else:
                result[key] = value
        return TaskParameters(result)

    def eval_single(self, key, data, data_store):
        """ Evaluate the value of a single parameter taking into account callables .

        Native types are not touched and simply returned, while callable methods are
        executed and their return value is returned.

        Args:
            key (str): The name of the parameter that should be evaluated.
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.

        """
        if key in self:
            value = self[key]
            if value is not None and callable(value):
                return value(data, data_store)
            else:
                return value
        else:
            raise AttributeError()
