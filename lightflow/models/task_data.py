from copy import deepcopy

from .exceptions import DataInvalidIndex, DataInvalidAlias


class TaskData:
    """ This class represents a single dataset that is passed between tasks.

    It behaves like a dictionary but also contains a history of all tasks that have
    contributed to this dataset.
    """
    def __init__(self, data=None, *, task_history=None):
        """  Initialize the task data object.

        Args:
            data (dict): A dictionary with the initial data that should be stored.
            task_history (list): A list of task names that have contributed to this data.
        """
        self._data = data if data is not None else {}
        self._task_history = task_history if task_history is not None else []

    def add_task_history(self, task_name):
        """ Add a task name to the list of tasks that have contributed to this dataset.

        Args:
            task_name (str): The name of the task that contributed.
        """
        self._task_history.append(task_name)

    @property
    def data(self):
        """ Return the data of this dataset. """
        return self._data

    @property
    def task_history(self):
        """ Return the list of task names that have contributed to this dataset.  """
        return self._task_history

    def get(self, key, default=None):
        """ Access a single value in the dataset by its key

        Args:
            key (str): The key under which the value is stored.
            default: Return this value if the key cannot be found.

        Returns:
            object: The value that is stored under the specified key.
        """
        return self._data.get(key, default)

    def set(self, key, value):
        """ Change the value of a field in the dataset.

        Args:
            key (str): The key pointing to the value that should be changed.
            value: The new value that should be set.
        """
        self._data[key] = value

    def merge(self, dataset):
        """ Merge the specified dataset on top of the existing data.

        This replaces all values in the existing dataset with the values from the
        given dataset.

        Args:
            dataset (TaskData): A reference to the TaskData object that should be merged
                                on top of the existing object.
        """
        def merge_data(source, dest):
            for key, value in source.items():
                if isinstance(value, dict):
                    merge_data(value, dest.setdefault(key, {}))
                else:
                    dest[key] = value
            return dest

        merge_data(dataset.data, self._data)

        for h in dataset.task_history:
            if h not in self._task_history:
                self._task_history.append(h)

    def __deepcopy__(self, memo):
        """ Copy the object. """
        return TaskData(data=deepcopy(self._data, memo),
                        task_history=self._task_history[:])

    def __getitem__(self, item):
        """ Access a single value in the dataset by its key. """
        return self._data[item]

    def __setitem__(self, key, value):
        """ Change the value of a field in the dataset. """
        self._data[key] = value

    def __delitem__(self, key):
        """ Delete a field in the dataset. """
        del self._data[key]

    def __repr__(self):
        """ Return a representation of the object. """
        return '{}({})'.format(self.__class__.__name__, self._data)

    def __str__(self):
        """ Return a string of the data. """
        return str(self._data)


class MultiTaskData:
    """ Manages multiple TaskData datasets and their aliases.

    This class implements the data object that is being passed between tasks. It consists
    of one or more TaskData datasets in order to accommodate multiple inputs to a single
    task. Each dataset can be accessed by its index or by one or more aliases. There is
    a default dataset, which is used whenever the user does not specify the exact dataset
    to work with.
    """

    def __init__(self, *, dataset=None, aliases=None):
        """ Initialize the MultiTaskData object.

        Args:
            dataset (TaskData): An initial TaskData dataset.
            aliases (list): A list of aliases for the initial dataset.
        """
        self._datasets = [] if dataset is None else [dataset]
        self._aliases = {} if aliases is None else {a: 0 for a in aliases}
        self._default_index = 0

    @property
    def default_index(self):
        """ Return the index of the default dataset. """
        return self._default_index

    @property
    def default_dataset(self):
        """ Return the default dataset.

        Returns:
            TaskData: A reference to the default dataset.
        """
        return self.get_by_index(self._default_index)

    def add_dataset(self, task_name, dataset=None, *, aliases=None):
        """ Add a new dataset to the MultiTaskData.

        Args:
            task_name (str): The name of the task from which the dataset was received.
            dataset (TaskData): The dataset that should be added.
            aliases (list): A list of aliases that should be registered with the dataset.
        """
        self._datasets.append(dataset if dataset is not None else TaskData())
        last_index = len(self._datasets) - 1
        self._aliases[task_name] = last_index

        if aliases is not None:
            for alias in aliases:
                self._aliases[alias] = last_index

        if len(self._datasets) == 1:
            self._default_index = 0

    def add_alias(self, alias, index):
        """ Add an alias pointing to the specified index.

        Args:
            alias (str): The alias that should point to the given index.
            index (int): The index of the dataset for which an alias should be added.

        Raises:
            DataInvalidIndex: If the index does not represent a valid dataset.
        """
        if index >= len(self._datasets):
            raise DataInvalidIndex('A dataset with index {} does not exist'.format(index))
        self._aliases[alias] = index

    def flatten(self, in_place=True):
        """ Merge all datasets into a single dataset.

        The default dataset is the last dataset to be merged, as it is considered to be
        the primary source of information and should overwrite all existing fields with
        the same key.

        Args:
            in_place (bool): Set to True to replace the existing datasets with the
                             merged one. If set to False, will return a new MultiTaskData
                             object containing the merged dataset.

        Returns:
            MultiTaskData: If the in_place flag is set to False.
        """
        new_dataset = TaskData()

        for i, dataset in enumerate(self._datasets):
            if i != self._default_index:
                new_dataset.merge(dataset)

        new_dataset.merge(self.default_dataset)

        # point all aliases to the new, single dataset
        new_aliases = {alias: 0 for alias, _ in self._aliases.items()}

        # replace existing datasets or return a new MultiTaskData object
        if in_place:
            self._datasets = [new_dataset]
            self._aliases = new_aliases
            self._default_index = 0
        else:
            return MultiTaskData(dataset=new_dataset, aliases=list(new_aliases.keys()))

    def set_default_by_alias(self, alias):
        """ Set the default dataset by its alias.

        After changing the default dataset, all calls without explicitly specifying the
        dataset by index or alias will be redirected to this dataset.

        Args:
            alias (str): The alias of the dataset that should be made the default.

        Raises:
            DataInvalidAlias: If the alias does not represent a valid dataset.
        """
        if alias not in self._aliases:
            raise DataInvalidAlias('A dataset with alias {} does not exist'.format(alias))

        self._default_index = self._aliases[alias]

    def set_default_by_index(self, index):
        """ Set the default dataset by its index.

        After changing the default dataset, all calls without explicitly specifying the
        dataset by index or alias will be redirected to this dataset.

        Args:
            index (int): The index of the dataset that should be made the default.

        Raises:
            DataInvalidIndex: If the index does not represent a valid dataset.
        """
        if index >= len(self._datasets):
            raise DataInvalidIndex('A dataset with index {} does not exist'.format(index))

        self._default_index = index

    def get_by_alias(self, alias):
        """ Return a dataset by its alias.

        Args:
            alias (str): The alias of the dataset that should be returned.

        Raises:
            DataInvalidAlias: If the alias does not represent a valid dataset.
        """
        if alias not in self._aliases:
            raise DataInvalidAlias('A dataset with alias {} does not exist'.format(alias))

        return self.get_by_index(self._aliases[alias])

    def get_by_index(self, index):
        """ Return a dataset by its index.

        Args:
            index (int): The index of the dataset that should be returned.

        Raises:
            DataInvalidIndex: If the index does not represent a valid dataset.
        """
        if index >= len(self._datasets):
            raise DataInvalidIndex('A dataset with index {} does not exist'.format(index))

        return self._datasets[index]

    def add_task_history(self, task_name):
        """ Add a task name to the list of tasks that have contributed to all datasets.

        Args:
            task_name (str): The name of the task that contributed.
        """
        for dataset in self._datasets:
            dataset.add_task_history(task_name)

    def __getitem__(self, item):
        """ Access a single value in the default dataset by its key. """
        return self.default_dataset[item]

    def __setitem__(self, key, value):
        """ Change the value of a field in the default dataset. """
        self.default_dataset[key] = value

    def __delitem__(self, key):
        """ Delete a field in the default dataset. """
        del self.default_dataset[key]

    def __call__(self, alias):
        """ Shorthand notation for accessing a dataset by its alias. """
        return self.get_by_alias(alias)

    def __iter__(self):
        """ Forward iteration requests to the internal list of datasets. """
        return iter(self._datasets)
