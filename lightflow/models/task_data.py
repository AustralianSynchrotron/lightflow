from collections import OrderedDict, defaultdict
from itertools import islice


class MultiTaskData:
    def __init__(self):
        self._datasets = OrderedDict()
        self._selected_index = 0
        self._selected_task_name = ''
        self._aliases = {}
        self._alias_lookup = defaultdict(list)

    def add_dataset(self, task_name, dataset, aliases=None):
        self._datasets[task_name] = dataset
        if aliases is not None:
            for alias in aliases:
                self._aliases[alias] = task_name
            self._alias_lookup[task_name] = aliases

        if len(self._datasets) == 1:
            self.select_by_index(0)

    def remove_dataset(self, task_name):
        del self._datasets[task_name]
        if self._selected_task_name == task_name:
            self.select_by_index(0)

    @property
    def selected_index(self):
        return self._selected_index

    @property
    def selected_key(self):
        return self._selected_task_name

    @property
    def selected_aliases(self):
        return self._alias_lookup[self._selected_task_name]

    @property
    def selected_dataset(self):
        return self._datasets[self._selected_task_name]

    def select_by_index(self, index):
        self._selected_index = index
        self._selected_task_name = self.task_name_from_index(index)
        assert(self.index_from_task_name(self._selected_task_name) == index)

    def select_by_task_name(self, task_name):
        self._selected_task_name = task_name
        self._selected_index = self.index_from_task_name(task_name)
        assert (self.task_name_from_index(self._selected_index) == task_name)

    def select_by_alias(self, alias):
        self.select_by_task_name(self._aliases[alias])

    def add_task_history(self, task):
        self.selected_dataset.add_task_history(task)

    @property
    def task_history(self):
        return self.selected_dataset.task_history

    def __getitem__(self, item):
        return self.selected_dataset[item]

    def __setitem__(self, key, value):
        self.selected_dataset[key] = value

    def __delitem__(self, key):
        del self.selected_dataset[key]

    def dataset_from_index(self, index):
        return next(islice(self._datasets.values(), index, index+1))

    def dataset_from_alias(self, alias):
        return self.dataset_from_index(self.index_from_alias(alias))

    def task_name_from_index(self, index):
        return next(islice(self._datasets.keys(), index, index+1))

    def index_from_task_name(self, task_name):
        return list(self._datasets.keys()).index(task_name)

    def index_from_alias(self, alias):
        return self.index_from_task_name(self._aliases[alias])

    @property
    def first_dataset(self):
        return self.dataset_from_index(0)
    
    @property
    def first_task_name(self):
        return self.task_name_from_index(0)


class TaskData:
    def __init__(self, task_history=[], data=None):
        if data is None:
            self._data = {}
        else:
            self._data = data
        self._task_history = task_history

    def add_task_history(self, task):
        self._task_history.append(task)

    @property
    def task_history(self):
        return self._task_history

    def get(self, key):
        return self._data[key]

    def set(self, key, value):
        self._data[key] = value

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._data)

    def __str__(self):
        return str(self._data)


    # This pattern below doesn't work with celery (haven't worked out why)
    # def __getattr__(self, key):
    #     if key != '_data':
    #         return self._data[key]
    #     else:
    #         return object.__getattribute__(self, key)
    #
    # def __setattr__(self, key, value):
    #     if key != '_data':
    #         self._data[key] = value
    #     else:
    #         return object.__setattr__(self, key, value)
