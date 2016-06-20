from copy import copy


class Action:
    def __init__(self, data, selected_tasks=None):
        self._data = data
        self._selected_tasks = selected_tasks

    @property
    def data(self):
        return self._data

    @property
    def selected_tasks(self):
        return self._selected_tasks

    def copy(self):
        return copy(self)
