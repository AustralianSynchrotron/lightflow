from typing import Callable

from lightflow.tasks import Task


class PythonTask(Task):
    def __init__(self, name: str, callback: Callable):
        super().__init__(name)
        self._callback = callback

    def run(self):
        # TODO: don't
        self._callback(data={'number': 1})
