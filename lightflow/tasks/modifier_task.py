from lightflow.models import BaseTask


class ModifierTask(BaseTask):
    def __init__(self, name, dag, python_callable=None):
        super().__init__(name, dag)
        self._python_callable = python_callable

    def run(self):
        if self._python_callable is not None:
            self._python_callable(self._dag)
