from lightflow.models import BaseTask


class PythonTask(BaseTask):
    def __init__(self, name, dag, python_callable=None):
        super().__init__(name, dag)
        self._python_callable = python_callable

    def run(self, data, **kwargs):
        if self._python_callable is not None:
            return self._python_callable(self.name, data, **kwargs)
