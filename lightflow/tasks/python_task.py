from lightflow.models import BaseTask, run_task


class PythonTask(BaseTask):
    def __init__(self, name, dag, python_callable=None):
        super().__init__(name, dag)
        self._python_callable = python_callable

    @run_task
    def run(self):
        if self._python_callable is not None:
            self._python_callable(self.name)
