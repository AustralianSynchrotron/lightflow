from socro.models import BaseTask, run_task


class BranchingTask(BaseTask):
    def __init__(self, name, dag, python_callable=None):
        super().__init__(name, dag)
        self._python_callable = python_callable

    def limit_successors(self):
        # TODO: handle the case that the name doesn't exist
        if (self._python_callable is not None) and (self._python_callable() is not None):
            return [self._dag.get_task_by_name(name) for name in self._python_callable()]
        else:
            return None

    @run_task
    def run(self):
        print(self._name)
