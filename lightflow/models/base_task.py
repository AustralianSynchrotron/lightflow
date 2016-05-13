
def run_task(func):
    def func_wrapper(self):
        self._running = True
        self._finished = False
        func(self)
        self._running = False
        self._finished = True
    return func_wrapper


class BaseTask:
    def __init__(self, name, dag):
        self._name = name
        self._dag = dag

        self._task_id = self._dag.add_task(self)
        self._running = False
        self._finished = False
        self._data = {}

        # task parameters
        self._max_inputs = 1

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._task_id

    @property
    def max_inputs(self):
        return self._max_inputs

    def is_finished(self):
        return self._finished

    def is_running(self):
        return self._running

    def can_run(self):
        return not (self._running or self._finished)

    def add_upstream(self, task):
        self._dag.add_edge(self, task)

    def add_downstream(self, task):
        self._dag.add_edge(task, self)

    def add(self, task):
        self.add_downstream(task)

    def reset(self):
        self._running = False
        self._finished = False

    def limit_successors(self):
        return None

    def check_predecessors(self, predecessor_ids):
        return all([self._dag.get_task_by_id(n).is_finished() for n in predecessor_ids])

    @run_task
    def run(self):
        pass
