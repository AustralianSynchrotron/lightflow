from lightflow.models.action import Action
from lightflow.models.task_data import MultiTaskData


class BaseTask:
    def __init__(self, name, dag, force_run=False):
        self._name = name
        self._dag = dag
        self._force_run = force_run

        self.celery_result = None
        self._skip = False

    @property
    def name(self):
        return self._name

    @property
    def is_queued(self):
        return self.celery_result is not None

    @property
    def is_finished(self):
        if self.is_queued:
            return self.celery_result.ready()
        else:
            return False

    @property
    def is_skipped(self):
        return self._skip

    @property
    def state(self):
        if self.is_queued:
            return self.celery_result.state
        else:
            return "NOT_QUEUED"

    def skip(self):
        self._skip = True

    def add_upstream(self, task):
        self._dag.add_edge(self, task)

    def add_downstream(self, task):
        self._dag.add_edge(task, self)

    def add(self, task):
        self.add_downstream(task)

    def _run(self, data=None):
        if data is None:
            data = MultiTaskData(self._name)

        # TODO: check that result is of type Action. If not throw exception.
        if not self.is_skipped or self._force_run:
            result = self.run(data)
        else:
            result = None

        if result is None:
            return Action(data.copy())
        else:
            result.data.add_task_history(self.name)
            return result.copy()

    def run(self, data, **kwargs):
        pass
