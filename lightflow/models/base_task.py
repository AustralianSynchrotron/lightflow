
class BaseTask:
    def __init__(self, name, dag):
        self._name = name
        self.celery_result = None

        self._dag = dag

    @property
    def name(self):
        return self._name

    @property
    def is_queued(self):
        return self.celery_result is not None

    @property
    def is_finished(self):
        if self.is_queued:
            return self.celery_result.successful()
        else:
            return False

    def add_upstream(self, task):
        self._dag.add_edge(self, task)

    def add_downstream(self, task):
        self._dag.add_edge(task, self)

    def add(self, task):
        self.add_downstream(task)

    def run(self):
        pass
