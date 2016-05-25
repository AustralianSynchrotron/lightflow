from lightflow.models import BaseTask


class JoinTask(BaseTask):
    def __init__(self, name, dag, wait_count=-1):
        super().__init__(name, dag)
        self._wait_count = wait_count

        # task parameters
        self._max_inputs = -1

    def check_predecessors(self, predecessor_ids):
        if self._wait_count < 0:
            return super().check_predecessors(predecessor_ids)
        else:
            return sum([self._dag.get_task_by_id(n).is_finished() for n in predecessor_ids]) >= self._wait_count

    def run(self):
        print(self._name)
