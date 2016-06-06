import pytest
from lightflow.models.base_task import BaseTask
from lightflow.models.task_data import MultiTaskData


class TestBaseTask:
    def test_constructor(self):
        test_task = BaseTask('test', None)

        assert test_task.name == 'test'
        assert test_task.state == "NOT_QUEUED"

    def test_dummy_run(self):
        test_task = BaseTask('test', None)
        result = test_task._run()
        assert isinstance(result, MultiTaskData)