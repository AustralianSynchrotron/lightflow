from lightflow.models.task import BaseTask, TaskState


def test_base_task_constructor():
    test_task = BaseTask('test')
    assert test_task.name == 'test'
    assert test_task.state == TaskState.Init
