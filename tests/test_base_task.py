from unittest.mock import Mock, create_autospec, call

import pytest  # noqa

from lightflow.models.task import BaseTask, TaskState, TaskStatus
from lightflow.queue import JobType
from lightflow.models.task_data import MultiTaskData
from lightflow.models.exceptions import AbortWorkflow, StopTask, TaskReturnActionInvalid
from lightflow.models.action import Action


@pytest.fixture
def task():
    yield BaseTask('task-name')


class CeleryResultMock:
    def __init__(self, *, state=None, ready=False, failed=False):
        self.state = state
        self._ready = ready
        self._failed = failed
        self._forget_called = False

    def ready(self):
        return self._ready

    def failed(self):
        return self._failed

    def forget(self):
        self._forget_called = True


def test_base_task_properties(task):
    assert task.name == 'task-name'
    assert task.state == TaskState.Init
    assert task.queue == JobType.Task
    assert task.has_to_run is False
    assert task.propagate_skip is True
    assert task.is_waiting is False
    assert task.is_running is False
    assert task.is_completed is False
    assert task.is_stopped is False
    assert task.is_aborted is False
    assert task.is_skipped is False
    assert task.celery_pending is False
    assert task.celery_completed is False
    assert task.celery_failed is False
    assert task.celery_state == 'NOT_QUEUED'
    assert task.has_celery_result is False


def test_base_task_skipped_setter(task):
    task.is_skipped = True
    assert task.is_skipped is True


def test_base_task_state_setter(task):
    task.state = TaskState.Waiting
    assert task.state == TaskState.Waiting


def test_base_task_celery_pending(task):
    task.celery_result = CeleryResultMock(state='PENDING')
    assert task.celery_pending is True


def test_base_task_celery_completed(task):
    task.celery_result = CeleryResultMock(ready=True)
    assert task.celery_completed is True


def test_base_task_celery_failed(task):
    task.celery_result = CeleryResultMock(failed=True)
    assert task.celery_failed is True


def test_base_task_celery_state(task):
    task.celery_result = CeleryResultMock(state='PENDING')
    assert task.celery_state == 'PENDING'


def test_base_task_clear_result(task):
    celery_result = CeleryResultMock()
    task.celery_result = celery_result
    task.clear_celery_result()
    assert celery_result._forget_called is True


def test_run_calls_callbacks(data_mock, store_mock, signal_mock, context_mock):
    init_cb = Mock()
    finally_cb = Mock()
    success_cb = Mock()
    stop_cb = Mock()
    abort_cb = Mock()
    task = BaseTask('task-name', callback_init=init_cb, callback_finally=finally_cb)
    task._run(data_mock, store_mock, signal_mock, context_mock,
              success_callback=success_cb, stop_callback=stop_cb, abort_callback=abort_cb)
    assert init_cb.call_args == call(data_mock, store_mock, signal_mock, context_mock)
    assert finally_cb.call_args == call(TaskStatus.Success, data_mock, store_mock, signal_mock, context_mock)
    assert success_cb.called is True
    assert stop_cb.called is False
    assert abort_cb.called is False


def test_run_calls_callback_finally_on_error(data_mock, store_mock, signal_mock, context_mock):

    class FailingTask(BaseTask):
        def run(self, *args, **kwargs):
            raise Exception()

    finally_cb = Mock()
    success_cb = Mock()
    stop_cb = Mock()
    abort_cb = Mock()
    task = FailingTask('task-name', callback_finally=finally_cb)
    with pytest.raises(Exception):
        task._run(data_mock, store_mock, signal_mock, context_mock,
                  success_callback=success_cb, stop_callback=stop_cb,
                  abort_callback=abort_cb)
    assert finally_cb.call_args == call(TaskStatus.Error, data_mock, store_mock, signal_mock, context_mock)
    assert success_cb.called is False
    assert stop_cb.called is False
    assert abort_cb.called is False


def test_run_calls_callback_finally_on_stop_task(data_mock, store_mock, signal_mock, context_mock):

    class StoppingTask(BaseTask):
        def run(self, *args, **kwargs):
            raise StopTask()

    finally_cb = Mock()
    success_cb = Mock()
    stop_cb = Mock()
    abort_cb = Mock()
    task = StoppingTask('task-name', callback_finally=finally_cb)
    task._run(data_mock, store_mock, signal_mock, context_mock,
              success_callback=success_cb, stop_callback=stop_cb, abort_callback=abort_cb)
    assert finally_cb.call_args == call(TaskStatus.Stopped, data_mock, store_mock, signal_mock, context_mock)
    assert success_cb.called is False
    assert stop_cb.called is True
    assert abort_cb.called is False


def test_run_calls_callback_finally_on_abort_workflow(data_mock, store_mock, signal_mock, context_mock):

    class AbortingTask(BaseTask):
        def run(self, *args, **kwargs):
            raise AbortWorkflow()

    finally_cb = Mock()
    success_cb = Mock()
    stop_cb = Mock()
    abort_cb = Mock()
    task = AbortingTask('task-name', callback_finally=finally_cb)
    task._run(data_mock, store_mock, signal_mock, context_mock,
              success_callback=success_cb, stop_callback=stop_cb, abort_callback=abort_cb)
    assert finally_cb.call_args == call(TaskStatus.Aborted, data_mock, store_mock, signal_mock, context_mock)
    assert success_cb.called is False
    assert stop_cb.called is False
    assert abort_cb.called is True


def test_run_handles_invalid_result(data_mock, store_mock, signal_mock, context_mock):

    class InvalidResultTask(BaseTask):
        def run(self, *args, **kwargs):
            return 'whoops'

    with pytest.raises(TaskReturnActionInvalid):
        InvalidResultTask('task-name')._run(data_mock, store_mock, signal_mock, context_mock)


def test_run_handles_action_response(data_mock, store_mock, signal_mock, context_mock):

    run_result = Action(create_autospec(MultiTaskData, instance=True))

    class Task(BaseTask):
        def run(self, *args, **kwargs):
            return run_result

    result = Task('task-name')._run(data_mock, store_mock, signal_mock, context_mock)
    assert result == run_result


def test_run_handles_no_data(store_mock, signal_mock, context_mock):
    result = BaseTask('task-name')._run(None, store_mock, signal_mock, context_mock)
    assert result.data is not None
