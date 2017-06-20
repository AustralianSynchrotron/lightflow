from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest  # noqa

from lightflow.tasks.bash_task import BashTask
from lightflow.models.exceptions import StopTask, AbortWorkflow


def test_bash_task_executes_command(tmpdir, data_mock, store_mock, signal_mock, context_mock):
    tmp_file_path = Path(str(tmpdir.mkdir('bash-task').join('target.txt')))
    callback = Mock()
    command = 'echo ok > {target_path}'.format(target_path=tmp_file_path)
    task = BashTask('task-name', command, callback_process=callback)
    task.run(data_mock, store_mock, signal_mock, context_mock)
    assert callback.called is True
    assert tmp_file_path.open().read().strip() == 'ok'


def test_bash_task_calls_stdout_callback(data_mock, store_mock, signal_mock, context_mock):
    stdout_callback, stderr_callback = Mock(), Mock()
    task = BashTask('task-name',  'echo line1; echo line2', callback_stdout=stdout_callback,
                    callback_stderr=stderr_callback)
    task.run(data_mock, store_mock, signal_mock, context_mock)
    assert stdout_callback.call_args_list == [
        call('line1\n', data_mock, store_mock, signal_mock, context_mock),
        call('line2\n', data_mock, store_mock, signal_mock, context_mock),
    ]
    assert stderr_callback.called is False


def test_bash_task_calls_stderr_callback(data_mock, store_mock, signal_mock, context_mock):
    stdout_callback, stderr_callback = Mock(), Mock()
    task = BashTask('task-name', 'invalid-command-blerg', callback_stdout=stdout_callback,
                    callback_stderr=stderr_callback)
    task.run(data_mock, store_mock, signal_mock, context_mock)
    stderr_callback_args = stderr_callback.call_args[0]
    assert 'not found' in stderr_callback_args[0]
    assert stderr_callback_args[1:] == (data_mock, store_mock, signal_mock, context_mock)
    assert stdout_callback.called is False


def test_bash_task_captures_io_to_file(data_mock, store_mock, signal_mock, context_mock):

    supplied_return_code = None
    stdout_file_contents = None
    stderr_file_contents = None

    def end_callback(return_code, stdout_file, stderr_file, data, store, signal, context):
        nonlocal supplied_return_code, stdout_file_contents, stderr_file_contents
        supplied_return_code = return_code
        stdout_file_contents = stdout_file.read()
        stderr_file_contents = stderr_file.read()

    task = BashTask('task-name', 'echo ok; invalid-command-blerg',
                    capture_stdout=True, capture_stderr=True,
                    callback_end=end_callback)
    task.run(data_mock, store_mock, signal_mock, context_mock)
    assert supplied_return_code > 0
    assert stdout_file_contents == b'ok\n'
    assert b'not found' in stderr_file_contents


@pytest.mark.parametrize('ExceptionType', [StopTask, AbortWorkflow])
@patch('lightflow.tasks.bash_task.Popen')
def test_terminates_the_process_if_stopped_or_aborted(PopenMock, ExceptionType, data_mock, store_mock,
                                                      signal_mock, context_mock):

    def process_callback(*args):
        raise ExceptionType()

    task = BashTask('task-name', 'echo ok', callback_process=process_callback)
    with pytest.raises(ExceptionType):
        task.run(data_mock, store_mock, signal_mock, context_mock)
    assert PopenMock.return_value.terminate.called is True
