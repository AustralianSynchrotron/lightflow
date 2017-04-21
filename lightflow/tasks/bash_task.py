import os
import sys
from time import sleep
from threading import Thread
from subprocess import Popen, PIPE
from tempfile import TemporaryFile

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters, Action


logger = get_logger(__name__)


class BashTaskOutputReader(Thread):
    """ Helper class to read the output of the process. """
    def __init__(self, process, stdout_file, stderr_file,
                 callback_stdout, callback_stderr, refresh_time,
                 data, store, signal, context):
        """ Initializes the bash task reader thread. 

        Args:
            process: Reference to the Popen object.
            stdout_file: The file object for the standard output of the process.
            stderr_file: The file object for the standard error of the process.
            callback_stdout: The callback that should be called for every line of the
                             standard output.
            callback_stderr: The callback that should be called for every line of the
                             standard error.
            refresh_time (float): The time in seconds before checking for new output
                                  from the process.
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.
            context (TaskContext): The context in which the tasks runs. 
        """
        super().__init__()

        self._process = process
        self._stdout_file = stdout_file
        self._stderr_file = stderr_file
        self._callback_stdout = callback_stdout
        self._callback_stderr = callback_stderr
        self._refresh_time = refresh_time
        self._data = data
        self._store = store
        self._signal = signal
        self._context = context

    @property
    def data(self):
        return self._data

    def run(self):
        """ Drain the subprocesse's output streams. """
        while self._process.poll():
            if not self._read_output(self._process.stdout,
                                     self._callback_stdout,
                                     self._stdout_file):
                sleep(0.5 * self._refresh_time)

            if not self._read_output(self._process.stderr,
                                     self._callback_stderr,
                                     self._stderr_file):
                sleep(0.5 * self._refresh_time)

        # read remaining lines
        while not self._process.stdout.closed:
            if not self._read_output(self._process.stdout,
                                     self._callback_stdout,
                                     self._stdout_file):
                break

        while not self._process.stderr.closed:
            if not self._read_output(self._process.stderr,
                                     self._callback_stderr,
                                     self._stderr_file):
                break

    def _read_output(self, stream, callback, output_file):
        line = stream.readline()

        if line:
            if callback is not None:
                callback(line.decode(),
                         self._data, self._store, self._signal, self._context)

            if output_file is not None:
                output_file.write(line)

            return True
        else:
            return False


class BashTask(BaseTask):
    """ The Bash task executes a user-defined bash command or bash file. """
    def __init__(self, name, command, cwd=None, env=None, user=None, group=None,
                 stdin=None, refresh_time=0.1,
                 aggregate_stdout=False, aggregate_stderr=False,
                 callback_start=None, callback_end=None,
                 callback_stdout=None, callback_stderr=None,
                 *, queue=JobType.Task, force_run=False, propagate_skip=True):
        """ Initialize the Bash task.

        All task parameters except the name, callbacks, queue, force_run and
        propagate_skip can either be their native type or a callable returning
        the native type.

        Args:
            name (str): The name of the task.
            command (str): The command or bash file that should be executed.
            cwd (str, None): The working directory for the command.
            env (dict, None): A dictionary of environment variables.
            user (int, None): The user ID of the user with which the command should be
                              executed.
            group (int, None): The group ID of the group with which the command should be
                               executed.
            stdin (str, None): An input string that should be passed on to the process.
            refresh_time (float): The time in seconds the internal output handling waits
                                  before checking for new output from the process.
            aggregate_stdout (bool): Set to true to capture all standard output in a
                                     temporary file.
            aggregate_stderr (bool): Set to true to capture all standard errors in a
                                     temporary file.
            callback_start: A callable that is called after the process started.
                            The definition is:
                              def (pid, data, store, signal, context)
                            where the pid is the process PID, data the task data,
                            store the workflow data store, signal the task signal and
                            context the task context.
            callback_end: A callable that is called after the process completed.
                          The definition is:
                            def (returncode, stdout_file, stderr_file, data, store,
                                 signal, context)
                          where returncode is the return code of the process and
                          stdout_file/stderr_file a file object with the standard/error
                          output if the flag aggregate_stdout/aggregate_stderr was set to
                          True, otherwise None. The remaining parameters are identical
                          to callback_start.
            callback_stdout: A callable that is called for every line of output the
                             process sends to stdout. The definition is:
                               def (line, data, store, signal, context)
                             where line is a single line of the output, data the task
                             data, store the workflow data store, signal the task signal
                             and context the task context.
            callback_stderr: A callable that is called for every line of output the
                             process sends to stderr. The definition is:
                               def (line, data, store, signal, context)
                             where line is a single line of the output, data the task
                             data, store the workflow data store, signal the task signal
                             and context the task context.
            queue (str): Name of the queue the task should be scheduled to. Defaults to
                         the general task queue.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, queue=queue,
                         force_run=force_run, propagate_skip=propagate_skip)

        self.params = TaskParameters(
            command=command,
            cwd=cwd,
            env=env,
            user=user,
            group=group,
            stdin=stdin,
            refresh_time=refresh_time,
            aggregate_stdout=aggregate_stdout,
            aggregate_stderr=aggregate_stderr
        )

        self._callback_start = callback_start
        self._callback_end = callback_end
        self._callback_stdout = callback_stdout
        self._callback_stderr = callback_stderr

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the Python task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.
            context (TaskContext): The context in which the tasks runs.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, store)

        # check whether the output should be captured
        capture_output = self._callback_stdout is not None or \
            self._callback_stderr is not None or \
            params.aggregate_stdout or params.aggregate_stderr

        if capture_output:
            stdout = PIPE
            stderr = PIPE
        else:
            stdout = None
            stderr = None

        # change the user or group under which the process should run
        if params.user is not None or params.group is not None:
            pre_exec = self._run_as(params.user, params.group)
        else:
            pre_exec = None

        # call the command
        proc = Popen(params.command, cwd=params.cwd, shell=True, env=params.env,
                     preexec_fn=pre_exec, stdout=stdout, stderr=stderr,
                     stdin=PIPE if params.stdin is not None else None)

        # if input is available, send it to the process
        if params.stdin is not None:
            proc.stdin.write(params.stdin.encode(sys.getfilesystemencoding()))

        # send a notification that the process has been started
        if self._callback_start is not None:
            self._callback_start(proc.pid, data, store, signal, context)

        # create a temp file if the output should be aggregated
        if params.aggregate_stdout:
            stdout_file = TemporaryFile()
        else:
            stdout_file = None

        if params.aggregate_stderr:
            stderr_file = TemporaryFile()
        else:
            stderr_file = None

        # send the output handling to a thread
        if capture_output:
            output_reader = BashTaskOutputReader(proc, stdout_file, stderr_file,
                                                 self._callback_stdout,
                                                 self._callback_stderr,
                                                 params.refresh_time,
                                                 data, store, signal, context)
            output_reader.start()
        else:
            output_reader = None

        # wait for the process to complete and watch for a stop signal
        while proc.poll() is None:
            sleep(params.refresh_time)
            if signal.is_stopped:
                proc.terminate()

        if output_reader is not None:
            output_reader.join()
            data = output_reader.data

        # send a notification that the process has completed
        if self._callback_end is not None:
            stdout_file.seek(0)
            stderr_file.seek(0)
            self._callback_end(proc.returncode, stdout_file, stderr_file,
                               data, store, signal, context)

        if stdout_file is not None:
            stdout_file.close()

        if stderr_file is not None:
            stderr_file.close()

        return Action(data)

    @staticmethod
    def _run_as(user, group):
        """ Function wrapper that sets the user and group for the process """
        def wrapper():
            if user is not None:
                os.setgid(user)
            if group is not None:
                os.setuid(group)
        return wrapper
