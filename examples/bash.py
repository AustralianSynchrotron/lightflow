""" Use the BashTask to execute bash commands and shell scripts

This workflow demonstrates the use of the BashTask. A simple bash command is executed
and each line in stdout is displayed as well as counted. The full output is captured as
well and displayed after the process completed.

"""

from lightflow.models import Dag
from lightflow.tasks import BashTask


# this callback is executed after the process was started and before the stdout and stderr
# readers are started. Set the line counter to zero.
def proc_start(pid, data, store, signal, context):
    data['num_lines'] = 0


# this callback is called for each line of stdout. Print each line and increase
# the line counter.
def proc_stdout(line, data, store, signal, context):
    print(line.rstrip())
    data['num_lines'] += 1


# this callback is called after the process completed. Print the line counter and the
# full output of stdout and stderr.
def proc_end(return_code, stdout_file, stderr_file, data, store, signal, context):
    print('\n')
    print('Process return code: {}'.format(return_code))
    print('Number lines: {}'.format(data['num_lines']))
    print('\n')
    print('stdout:\n{}\n'.format(stdout_file.read().decode()))
    print('stderr:\n{}\n'.format(stderr_file.read().decode()))


# create the main DAG and the bash task. Please note how the output of stderr is being
# handled by the stdout callback.
d = Dag('main_dag')

proc_task = BashTask(name='proc_task',
                     command='for i in `seq 1 10`; do echo "This is line $i"; done',
                     capture_stdout=True,
                     capture_stderr=True,
                     callback_stdout=proc_stdout,
                     callback_stderr=proc_stdout,
                     callback_process=proc_start,
                     callback_end=proc_end)

# this DAG has only a single task
d.define({
    proc_task: None
})
