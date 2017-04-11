import click
import json

from lightflow.config import Config
from lightflow.version import __version__
from lightflow.queue.const import JobType
from lightflow.models.exceptions import (WorkflowArgumentError,
                                         WorkflowImportError,
                                         WorkerQueueUnknownError)

from lightflow.workers import (start_worker, stop_worker, list_workers)
from lightflow.workflows import (start_workflow, stop_workflow, list_workflows,
                                 list_jobs)
from lightflow.workflows import events as workflow_events


JOB_COLOR = {
    JobType.Workflow: 'green', JobType.Dag: 'yellow', JobType.Task: 'magenta'
}


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version=__version__, prog_name='Lightflow')
@click.option('--config', '-c', help='Path to configuration file.')
@click.option('--no-color', '-n', is_flag=True, help='Turn colored output off.')
@click.pass_context
def cli(ctx, config, no_color):
    """ Command line client for lightflow. A lightweight, high performance pipeline
    system for synchrotrons.

    Lightflow is being developed at the Australian Synchrotron.
    """
    ctx.obj = {'config': Config.from_file(config),
               'show_color': not no_color if no_color is not None else True}


@cli.group()
def config():
    """ Manage the configuration. """
    pass


@config.command('default')
def config_default():
    """ Print a default configuration. """
    click.echo(Config.default())


@config.command('list')
@click.pass_obj
def config_list(obj):
    """ List the current configuration. """
    click.echo(json.dumps(obj['config'].to_dict(), indent=4))


@cli.group()
def workflow():
    """ Start, stop and list workflows. """
    pass


@workflow.command('list')
@click.pass_obj
def workflow_list(obj):
    """ List all available workflows. """
    for wf in list_workflows(config=obj['config']):
        click.echo('{:23} {}'.format(
            _style(obj['show_color'], wf.name, bold=True),
            wf.docstring.split('\n')[0] if wf.docstring is not None else ''))


@workflow.command('start')
@click.option('--keep-data', '-k', is_flag=True, default=False,
              help='Do not delete the workflow data.')
@click.argument('name')
@click.argument('workflow_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def workflow_start(obj, keep_data, name, workflow_args):
    """ Send a workflow to the queue.

    \b
    NAME: The name of the workflow that should be started.
    WORKFLOW_ARGS: Workflow arguments in the form key1=value1 key2=value2.
    """
    try:
        start_workflow(name=name,
                       config=obj['config'],
                       clear_data_store=not keep_data,
                       store_args=dict([arg.split('=', maxsplit=1)
                                        for arg in workflow_args]))
    except (WorkflowArgumentError, WorkflowImportError) as e:
        click.echo(_style(obj['show_color'],
                          'An error occurred when trying to start the workflow',
                          fg='red', bold=True))
        click.echo('{}'.format(e))


@workflow.command('stop')
@click.argument('names', nargs=-1)
@click.pass_obj
def workflow_stop(obj, names):
    """ Stop one or more running workflows.
    
    \b
    NAMES: The names, ids or job ids of the workflows that should be stopped.
           Leave empty to stop all running workflows.
    """
    if len(names) == 0:
        msg = 'Would you like to stop all workflows?'
    else:
        msg = '\n{}\n\n{}'.format('\n'.join(names),
                                  'Would you like to stop these jobs?')

    if click.confirm(msg, default=True, abort=True):
        stop_workflow(obj['config'], names=names if len(names) > 0 else None)


@workflow.command('status')
def workflow_status():
    click.echo('workflow status command')


@cli.group()
def worker():
    """ Start and stop workers. """
    pass


@worker.command('start', context_settings=dict(ignore_unknown_options=True,))
@click.option('--queues', '-q',
              default='{},{},{}'.format(JobType.Workflow, JobType.Dag, JobType.Task),
              help='Comma separated list of queues to enable for this worker.')
@click.option('--name', '-n', default=None, help='Name of the worker.')
@click.argument('celery_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def worker_start(obj, queues, name, celery_args):
    """ Start a worker process.

    \b
    CELERY_ARGS: Additional Celery worker command line arguments.
    """
    try:
        start_worker(queues=queues.split(','),
                     config=obj['config'],
                     name=name,
                     celery_args=celery_args)
    except WorkerQueueUnknownError:
        click.echo(_style(obj['show_color'],
                          'One of the specified queues is not a valid queue name',
                          fg='red', bold=True))


@worker.command('stop')
@click.argument('worker_ids', nargs=-1)
@click.pass_obj
def worker_stop(obj, worker_ids):
    """ Stop running workers.

    \b
    WORKER_IDS: The IDs of the worker that should be stopped or none to stop them all.
    """
    if len(worker_ids) == 0:
        msg = 'Would you like to stop all workers?'
    else:
        msg = '\n{}\n\n{}'.format('\n'.join(worker_ids),
                                  'Would you like to stop these workers?')

    if click.confirm(msg, default=True, abort=True):
        stop_worker(obj['config'],
                    worker_ids=list(worker_ids) if len(worker_ids) > 0 else None)


@worker.command('status')
@click.option('--filter-queues', '-f', default=None,
              help='Only show workers for this comma separated list of queues.')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed worker information.')
@click.pass_obj
def worker_status(obj, filter_queues, verbose):
    """ Show the status of all running workers. """
    show_colors = obj['show_color']

    f_queues = filter_queues.split(',') if filter_queues is not None else None

    workers = list_workers(config=obj['config'], filter_by_queues=f_queues)
    if len(workers) == 0:
        click.echo('No workers are running at the moment.')
        return

    for ws in workers:
        click.echo('{} {}'.format(_style(show_colors, 'Worker:', fg='blue', bold=True),
                                  _style(show_colors, ws.name, fg='blue')))
        click.echo('{:23} {}'.format(_style(show_colors, '> pid:', bold=True), ws.pid))

        if verbose:
            click.echo('{:23} {}'.format(_style(show_colors, '> concurrency:', bold=True),
                                         ws.concurrency))
            click.echo('{:23} {}'.format(_style(show_colors, '> processes:', bold=True),
                                         ', '.join(str(p) for p in ws.process_pids)))
            click.echo('{:23} {}://{}:{}/{}'.format(_style(show_colors, '> broker:',
                                                           bold=True),
                                                    ws.broker.transport,
                                                    ws.broker.hostname,
                                                    ws.broker.port,
                                                    ws.broker.virtual_host))

        click.echo('{:23} {}'.format(_style(show_colors, '> queues:', bold=True),
                                     ', '.join([q.name for q in ws.queues])))

        if verbose:
            click.echo('{:23} {}'.format(_style(show_colors, '> job count:', bold=True),
                                         ws.job_count))

            jobs = list_jobs(config=obj['config'], filter_by_worker=ws.name)
            click.echo('{:23} [{}]'.format(_style(show_colors, '> jobs:', bold=True),
                                           len(jobs) if len(jobs) > 0 else 'No tasks'))

            for job in jobs:
                click.echo('{:15} {} {}'.format(
                    '',
                    _style(show_colors, '{}'.format(job.name),
                           bold=True, fg=JOB_COLOR[job.type]),
                    _style(show_colors, '({}) [{}] <{}> on {}'.format(
                        job.type, job.workflow_id, job.id, job.worker_pid),
                        fg=JOB_COLOR[job.type])))

        click.echo('\n')


@cli.command('monitor')
@click.pass_obj
def monitor(obj):
    """ Show the worker and workflow event stream. """
    show_colors = obj['show_color']

    click.echo('\n')
    click.echo('{:>10} {:>12} {:20} [{}]'.format('Status',
                                                 'Type',
                                                 'Name', 'Workflow ID'))
    click.echo('-' * 63)
    for event in workflow_events(obj['config']):
        click.echo('{:>10} {:>{}} {:20} [{}]'.format(
            event.label.upper(),
            _style(show_colors, event.type, bold=True, fg=JOB_COLOR[event.type]),
            25 if show_colors else 12,
            event.name,
            event.workflow_id))


def _style(enabled, text, **kwargs):
    """ Helper function to enable/disable styled output text.

    Args:
        enable (bool): Turn on or off styling.
        text (string): The string that should be styled.
        kwargs (dict): Parameters that are passed through to click.style

    Returns:
        string: The input with either the styling applied (enabled=True)
                or just the text (enabled=False)
    """
    if enabled:
        return click.style(text, **kwargs)
    else:
        return text

if __name__ == '__main__':
    cli(obj={})
