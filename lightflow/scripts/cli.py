import pytz
import click
import json
import shutil
from pathlib import Path
from functools import update_wrapper
from importlib import import_module

import lightflow
from lightflow.config import Config, LIGHTFLOW_CONFIG_NAME
from lightflow.version import __version__
from lightflow.queue.const import JobType, JobEventName, JobStatus, DefaultJobQueueName
from lightflow.models.exceptions import (ConfigLoadError,
                                         WorkflowArgumentError,
                                         WorkflowImportError,
                                         WorkflowDefinitionError)

from lightflow.workers import (start_worker, stop_worker, list_workers)
from lightflow.workflows import (start_workflow, stop_workflow, list_workflows,
                                 list_jobs)
from lightflow.workflows import events as workflow_events


JOB_COLOR = {
    JobType.Workflow: 'green', JobType.Dag: 'yellow', JobType.Task: 'magenta'
}


def config_required(f):
    """ Decorator that checks whether a configuration file was set. """
    def new_func(obj, *args, **kwargs):
        if 'config' not in obj:
            click.echo(_style(obj.get('show_color', False),
                              'Could not find a valid configuration file!',
                              fg='red', bold=True))
            raise click.Abort()
        else:
            return f(obj, *args, **kwargs)
    return update_wrapper(new_func, f)


def ingest_config_obj(ctx, *, silent=True):
    """ Ingest the configuration object into the click context. """
    try:
        ctx.obj['config'] = Config.from_file(ctx.obj['config_path'])
    except ConfigLoadError as err:
        click.echo(_style(ctx.obj['show_color'], str(err), fg='red', bold=True))
        if not silent:
            raise click.Abort()


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
    ctx.obj = {
        'show_color': not no_color if no_color is not None else True,
        'config_path': config
    }


@cli.group()
def config():
    """ Manage the configuration. """
    pass


@config.command('default')
@click.argument('dest', type=click.Path(exists=True))
def config_default(dest):
    """ Create a default configuration file.

    \b
    DEST: Path or file name for the configuration file.
    """
    conf_path = Path(dest).resolve()
    if conf_path.is_dir():
        conf_path = conf_path / LIGHTFLOW_CONFIG_NAME

    conf_path.write_text(Config.default())
    click.echo('Configuration written to {}'.format(conf_path))


@config.command('list')
@click.pass_context
def config_list(ctx):
    """ List the current configuration. """
    ingest_config_obj(ctx, silent=False)
    click.echo(json.dumps(ctx.obj['config'].to_dict(), indent=4))


@config.command('examples')
@click.option('--user-dir', '-u', is_flag=True,
              help='Do not create subdirectory /example.')
@click.argument('dest', type=click.Path(exists=True))
def config_examples(dest, user_dir):
    """ Copy the example workflows to a directory.

    \b
    DEST: Path to which the examples should be copied.
    """
    examples_path = Path(lightflow.__file__).parents[1] / 'examples'
    if examples_path.exists():
        dest_path = Path(dest).resolve()
        if not user_dir:
            dest_path = dest_path / 'examples'

        if dest_path.exists():
            if not click.confirm('Directory already exists. Overwrite existing files?',
                                 default=True, abort=True):
                return
        else:
            dest_path.mkdir()

        for example_file in examples_path.glob('*.py'):
            shutil.copy(str(example_file), str(dest_path / example_file.name))
        click.echo('Copied examples to {}'.format(str(dest_path)))
    else:
        click.echo('The examples source path does not exist')


@cli.group()
@click.pass_context
def workflow(ctx):
    """ Start, stop and list workflows. """
    ingest_config_obj(ctx)


@workflow.command('list')
@click.pass_obj
@config_required
def workflow_list(obj):
    """ List all available workflows. """
    try:
        for wf in list_workflows(config=obj['config']):
            click.echo('{:23} {}'.format(
                _style(obj['show_color'], wf.name, bold=True),
                wf.docstring.split('\n')[0] if wf.docstring is not None else ''))
    except WorkflowDefinitionError as e:
        click.echo(_style(obj['show_color'],
                          'The graph {} in workflow {} is not a directed acyclic graph'.
                          format(e.graph_name, e.workflow_name), fg='red', bold=True))


@workflow.command('start')
@click.option('--queue', '-q',
              default=DefaultJobQueueName.Workflow,
              help='Name of the queue the workflow should be scheduled to.')
@click.option('--keep-data', '-k', is_flag=True, default=False,
              help='Do not delete the workflow data.')
@click.argument('name')
@click.argument('workflow_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
@config_required
def workflow_start(obj, queue, keep_data, name, workflow_args):
    """ Send a workflow to the queue.

    \b
    NAME: The name of the workflow that should be started.
    WORKFLOW_ARGS: Workflow arguments in the form key1=value1 key2=value2.
    """
    try:
        start_workflow(name=name,
                       config=obj['config'],
                       queue=queue,
                       clear_data_store=not keep_data,
                       store_args=dict([arg.split('=', maxsplit=1)
                                        for arg in workflow_args]))
    except (WorkflowArgumentError, WorkflowImportError) as e:
        click.echo(_style(obj['show_color'],
                          'An error occurred when trying to start the workflow',
                          fg='red', bold=True))
        click.echo('{}'.format(e))
    except WorkflowDefinitionError as e:
        click.echo(_style(obj['show_color'],
                          'The graph {} in workflow {} is not a directed acyclic graph'.
                          format(e.graph_name, e.workflow_name), fg='red', bold=True))


@workflow.command('stop')
@click.argument('names', nargs=-1)
@click.pass_obj
@config_required
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
@click.option('--details', '-d', is_flag=True, help='Show detailed information.')
@click.pass_obj
@config_required
def workflow_status(obj, details):
    """ Show the status of the workflows. """
    show_colors = obj['show_color']
    config_cli = obj['config'].cli

    if details:
        temp_form = '{:>{}}  {:20} {:25} {:25} {:38} {}'
    else:
        temp_form = '{:>{}}  {:20} {:25} {} {} {}'

    click.echo('\n')
    click.echo(temp_form.format(
        'Status',
        12,
        'Name',
        'Start Time',
        'ID' if details else '',
        'Job' if details else '',
        'Arguments'
    ))
    click.echo('-' * (138 if details else 75))

    def print_jobs(jobs, *, label='', color='green'):
        for job in jobs:
            start_time = job.start_time if job.start_time is not None else 'unknown'

            click.echo(temp_form.format(
                _style(show_colors, label, fg=color, bold=True),
                25 if show_colors else 12,
                job.name,
                start_time.replace(tzinfo=pytz.utc).astimezone().strftime(
                    config_cli['time_format']),
                job.workflow_id if details else '',
                job.id if details else '',
                ','.join(['{}={}'.format(k, v) for k, v in job.arguments.items()]))
            )

    # running jobs
    print_jobs(list_jobs(config=obj['config'],
                         status=JobStatus.Active,
                         filter_by_type=JobType.Workflow),
               label='Running', color='green')

    # scheduled jobs
    print_jobs(list_jobs(config=obj['config'],
                         status=JobStatus.Scheduled,
                         filter_by_type=JobType.Workflow),
               label='Scheduled', color='blue')

    # registered jobs
    print_jobs(list_jobs(config=obj['config'],
                         status=JobStatus.Registered,
                         filter_by_type=JobType.Workflow),
               label='Registered', color='yellow')

    # reserved jobs
    print_jobs(list_jobs(config=obj['config'],
                         status=JobStatus.Reserved,
                         filter_by_type=JobType.Workflow),
               label='Reserved', color='yellow')


@cli.group()
@click.pass_context
def worker(ctx):
    """ Start and stop workers. """
    ingest_config_obj(ctx)


@worker.command('start', context_settings=dict(ignore_unknown_options=True,))
@click.option('--queues', '-q',
              default='{},{},{}'.format(DefaultJobQueueName.Workflow,
                                        DefaultJobQueueName.Dag,
                                        DefaultJobQueueName.Task),
              help='Comma separated list of queues to enable for this worker.')
@click.option('--name', '-n', default=None, help='Name of the worker.')
@click.argument('celery_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
@config_required
def worker_start(obj, queues, name, celery_args):
    """ Start a worker process.

    \b
    CELERY_ARGS: Additional Celery worker command line arguments.
    """
    start_worker(queues=queues.split(','),
                 config=obj['config'],
                 name=name,
                 celery_args=celery_args)


@worker.command('stop')
@click.argument('worker_ids', nargs=-1)
@click.pass_obj
@config_required
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
@click.option('--details', '-d', is_flag=True, help='Show detailed worker information.')
@click.pass_obj
@config_required
def worker_status(obj, filter_queues, details):
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

        if details:
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

        if details:
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
@click.option('--details', '-d', is_flag=True, help='Show detailed information.')
@click.pass_context
def monitor(ctx, details):
    """ Show the worker and workflow event stream. """
    ingest_config_obj(ctx, silent=False)

    show_colors = ctx.obj['show_color']

    event_display = {
        JobEventName.Started: {'color': 'blue', 'label': 'started'},
        JobEventName.Succeeded: {'color': 'green', 'label': 'succeeded'},
        JobEventName.Stopped: {'color': 'yellow', 'label': 'stopped'},
        JobEventName.Aborted: {'color': 'red', 'label': 'aborted'}
    }

    click.echo('\n')
    click.echo('{:>10} {:>12} {:25} {:18} {:16} {:28} {}'.format(
        'Status',
        'Type',
        'Name',
        'Duration (sec)',
        'Queue' if details else '',
        'Workflow ID' if details else '',
        'Worker' if details else ''))

    click.echo('-' * (136 if details else 65))

    for event in workflow_events(ctx.obj['config']):
        evt_disp = event_display[event.event]
        click.echo('{:>{}} {:>{}} {:25} {:18} {:16} {:28} {}'.format(
            _style(show_colors, evt_disp['label'], fg=evt_disp['color']),
            20 if show_colors else 10,
            _style(show_colors, event.type, bold=True, fg=JOB_COLOR[event.type]),
            24 if show_colors else 12,
            event.name,
            '{0:.3f}'.format(event.duration) if event.duration is not None else '',
            event.queue if details else '',
            event.workflow_id if details else '',
            event.hostname if details else ''))


@cli.command('ext')
@click.argument('ext_name', nargs=1)
@click.argument('ext_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def ext(obj, ext_name, ext_args):
    """ Run an extension by its name.

    \b
    EXT_NAME: The name of the extension.
    EXT_ARGS: Arguments that are passed to the extension.
    """
    try:
        mod = import_module('lightflow_{}.__main__'.format(ext_name))
        mod.main(ext_args)
    except ImportError as err:
        click.echo(_style(obj['show_color'],
                          'An error occurred when trying to call the extension',
                          fg='red', bold=True))
        click.echo('{}'.format(err))


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
