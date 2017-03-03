import click
import json

from lightflow.config import Config
from lightflow.version import __version__
from lightflow.models.const import TaskType
from lightflow.models.exceptions import WorkflowArgumentError, WorkflowImportError
from lightflow.workers import (start_worker, list_workers, list_tasks)
from lightflow.workflows import (start_workflow)

TASK_COLOR = {
    TaskType.Workflow: 'green', TaskType.Dag: 'yellow', TaskType.Task: 'magenta'
}


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version=__version__, prog_name='Lightflow')
@click.option('--config', '-c', help='Path to configuration file.')
@click.pass_context
def cli(ctx, config):
    """ Command line client for lightflow. A lightweight, high performance pipeline
    system for synchrotrons.

    Lightflow is being developed at the Australian Synchrotron.
    """
    ctx.obj = Config.from_file(config)


@cli.group()
def config():
    """ Manage the configuration """
    pass


@config.command('default')
def config_default():
    """ Print a default configuration """
    click.echo(Config.default())


@config.command('list')
@click.pass_obj
def config_list(conf_obj):
    """ List the current configuration """
    click.echo(json.dumps(conf_obj.to_dict(), indent=4))


@cli.group()
def workflow():
    """ Start, stop and list workflows """
    pass


@workflow.command('list')
def workflow_list():
    click.echo('workflow list command')


@workflow.command('start')
@click.option('--keep-data', '-k', is_flag=True, default=False,
              help='Do not delete the workflow data.')
@click.argument('name')
@click.argument('workflow_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def workflow_start(conf_obj, keep_data, name, workflow_args):
    """ Send a workflow to the queue.

    \b
    NAME: The name of the workflow that should be started.
    WORKFLOW_ARGS: Workflow arguments in the form key1=value1 key2=value2.
    """
    try:
        start_workflow(name=name,
                       config=conf_obj,
                       clear_data_store=not keep_data,
                       store_args=dict([arg.split('=', maxsplit=1)
                                        for arg in workflow_args]))
    except (WorkflowArgumentError, WorkflowImportError) as e:
        click.echo(click.style('An error occurred when trying to start the workflow',
                               fg='red', bold=True))
        click.echo('{}'.format(e))


@workflow.command('stop')
def workflow_stop():
    click.echo('workflow stop command')


@workflow.command('create')
def workflow_create():
    click.echo('workflow create command')


@workflow.command('validate')
def workflow_validate():
    click.echo('workflow validate command')


@workflow.command('status')
def workflow_status():
    click.echo('workflow status command')


@cli.group()
def worker():
    """ Start and stop workers. """
    pass


@worker.command('start', context_settings=dict(ignore_unknown_options=True,))
@click.option('--queues', '-q', default='workflow,dag,task',
              help='Comma separated list of queues to enable for this worker.')
@click.argument('celery_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def worker_start(conf_obj, queues, celery_args):
    """ Start a worker process. """
    start_worker(queues=queues.split(','),
                 config=conf_obj,
                 celery_args=celery_args)


@worker.command('stop')
def worker_stop():
    click.echo('worker stop command')


@worker.command('restart')
def worker_restart():
    click.echo('worker restart command')


@worker.command('status')
@click.option('--filter-queues', '-f', default=None,
              help='Only show workers for this comma separated list of queues.')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed worker information')
@click.pass_obj
def worker_status(conf_obj, filter_queues, verbose):
    """ Show the status of all running workers. """
    f_queues = filter_queues.split(',') if filter_queues is not None else None

    for ws in list_workers(config=conf_obj, filter_by_queues=f_queues):
        click.echo('{} {}'.format(click.style('Worker:', fg='blue', bold=True),
                                  click.style(ws.name, fg='blue')))
        click.echo('{:23} {}'.format(click.style('> pid:', bold=True), ws.pid))

        if verbose:
            click.echo('{:23} {}'.format(click.style('> concurrency:', bold=True),
                                         ws.concurrency))
            click.echo('{:23} {}'.format(click.style('> processes:', bold=True),
                                         ', '.join(str(p) for p in ws.process_pids)))
            click.echo('{:23} {}://{}:{}/{}'.format(click.style('> broker:', bold=True),
                                                    ws.broker.transport,
                                                    ws.broker.hostname,
                                                    ws.broker.port,
                                                    ws.broker.virtual_host))

        click.echo('{:23} {}'.format(click.style('> queues:', bold=True),
                                     ', '.join([q.name for q in ws.queues])))

        if verbose:
            click.echo('{:23} [{}]'.format(click.style('> tasks:', bold=True),
                                           ws.total_running))

            for task in list_tasks(config=conf_obj, filter_by_worker=ws.name):
                click.echo('{:15} {} {}'.format(
                    '',
                    click.style('{}'.format(task.name), bold=True,
                                fg=TASK_COLOR[task.type]),
                    click.style('({}) [{}] <{}> on {}'.format(
                        task.type, task.workflow_id, task.id, task.worker_pid),
                        fg=TASK_COLOR[task.type])))

        click.echo('\n')


@cli.command()
def status():
    click.echo('Status')


@cli.command()
def monitor():
    click.echo('Monitor')


if __name__ == '__main__':
    cli(obj={})
