import click
import json

from lightflow.config import Config
from lightflow.version import __version__
from lightflow.workers import (run_worker)
from lightflow.workflows import (run_workflow)


@click.group()
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


@workflow.command('run')
@click.option('--keep-data', '-k', is_flag=True, default=False,
              help='Do not delete the workflow data.')
@click.argument('name')
@click.argument('workflow_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def workflow_run(conf_obj, keep_data, name, workflow_args):
    """ Send a workflow to the queue. """
    run_workflow(name=name,
                 config=conf_obj,
                 clear_data_store=not keep_data,
                 store_args=dict([arg.split('=', maxsplit=1) for arg in workflow_args]))


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


@worker.command('run', context_settings=dict(ignore_unknown_options=True,))
@click.option('--queues', '-q', default='workflow,dag,task',
              help='Comma separated list of queues to enable for this worker.')
@click.option('--detach', '-d', is_flag=True,
              help='Start worker as a background process.')
@click.argument('celery_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def worker_run(conf_obj, queues, detach, celery_args):
    """ Start a worker process. """
    run_worker(queues=queues.split(','),
               config=conf_obj,
               detach=detach,
               celery_args=celery_args)


@worker.command('stop')
def worker_stop():
    click.echo('worker stop command')


@worker.command('restart')
def worker_restart():
    click.echo('worker restart command')


@worker.command('status')
def worker_status():
    click.echo('worker status command')


@cli.command()
def status():
    click.echo('Status')


@cli.command()
def monitor():
    click.echo('Monitor')


if __name__ == '__main__':
    cli(obj={})
