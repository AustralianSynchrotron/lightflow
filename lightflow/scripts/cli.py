import click
import json

import lightflow
from lightflow.config import config as lf_config
from lightflow.version import __version__
from lightflow.models.exceptions import WorkflowArgumentError





@click.group()
@click.version_option(version=__version__, prog_name='Lightflow')
@click.option('--config', '-c', help='Path to configuration file.')
def cli(config):
    """ Command line client for lightflow. A lightweight, high performance pipeline
    system for synchrotrons.

    Lightflow is being developed at the Australian Synchrotron.
    """
    lf_config.load_from_file(config)


@cli.group()
def config():
    """ Manage the configuration """
    pass


@config.command('default')
def config_default():
    """ Print a default configuration """
    click.echo(lf_config.default())


@config.command('list')
def config_list():
    """ List the current configuration """
    click.echo(json.dumps(lf_config.to_dict(), indent=4))


@cli.group()
def workflow():
    """ Start, stop and list workflows """
    pass


@workflow.command('list')
def workflow_list():
    click.echo('workflow list command')


@workflow.command('run')
def workflow_run():
    click.echo('workflow run command')


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
    pass


@workflow.command('run')
def worker_run():
    click.echo('worker run command')


@workflow.command('stop')
def worker_stop():
    click.echo('worker stop command')


@workflow.command('restart')
def worker_restart():
    click.echo('worker restart command')


@workflow.command('status')
def worker_status():
    click.echo('worker status command')


@cli.command()
def status():
    click.echo('Status')


@cli.command()
def monitor():
    click.echo('Monitor')


if __name__ == '__main__':
    cli()
