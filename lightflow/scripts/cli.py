import click
import json

import lightflow
from lightflow.config import config as lf_config
from lightflow.version import __version__
from lightflow.models.exceptions import WorkflowArgumentError





@click.group(invoke_without_command=True)
@click.pass_context
@click.option('--config', '-c', help='Path to configuration file.')
@click.option('--version', '-v', help='Prints the version number.', is_flag=True)
def cli(ctx, config, version):
    """ Command line client for lightflow. A lightweight, high performance pipeline
    system for synchrotrons.

    Lightflow is being developed at the Australian Synchrotron.
    """
    lf_config.load_from_file(config)

    if ctx.invoked_subcommand is None and version:
        click.echo('Lightflow {}'.format(__version__))
    elif ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


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


@workflow.command('info')
def workflow_info():
    click.echo('workflow info command')


@workflow.command('validate')
def workflow_validate():
    click.echo('workflow validate command')


@cli.group()
def worker():
    pass


@workflow.command('list')
def worker_list():
    click.echo('worker list command')


@workflow.command('run')
def worker_run():
    click.echo('worker run command')


@workflow.command('stop')
def worker_stop():
    click.echo('worker stop command')


@workflow.command('restart')
def worker_restart():
    click.echo('worker restart command')


if __name__ == '__main__':
    cli()
