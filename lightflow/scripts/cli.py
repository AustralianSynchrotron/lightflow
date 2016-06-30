import click
import lightflow


@click.group()
def cli():
    """ Command line client for lightflow, a lightweight, high performance pipeline
    system for synchrotrons.

    Lightflow is being developed at the Australian Synchrotron.
    """
    pass


@click.command()
@click.argument('names', nargs=-1)
def run(names):
    """ Run one or more workflows.

    NAMES: A list of workflow names that should be run.
    """
    if len(names) == 0:
        click.echo('Please specify at least one workflow')
        return

    for name in names:
        lightflow.run_workflow(name)


@click.command()
def worker():
    """ Start a worker process. """
    lightflow.run_worker()


cli.add_command(run, 'run')
cli.add_command(worker, 'worker')

if __name__ == '__main__':
    cli()
