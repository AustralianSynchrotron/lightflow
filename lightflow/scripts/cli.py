import click
import lightflow


@click.group()
def cli():
    """ Command line client for lightflow. A lightweight, high performance pipeline
    system for synchrotrons.

    Lightflow is being developed at the Australian Synchrotron.
    """
    pass


@click.command()
def info():
    """ Show information about the. """
    workers = lightflow.get_workers()

    click.echo('\n')
    for name, worker_data in workers.items():
        broker = worker_data['broker']
        proc = worker_data['proc']

        click.echo('{} {}'.format(click.style('Worker:', fg='blue', bold=True),
                                  click.style(name, fg='blue')))
        click.echo('{:20} {}'.format(click.style('> pid:', bold=True), proc['pid']))
        click.echo('{:20} {}'.format(click.style('> processes:', bold=True),
                                     ', '.join(str(p) for p in proc['processes'])))
        click.echo('{:20} {}://{}:{}/{}'.format(click.style('> broker:', bold=True),
                                                broker['transport'],
                                                broker['hostname'],
                                                broker['port'],
                                                broker['virtual_host']))

        click.echo('{:20} {}'.format(click.style('> queues:', bold=True),
                                     ', '.join(lightflow.get_queues(name))))

        for task_status in ['active', 'scheduled']:
            task_colour = {
                'active': {
                    'workflow': 'green',
                    'dag': 'yellow',
                    'task': 'magenta'
                },
                'scheduled': {
                    'workflow': None,
                    'dag': None,
                    'task': None
                }
            }

            for i, task in enumerate(lightflow.get_tasks(name, task_status)):
                wid_str = '[{}]'.format(task['workflow_id'])\
                    if task['type'] == 'workflow' else ''

                if i == 0:
                    click.echo('{:20} {}'.format(
                        click.style('> {}:'.format(task_status), bold=True),
                        click.style('{} ({}) {} <{}>'.format(
                            task['name'], task['type'], wid_str, task['id']),
                            fg=task_colour[task_status][task['type']])))
                else:
                    click.echo('{:12} {}'.format(
                        ' ',
                        click.style('{} ({}) {} <{}>'.format(
                            task['name'], task['type'], wid_str, task['id']),
                            fg=task_colour[task_status][task['type']])))

        click.echo('\n')


@click.command()
@click.option('--keep-data', '-k', is_flag=True, default=False,
              help='Do not delete the workflow data.')
@click.argument('names', nargs=-1)
def run(keep_data, names):
    """ Run one or more workflows.

    NAMES: A list of workflow names that should be run.
    """
    if len(names) == 0:
        click.echo('Please specify at least one workflow')
        return

    for name in names:
        lightflow.run_workflow(name, not keep_data)


@click.command()
@click.argument('ids', nargs=-1)
def stop(ids):
    """ Stop one or more running dags or workflows.

    IDS: A list of workflow/dag task ids or workflow ids. Use 'all' to stop all running workflows.
    """
    if len(ids) == 0:
        click.echo('Please specify at least one dag or workflow')
        return

    # first try workflows and if there are ids left, try the dags
    tasks_stopped = lightflow.stop_workflows(ids)
    if len(tasks_stopped) < len(ids):
        tasks_stopped.extend(lightflow.stop_dags(ids))

    if len(tasks_stopped) > 0:
        for task in tasks_stopped:
            click.echo('Sent stop signal to {}: {} <{}>'.format(task['type'],
                                                                task['name'],
                                                                task['id']))
    else:
        click.echo('Could not find the specified dag or workflow')


@click.command()
@click.option('--queues', '-q', default='workflow,dag,task',
              help='Comma separated list of queues to enable for this worker.')
def worker(queues):
    """ Start a worker process. """
    lightflow.run_worker(queues.split(','))


cli.add_command(info, 'info')
cli.add_command(run, 'run')
cli.add_command(stop, 'stop')
cli.add_command(worker, 'worker')

if __name__ == '__main__':
    cli()
