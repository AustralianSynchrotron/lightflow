import os
import glob

from .models import Workflow
from .models.signal import Client, Request, SignalConnection
from .models.exceptions import (WorkflowImportError,
                                JobEventTypeUnsupported, JobStatInvalid)

from .queue.app import create_app
from .queue.models import JobStats
from .queue.event import event_stream, create_event_model
from .queue.const import JobExecPath, JobStatus, JobType


def start_workflow(name, config, *, clear_data_store=True, store_args=None):
    """ Start a single workflow by sending it to the workflow queue.

    Args:
        name (str): The name of the workflow that should be started. Refers to the
                    name of the workflow file without the .py extension.
        config (Config): Reference to the configuration object from which the
                         settings for the workflow are retrieved.
        clear_data_store (bool): Remove any documents created during the workflow
                                 run in the data store after the run.
        store_args (dict): Dictionary of additional arguments that are ingested into the
                           data store prior to the execution of the workflow.
    Returns:
        str: The ID of the workflow job.
    Raises:
        WorkflowArgumentError: If the workflow requires arguments to be set in store_args
                               that were not supplied to the workflow.
        WorkflowImportError: If the import of the workflow fails.
    """
    wf = Workflow.from_name(name,
                            clear_data_store=clear_data_store,
                            arguments=store_args)

    celery_app = create_app(config)
    result = celery_app.send_task(JobExecPath.Workflow,
                                  args=(wf,),
                                  queue=JobType.Workflow,
                                  routing_key=JobType.Workflow
                                  )
    return result.id


def stop_workflow(config, *, names=None):
    """ Stop one or more workflows.

    Args:
        config (Config): Reference to the configuration object from which the
                         settings for the workflow are retrieved.
        names (list): List of workflow names, workflow ids or workflow job ids for the
                      workflows that should be stopped. If all workflows should be
                      stopped, set it to None.

    Returns:
        tuple: A tuple of the workflow jobs that were successfully stopped and the ones
               that could not be stopped.
    """
    jobs = list_jobs(config, filter_by_type=JobType.Workflow)

    if names is not None:
        filtered_jobs = []
        for job in jobs:
            if (job.id in names) or (job.name in names) or (job.workflow_id in names):
                filtered_jobs.append(job)
    else:
        filtered_jobs = jobs

    success = []
    failed = []
    for job in filtered_jobs:
        client = Client(SignalConnection(**config.signal, auto_connect=True),
                        request_key=job.workflow_id)

        if client.send(Request(action='stop_workflow')).success:
            success.append(job)
        else:
            failed.append(job)

    return success, failed


def list_workflows(config):
    """ List all available workflows.

    Returns a list of all workflows that are available from the paths specified
    in the config. A workflow is defined as a Python file with at least one DAG.

    Args:
        config (Config): Reference to the configuration object from which the
                         settings are retrieved.

    Returns:
        list: A list of WorkflowStats.
    """
    workflows = []
    for path in config.workflows:
        filenames = glob.glob(os.path.join(os.path.abspath(path), '*.py'))

        for filename in filenames:
            module_name = os.path.splitext(os.path.basename(filename))[0]
            workflow = Workflow()
            try:
                workflow.load(module_name, validate_arguments=False, strict_dag=True)
                workflows.append(workflow)
            except WorkflowImportError:
                continue

    return workflows


def list_jobs(config, *, status=JobStatus.Active,
              filter_by_type=None, filter_by_worker=None):
    """ Return a list of Celery jobs.

    filter_by_worker improves performance

    Args:
        config (Config): Reference to the configuration object from which the
                         settings are retrieved.
        status (JobStatus): The status of the jobs that should be returned.
        filter_by_type (list): Restrict the returned jobs to the types in this list.
        filter_by_worker (list): Only return jobs that were registered, reserved or are
                                 running on the workers given in this
                                 list of worker names.

    Returns:
        list: A list of JobStats.
    """
    celery_app = create_app(config)

    # option to filter by the worker (improves performance)
    if filter_by_worker is not None:
        inspect = celery_app.control.inspect(destination=[filter_by_worker])
    else:
        inspect = celery_app.control.inspect()

    # get active, registered or reserved jobs
    if status == JobStatus.Active:
        job_map = inspect.active()
    elif status == JobStatus.Registered:
        job_map = inspect.registered()
    elif status == JobStatus.Reserved:
        job_map = inspect.reserved()
    else:
        job_map = None

    if job_map is None:
        return []

    result = []
    for worker_name, jobs in job_map.items():
        for job in jobs:
            try:
                job_stats = JobStats.from_celery(worker_name, job, celery_app)

                if (filter_by_type is None) or (job_stats.type == filter_by_type):
                    result.append(job_stats)
            except JobStatInvalid:
                pass

    return result


def events(config):
    """ Return a generator that yields workflow events.

    For every workflow event that is sent from celery this generator yields an event
    object.

    Args:
        config (Config): Reference to the configuration object from which the
                         settings are retrieved.

    Returns:
        generator: A generator that returns workflow events.

    """
    celery_app = create_app(config)

    for event in event_stream(celery_app, filter_by_prefix='task'):
        try:
            yield create_event_model(event)
        except JobEventTypeUnsupported:
            pass
