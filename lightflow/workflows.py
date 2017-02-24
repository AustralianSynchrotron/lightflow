from .models import Workflow
from .celery.app import create_app


def run_workflow(name, config, *, clear_data_store=True, store_args=None):
    """ Run a single workflow by sending it to the workflow queue.

    Args:
        name (str): The name of the workflow that should be run.
        config (Config): Reference to the configuration object from which the
                         settings for the workflow are retrieved.
        clear_data_store (bool): Remove any documents created during the workflow
                                 run in the data store after the run.
        store_args (dict): Dictionary of additional arguments that are ingested into the
                           data store prior to the execution of the workflow.
    """
    wf = Workflow.from_name(name, config,
                            clear_data_store=clear_data_store,
                            arguments=store_args)

    celery = create_app(config)
    celery.send_task('lightflow.celery.tasks.execute_workflow',
                     args=(wf,),
                     queue='workflow',
                     routing_key='workflow'
                     )
