import logging.config
from celery.signals import setup_logging
from celery.utils.log import get_task_logger

from .config import config


def get_logger(name):
    """ Helper function to return a  valid logger object

    Args:
        name (str): The name of the logger. Typically: __name__.

    Returns:
        Logger: A logger object for sending messages to the logging system
    """
    return get_task_logger(name)


@setup_logging.connect
def setup_logging(**kwargs):
    """ Hook into the logging system of celery.

    Connects the local logging system to the celery logging system such that both systems
    can coexist next to each other.

    Args:
        **kwargs: Keyword arguments from the hook
    """
    conf = config.get('logging')
    if conf is not None:
        logging.config.dictConfig(conf)
