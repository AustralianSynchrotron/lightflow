import logging.config
from celery.utils.log import get_task_logger
from celery.signals import setup_logging

from .config import Config


def get_logger(name):
    return get_task_logger(name)


@setup_logging.connect
def setup(**kwargs):
    conf = Config().get('logging')
    if conf is not None:
        logging.config.dictConfig(conf)
