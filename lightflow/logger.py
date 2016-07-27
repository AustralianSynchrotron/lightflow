import logging.config
from celery.signals import setup_logging
from celery.utils.log import get_task_logger

from .config import Config


def get_logger(name):
    return get_task_logger(name)


@setup_logging.connect
def setup_logging(**kwargs):
    conf = Config().get('logging')
    if conf is not None:
        logging.config.dictConfig(conf)
