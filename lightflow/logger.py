from celery.utils.log import get_task_logger


def get_logger(name):
    """ Helper function to return a  valid logger object

    Args:
        name (str): The name of the logger. Typically: __name__.

    Returns:
        Logger: A logger object for sending messages to the logging system
    """
    return get_task_logger(name)
