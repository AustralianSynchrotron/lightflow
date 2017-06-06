import threading
from queue import Queue

from .const import JobEventName
from .models import JobStartedEvent, JobSucceededEvent, JobStoppedEvent, JobAbortedEvent
from lightflow.models.exceptions import (EventTypeUnknown, JobEventTypeUnsupported,
                                         WorkerEventTypeUnsupported)


def event_stream(app, *, filter_by_prefix=None):
    """ Generator function that returns celery events.

    This function turns the callback based celery event handling into a generator.

    Args:
        app: Reference to a celery application object.
        filter_by_prefix (str): If not None, only allow events that have a type that
                                 starts with this prefix to yield an generator event.

    Returns:
        generator: A generator that returns celery events.

    """
    q = Queue()

    def handle_event(event):
        if filter_by_prefix is None or\
                (filter_by_prefix is not None and
                 event['type'].startswith(filter_by_prefix)):
            q.put(event)

    def receive_events():
        with app.connection() as connection:
            recv = app.events.Receiver(connection, handlers={
                '*': handle_event
            })

            recv.capture(limit=None, timeout=None, wakeup=True)

    t = threading.Thread(target=receive_events)
    t.start()

    while True:
        yield q.get(block=True)


def create_event_model(event):
    """ Factory function that turns a celery event into an event object.

    Args:
        event (dict): A dictionary that represents a celery event.

    Returns:
        object: An event object representing the received event.

    Raises:
        JobEventTypeUnsupported: If an unsupported celery job event was received.
        WorkerEventTypeUnsupported: If an unsupported celery worker event was received.
        EventTypeUnknown: If an unknown event type (neither job nor worker) was received.
    """
    if event['type'].startswith('task'):
        factory = {
            JobEventName.Started: JobStartedEvent,
            JobEventName.Succeeded: JobSucceededEvent,
            JobEventName.Stopped: JobStoppedEvent,
            JobEventName.Aborted: JobAbortedEvent
        }
        if event['type'] in factory:
            return factory[event['type']].from_event(event)
        else:
            raise JobEventTypeUnsupported(
                'Unsupported event type {}'.format(event['type']))
    elif event['type'].startswith('worker'):
        raise WorkerEventTypeUnsupported(
            'Unsupported event type {}'.format(event['type']))
    else:
        raise EventTypeUnknown('Unknown event type {}'.format(event['type']))
