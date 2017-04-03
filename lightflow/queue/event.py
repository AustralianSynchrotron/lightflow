import threading
from queue import Queue

from .models import JobStartedEvent, JobSucceededEvent
from lightflow.models.exceptions import (EventTypeUnknown, JobEventTypeUnsupported,
                                         WorkerEventTypeUnsupported)


def event_stream(app, *, filter_by_prefix=None):
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
    if event['type'].startswith('task'):
        factory = {
            'task-lightflow-started': JobStartedEvent,
            'task-lightflow-succeeded': JobSucceededEvent
        }
        if event['type'] in factory:
            return factory[event['type']].from_event(event)
        else:
            raise JobEventTypeUnsupported('Unsupported event type {}'.format(event['type']))
    elif event.type.startswith('worker'):
        raise WorkerEventTypeUnsupported('Unsupported event type {}'.format(event['type']))
    else:
        raise EventTypeUnknown('Unknown event type {}'.format(event['type']))
