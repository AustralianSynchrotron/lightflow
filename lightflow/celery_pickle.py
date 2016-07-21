"""Patch Celery to use cloudpickle instead of pickle.

This file is based on the file '_patch_celery.py' of the cesium project.
Copyright (C) 2016, the cesium team.

The project can be found at: https://github.com/cesium-ml/cesium
"""
import cloudpickle
import kombu.serialization as serialization
from io import BytesIO


def cloudpickle_loads(s, load=cloudpickle.load):
    """ Decode the byte stream into Python objects using cloudpickle. """
    return load(BytesIO(s))


def cloudpickle_dumps(obj, dumper=cloudpickle.dumps):
    """ Encode Python objects into a byte stream using cloudpickle. """
    return dumper(obj, protocol=serialization.pickle_protocol)


def patch_celery():
    """ Monkey patch Celery to use cloudpickle instead of pickle. """
    registry = serialization.registry
    serialization.pickle = cloudpickle
    registry.unregister('pickle')
    registry.register('pickle', cloudpickle_dumps, cloudpickle_loads,
                      content_type='application/x-python-serialize',
                      content_encoding='binary')

    import celery.worker as worker
    import celery.concurrency.asynpool as asynpool
    worker.state.pickle = cloudpickle
    asynpool._pickle = cloudpickle

    import billiard.common
    billiard.common.pickle = cloudpickle
    billiard.common.pickle_dumps = cloudpickle_dumps
    billiard.common.pickle_loads = cloudpickle_loads
