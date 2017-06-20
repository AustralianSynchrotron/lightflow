from unittest.mock import create_autospec

import pytest

from lightflow.models.task_data import MultiTaskData
from lightflow.models.datastore import DataStoreDocument
from lightflow.models.task_signal import TaskSignal
from lightflow.models.task_context import TaskContext


@pytest.fixture
def data_mock():
    yield create_autospec(MultiTaskData, instance=True)


@pytest.fixture
def store_mock():
    yield create_autospec(DataStoreDocument, instance=True)


@pytest.fixture
def signal_mock():
    m = create_autospec(TaskSignal, instance=True)
    m.configure_mock(is_stopped=False)
    yield m


@pytest.fixture
def context_mock():
    yield create_autospec(TaskContext, instance=True)
