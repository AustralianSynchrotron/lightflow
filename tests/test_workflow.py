import sys
from pathlib import Path

import pytest  # noqa

from lightflow.models.workflow import Workflow
from lightflow.models.exceptions import WorkflowImportError, WorkflowArgumentError


@pytest.fixture(autouse=True)
def add_workflow_path():
    path = str(Path(__file__).parent / 'fixtures/workflows')
    sys.path.append(path)
    yield
    sys.path.remove(path)


def test_load_workflow_for_missing_name():
    with pytest.raises(WorkflowImportError):
        Workflow().load('invalid_name_workflow')


def test_load_workflow_with_no_dag():
    Workflow().load('no_dag_workflow', strict_dag=False)
    with pytest.raises(WorkflowImportError):
        Workflow().load('no_dag_workflow', strict_dag=True)


def test_load_workflow_with_dag():
    wf = Workflow()
    wf.load('dag_present_workflow', strict_dag=True)
    assert wf.name == 'dag_present_workflow'
    assert wf.docstring == 'The docstring'
    assert len(wf.parameters) == 0


def test_load_workflow_with_no_arguments():
    with pytest.raises(WorkflowArgumentError):
        Workflow().load('parameters_workflow')


def test_load_workflow_with_missing_arguments():
    with pytest.raises(WorkflowArgumentError):
        Workflow().load('parameters_workflow', arguments={})


def test_load_workflow_with_all_arguments():
    wf = Workflow()
    wf.load('parameters_workflow', arguments={'required_arg': 'ok'})
    assert wf.parameters[0].name == 'required_arg'


def test_load_workflow_when_validate_arguments_is_false():
    Workflow().load('parameters_workflow', validate_arguments=False)


def test_workflow_from_name_constructor():
    wf = Workflow.from_name('parameters_workflow', arguments={'required_arg': 'ok'})
    assert wf.parameters[0].name == 'required_arg'
