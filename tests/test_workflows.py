from pathlib import Path

from lightflow.workflows import list_workflows
from lightflow.config import Config


def test_list_workflows_when_no_workflow_dirs_in_config():
    config = Config()
    config.load_from_dict({'workflows': []})
    assert list_workflows(config) == []


def test_list_workflows_handles_missing_parameters():
    config = Config()
    workflows_path = str(Path(__file__).parent / 'fixtures/workflows')
    config.load_from_dict({'workflows': [workflows_path]})
    assert 'parameters_workflow' in {wf.name for wf in list_workflows(config)}
