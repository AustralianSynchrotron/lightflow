import pytest
from lightflow import Config
from lightflow import workflows

config = Config()
config.load_from_file()


class TestWorkflows:

    def test_workflow_list(self):
        wfs = workflows.list_workflows(config)
        assert {wf.name for wf in wfs} == {'arguments', 'branching', 'chunking_dag',
                                           'data_store', 'simple', 'slots', 'sub_dag'}
