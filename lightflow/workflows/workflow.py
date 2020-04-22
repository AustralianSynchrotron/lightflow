import sys
import inspect
import importlib
from uuid import uuid4

from lightflow.graphs import Graph


class Workflow:
    def __init__(self, name: str, *, workflow_id: str = None, doc: str = None):
        self._name = name
        self._workflow_id = workflow_id or uuid4()
        self._doc = doc

        self._graphs = {}

    @property
    def id(self) -> str:
        return self._workflow_id

    @property
    def doc(self) -> str:
        return self._doc

    @property
    def graphs(self):
        return list(self._graphs.keys())

    @classmethod
    def from_module(cls, name: str):
        new_workflow = cls(name)
        new_workflow.load(name)
        return new_workflow

    def load(self, name: str):
        try:
            workflow_module = importlib.import_module(name)
            self._doc = inspect.getdoc(workflow_module)

            is_graph_present = False
            for key, obj in workflow_module.__dict__.items():
                if isinstance(obj, Graph):
                    self._graphs[obj.name] = obj
                    is_graph_present = True

            del sys.modules[name]

        except (TypeError, ImportError):
            # TODO: raise an error here
            pass

    def run(self):
        pass
