from time import sleep
from collections import defaultdict
import networkx as nx
from lightflow.celery_tasks import app, task_celery_task
from lightflow.models.task_data import MultiTaskData


class Dag:
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self._graph = nx.DiGraph()
        self._slots = defaultdict(dict)

    def add_task(self, task):
        self._graph.add_node(task)

    def add_edge(self, from_task, to_task):
        self._graph.add_edge(from_task, to_task)

    def define_workflow(self, workflow):
        for parent, children in workflow.items():
            for child in children:
                self.add_edge(parent, child)
                try:
                    slot = children[child]
                    if slot != '':
                        self._slots[child][parent] = slot
                except TypeError:
                    pass

    def run(self, workflow_id=None):
        # check whether the start graph is a directed acyclic graph
        if nx.is_directed_acyclic_graph(self._graph):
            running = []

            linearised_graph = nx.topological_sort(self._graph)
            for node in linearised_graph:
                if len(self._graph.predecessors(node)) == 0:
                    running.append(node)

            while running:
                sleep(0.5)
                for t in reversed(running):
                    if not t.is_queued:
                        pt = self._graph.predecessors(t)
                        if len(pt) == 0:
                            t.celery_result = task_celery_task.delay(t)
                        else:
                            data_dict = MultiTaskData()
                            for p in pt:
                                if t in self._slots:
                                    alias = self._slots[t][p]
                                else:
                                    alias = None

                                data_dict.add_dataset(p.name, p.celery_result.result, aliases=[alias])

                            t.celery_result = task_celery_task.delay(t, data_dict)

                    else:
                        if t.is_finished:
                            for n in self._graph.successors(t):
                                if not n.is_queued and n not in running:
                                    if all([pn.is_finished for pn in self._graph.predecessors(n)]):
                                        running.append(n)

                            running.remove(t)
        else:
            # TODO: throw exception !!!!
            print("Oh no, loops!")
