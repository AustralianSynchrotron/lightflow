from time import sleep
from collections import defaultdict
import networkx as nx
from lightflow.celery_tasks import task_celery_task
from lightflow.models.task_data import MultiTaskData


class Dag:
    def __init__(self, name, autostart=True):
        self._name = name
        self._autostart = autostart
        self._graph = nx.DiGraph()
        self._slots = defaultdict(dict)

    @property
    def name(self):
        return self._name

    @property
    def autostart(self):
        return self._autostart

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

    def run(self, workflow_id):
        # TODO: any kind of exception handling of failed tasks
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
                            t.celery_result = task_celery_task.delay(t, workflow_id)
                        else:
                            data_dict = MultiTaskData()
                            for p in pt:
                                if t in self._slots:
                                    alias = self._slots[t][p]
                                else:
                                    alias = None

                                data_dict.add_dataset(p.name, p.celery_result.result.data,
                                                      aliases=[alias])

                            t.celery_result = task_celery_task.delay(t,
                                                                     workflow_id,
                                                                     data_dict)

                    else:
                        if t.is_finished:
                            all_successors_queued = True

                            for n in self._graph.successors(t):
                                if t.celery_result.result.selected_tasks is not None:
                                    if n.name not in t.celery_result.result.selected_tasks:
                                        n.skip()

                                if not n.is_queued and n not in running:
                                    if all([pn.is_finished or pn.is_skipped for pn in self._graph.predecessors(n)]):
                                        if all([pn.is_skipped for pn in self._graph.predecessors(n)]):
                                            n.skip()
                                        running.append(n)
                                    else:
                                        all_successors_queued = False

                            if all_successors_queued:
                                running.remove(t)
        else:
            # TODO: throw exception !!!!
            print("Oh no, loops!")
