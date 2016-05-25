from time import sleep
import networkx as nx
from lightflow.celery_tasks import app, task_celery_task


class Dag:
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self._graph = nx.DiGraph()

    def add_task(self, task):
        self._graph.add_node(task)

    def add_edge(self, from_task, to_task):
        self._graph.add_edge(from_task, to_task)

    def run(self, workflow_id=None):

        # check whether the start graph is a directed acyclic graph
        if nx.is_directed_acyclic_graph(self._graph):
            running = []

            linearised_graph = nx.topological_sort(self._graph)
            for node in linearised_graph:
                if len(self._graph.predecessors(node)) == 0:
                    print(node.name)
                    running.append(node)

            while running:
                sleep(0.5)
                for t in reversed(running):
                    print(running)
                    if not t.is_queued:
                        t.celery_result = task_celery_task.delay(t)
                    else:
                        if t.celery_result.successful():
                            print(t.name)
                            for n in self._graph[t]:
                                if all([pn.celery_result.successful() for pn in self._graph.predecessors(n)]):
                                    running.append(n)

                            running.remove(t)
        else:
            # TODO: throw exception !!!!
            print("Oh no, loops!")
