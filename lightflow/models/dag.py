import collections
import networkx as nx


class Dag:
    def __init__(self, dag_id, params=None):
        self._dag_id = dag_id
        self._params = {} if params is None else params

        self._graph = nx.DiGraph()
        self._task_map = collections.OrderedDict()

    def add_task(self, task):
        if task.name in self._task_map:
            # TODO: raise exception !!!!
            print('PANIC')

        task_id = len(self._task_map)
        self._graph.add_node(task_id)
        self._task_map[task.name] = task
        return task_id

    def get_task_by_id(self, task_id):
        # TODO: range check and throw exception !!!!
        return list(self._task_map.values())[task_id]  # TODO: ugly

    def get_task_by_name(self, task_name):
        # TODO: check whether name exists and throw exception !!!!
        return self._task_map[task_name]

    def add_edge(self, from_task, to_task):
        if (len(self._graph.predecessors(to_task.id)) < to_task.max_inputs) or\
                (to_task.max_inputs < 0):
            self._graph.add_edge(from_task.id, to_task.id)
        else:
            # TODO: throw exception !!!!
            print('max inputs violated!!!')

    def run(self, start_task=None):
        if len(self._task_map) == 0:
            # TODO: throw exception
            pass

        # check whether the start graph is a directed acyclic graph
        if nx.is_directed_acyclic_graph(self._graph):
            if start_task is None:
                start_task = self.get_task_by_id(0)

            for n in self._graph:
                self.get_task_by_id(n).reset()

            running = [start_task]
            while running:
                for t in reversed(running):
                    if t.is_finished():
                        running.remove(t)

                        # if the task limits the successor tasks run with those only
                        # TODO: check for loops !!!
                        if t.limit_successors() is not None:
                            # TODO: check whether the tasks are really successors
                            running.extend(t.limit_successors())
                        else:
                            for n in self._graph[t.id]:
                                running.append(self.get_task_by_id(n))

                    # check whether the task can run and all its predecessor requirements are fulfilled
                    if t.can_run() and t.check_predecessors(self._graph.predecessors(t.id)):
                        t.run() # TODO: send this call off to an executor class

        else:
            # TODO: throw exception !!!!
            print("Oh no, loops!")
