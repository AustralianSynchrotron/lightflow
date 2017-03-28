from time import sleep
from copy import deepcopy
from collections import defaultdict
import networkx as nx

from .signal import Request
from .task_data import MultiTaskData
from .exceptions import DirectedAcyclicGraphInvalid, ConfigNotDefinedError
from .const import JobType
from lightflow.logger import get_logger
from lightflow.queue.app import create_app


logger = get_logger(__name__)


class DagSignal:
    """ Class to wrap the construction and sending of signals into easy to use methods """
    def __init__(self, client, dag_name):
        """ Initialise the dag signal convenience class.

        Args:
            client (Client): A reference to a signal client object.
            dag_name (str): The name of the dag sending this signal.
        """
        self._client = client
        self._dag_name = dag_name

    @property
    def is_stopped(self):
        """ Check whether the dag received a stop signal from the workflow.

        As soon as the dag receives a stop signal, no new tasks will be queued
        and the dag will wait for the active tasks to terminate.

        Returns:
            bool: True if the dag should be stopped.
        """
        resp = self._client.send(
            Request(
                action='is_dag_stopped',
                payload={'dag_name': self._dag_name}
            )
        )
        return resp.payload['is_stopped']


class Dag:
    """ A dag hosts a graph built from tasks and manages the task execution process.

    One ore more tasks, that are connected with each other, form a graph of tasks. The
    connection between tasks has a direction, indicating the flow of data from task to
    task. Closed loops are not allowed. Therefore, the graph topology employed here is
    referred to as a dag (directed acyclic graph).

    The dag class not only provides tools to build a task graph, but also manages the
    processing of the tasks in the right order by traversing the graph using a
    breadth-first search strategy.

    Please note: this class has to be serialisable (e.g. by pickle)
    """
    def __init__(self, name, *, autostart=True):
        """ Initialise the dag.

        Args:
            name (str): The name of the dag.
            autostart (bool): Set to True in order to start the processing of the tasks
                              upon the start of the workflow.
        """
        self._name = name
        self._autostart = autostart

        self._config = None
        self._graph = nx.DiGraph()
        self._slots = defaultdict(dict)
        self._copy_counter = 0

    @property
    def name(self):
        """ Return the name of the dag. """
        return self._name

    @property
    def autostart(self):
        """ Return whether the dag is automatically run upon the start of the workflow."""
        return self._autostart

    @property
    def config(self):
        """ Returns the dag configuration. """
        return self._config

    @config.setter
    def config(self, value):
        """ Sets the dag configuration.

        Args:
            value (Config): A reference to a Config object.
        """
        self._config = value

    def define(self, schema):
        """ Constructs the task graph (dag) from a given schema.

        Parses the graph schema definition and creates the task graph. Tasks are the
        vertices of the graph and the connections defined in the schema become the edges.

        A key in the schema dict represents a parent task and the value one or more
        children:
            {parent: [child]} or {parent: [child1, child2]}

        The data output of one task can be routed to a labelled input slot of successor
        tasks using a dictionary instead of a list for the children:
            {parent: {child1: 'positive', child2: 'negative'}}

        An empty slot name or None skips the creation of a labelled slot:
            {parent: {child1: '', child2: None}}

        The underlying graph library creates nodes automatically, when an edge between
        non-existing nodes is created.

        Args:
            schema (dict): A dictionary with the schema definition.
        """
        self._graph.clear()
        for parent, children in schema.items():
            if children is not None:
                children = children if (isinstance(children, list) or
                                        isinstance(children, dict)) else [children]

            if children is not None and len(children) > 0:
                for child in children:
                    self._graph.add_edge(parent, child)
                    try:
                        slot = children[child]
                        if slot != '' and slot is not None:
                            self._slots[child][parent] = slot
                    except TypeError:
                        pass
            else:
                self._graph.add_node(parent)

    def run(self, workflow_id, signal, *, data=None):
        """ Run the dag by calling the tasks in the correct order.

        Args:
            workflow_id (str): The unique ID of the workflow that runs this dag.
            signal (DagSignal): The signal object for dags. It wraps the construction
                                and sending of signals into easy to use methods.
            data (MultiTaskData): The initial data that is passed on to the start tasks.

        Raises:
            DirectedAcyclicGraphInvalid: If the graph is not a dag (e.g. contains loops).
            ConfigNotDefinedError: If the configuration for the dag is empty.
        """
        if not nx.is_directed_acyclic_graph(self._graph):
            raise DirectedAcyclicGraphInvalid()

        if self._config is None:
            raise ConfigNotDefinedError()

        # create the celery app for submitting tasks
        celery_app = create_app(self._config)

        # add all tasks without predecessors to the initial task list and
        # set the dag_name for all tasks (which binds the task to this dag).
        tasks = []
        linearised_graph = nx.topological_sort(self._graph)
        for node in linearised_graph:
            node.dag_name = self.name
            if len(self._graph.predecessors(node)) == 0:
                tasks.append(node)

        # process tasks as long as there are tasks in the task list
        stopped = False
        while tasks:
            if self._config.dag_polling_time > 0.0:
                sleep(self._config.dag_polling_time)

            for task in reversed(tasks):
                if not stopped:
                    stopped = signal.is_stopped

                if not task.has_result:
                    task.config = self._config

                    # a task is in the task list but has never been queued or ran.
                    pre_tasks = self._graph.predecessors(task)
                    if len(pre_tasks) == 0:
                        # start a task without predecessors with the supplied initial data
                        if not stopped:
                            task.celery_result = celery_app.send_task(
                                'lightflow.queue.jobs.execute_task',
                                args=(task, workflow_id, data),
                                queue=JobType.Task,
                                routing_key=JobType.Task
                                )
                    else:
                        # compose the input data from the predecessor tasks output data
                        input_data = MultiTaskData()
                        for pre_task in pre_tasks:
                            if task in self._slots:
                                aliases = [self._slots[task][pre_task]]
                            else:
                                aliases = None

                            input_data.add_dataset(pre_task.name,
                                                   pre_task.celery_result.result.data,
                                                   aliases=aliases)

                        # start task with the aggregated data from its predecessors
                        if not stopped:
                            task.celery_result = celery_app.send_task(
                                'lightflow.queue.jobs.execute_task',
                                args=(task, workflow_id, input_data),
                                queue=JobType.Task,
                                routing_key=JobType.Task
                                )
                else:
                    # the task finished processing. Check whether its successor tasks can
                    # be added to the task list
                    if task.is_finished:
                        all_successors_queued = True
                        for next_task in self._graph.successors(task):

                            # check whether the task imposes a limit on its successors
                            if task.celery_result.result.limit is not None:
                                if next_task.name not in task.celery_result.result.limit:
                                    next_task.skip()

                            # consider queuing the successor task if it is not in the list
                            if next_task not in tasks and not next_task.has_result:
                                if all([pt.is_finished or pt.is_skipped
                                        for pt in self._graph.predecessors(next_task)]):

                                    # check whether the skip flag should be propagated
                                    if all([pt.is_skipped and pt.propagate_skip for pt in
                                            self._graph.predecessors(next_task)]):
                                        next_task.skip()

                                    if not stopped:
                                        tasks.append(next_task)
                                else:
                                    all_successors_queued = False

                        # if all the successor tasks are queued, the task has done
                        # its duty and can be removed from the task list
                        if all_successors_queued:
                            tasks.remove(task)

    def __deepcopy__(self, memo):
        """ Create a copy of the dag object.

        This method keeps track of the number of copies that have been made. The number is
        appended to the name of the copy.

        Args:
            memo (dict): a dictionary that keeps track of the objects that
                         have already been copied.

        Returns:
            Dag: a copy of the dag object
        """
        self._copy_counter += 1
        new_dag = Dag('{}:{}'.format(self._name, self._copy_counter),
                      autostart=self._autostart)

        new_dag._graph = deepcopy(self._graph, memo)
        new_dag._slots = deepcopy(self._slots, memo)
        return new_dag
