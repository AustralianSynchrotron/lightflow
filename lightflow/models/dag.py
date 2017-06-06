from time import sleep
import networkx as nx
from copy import deepcopy

from .task import BaseTask, TaskState
from .task_data import MultiTaskData
from .exceptions import (DirectedAcyclicGraphInvalid, DirectedAcyclicGraphUndefined,
                         ConfigNotDefinedError)
from lightflow.logger import get_logger
from lightflow.queue.app import create_app
from lightflow.queue.const import JobExecPath


logger = get_logger(__name__)


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
    def __init__(self, name, *, autostart=True, schema=None):
        """ Initialize the dag.

        Args:
            name (str): The name of the dag.
            autostart (bool): Set to True in order to start the processing of the tasks
                              upon the start of the workflow.
            schema (dict): A dictionary with the definition of the task graph.
        """
        self._name = name
        self._autostart = autostart
        self._schema = schema

        self._copy_counter = 0
        self._workflow_name = None

    @property
    def name(self):
        """ Return the name of the dag. """
        return self._name

    @property
    def autostart(self):
        """ Return whether the dag is automatically run upon the start of the workflow."""
        return self._autostart

    @property
    def workflow_name(self):
        """ Return the name of the workflow this dag belongs to. """
        return self._workflow_name

    @workflow_name.setter
    def workflow_name(self, name):
        """ Set the name of the workflow this dag belongs to.

        Args:
            name (str): The name of the workflow.
        """
        self._workflow_name = name

    def define(self, schema, *, validate=True):
        """ Store the task graph definition (schema).

        The schema has to adhere to the following rules:

        A key in the schema dict represents a parent task and the value one or more
        children:
            {parent: [child]} or {parent: [child1, child2]}

        The data output of one task can be routed to a labelled input slot of successor
        tasks using a dictionary instead of a list for the children:
            {parent: {child1: 'positive', child2: 'negative'}}

        An empty slot name or None skips the creation of a labelled slot:
            {parent: {child1: '', child2: None}}

        Args:
            schema (dict): A dictionary with the schema definition.
            validate (bool): Set to True to validate the graph by checking whether it is
                             a directed acyclic graph.
        """
        self._schema = schema
        if validate:
            self.validate(self.make_graph(self._schema))

    def run(self, config, workflow_id, signal, *, data=None):
        """ Run the dag by calling the tasks in the correct order.

        Args:
            config (Config): Reference to the configuration object from which the
                             settings for the dag are retrieved.
            workflow_id (str): The unique ID of the workflow that runs this dag.
            signal (DagSignal): The signal object for dags. It wraps the construction
                                and sending of signals into easy to use methods.
            data (MultiTaskData): The initial data that is passed on to the start tasks.

        Raises:
            DirectedAcyclicGraphInvalid: If the graph is not a dag (e.g. contains loops).
            ConfigNotDefinedError: If the configuration for the dag is empty.
        """
        graph = self.make_graph(self._schema)

        # pre-checks
        self.validate(graph)

        if config is None:
            raise ConfigNotDefinedError()

        # create the celery app for submitting tasks
        celery_app = create_app(config)

        # the task queue for managing the current state of the tasks
        tasks = []
        stopped = False

        # add all tasks without predecessors to the task list
        for task in nx.topological_sort(graph):
            task.workflow_name = self.workflow_name
            task.dag_name = self.name
            if len(graph.predecessors(task)) == 0:
                task.state = TaskState.Waiting
                tasks.append(task)

        def set_task_completed(completed_task):
            """ For each completed task, add all successor tasks to the task list.
            If they are not in the task list yet, flag them as 'waiting'.
            """
            completed_task.state = TaskState.Completed
            for successor in graph.successors(completed_task):
                if successor not in tasks:
                    successor.state = TaskState.Waiting
                    tasks.append(successor)

        # process the task queue as long as there are tasks in it
        while tasks:
            if not stopped:
                stopped = signal.is_stopped

            # delay the execution by the polling time
            if config.dag_polling_time > 0.0:
                sleep(config.dag_polling_time)

            for i in range(len(tasks) - 1, -1, -1):
                task = tasks[i]

                # for each waiting task, wait for all predecessor tasks to be
                # completed. Then check whether the task should be skipped by
                # interrogating the predecessor tasks.
                if task.is_waiting:
                    if stopped:
                        task.state = TaskState.Stopped
                    else:
                        pre_tasks = graph.predecessors(task)
                        if all([p.is_completed for p in pre_tasks]):

                            # check whether the task should be skipped
                            run_task = task.has_to_run or len(pre_tasks) == 0
                            for pre in pre_tasks:
                                if run_task:
                                    break

                                # predecessor task is skipped and flag should
                                # not be propagated
                                if pre.is_skipped and not pre.propagate_skip:
                                    run_task = True

                                # limits of a non-skipped predecessor task
                                if not pre.is_skipped:
                                    if pre.celery_result.result.limit is not None:
                                        if task.name in [
                                            n.name if isinstance(n, BaseTask) else n
                                                for n in pre.celery_result.result.limit]:
                                            run_task = True
                                    else:
                                        run_task = True

                            task.is_skipped = not run_task

                            # send the task to celery or, if skipped, mark it as completed
                            if task.is_skipped:
                                set_task_completed(task)
                            else:
                                # compose the input data from the predecessor tasks
                                # output. Data from skipped predecessor tasks do not
                                # contribute to the input data
                                if len(pre_tasks) == 0:
                                    input_data = data
                                else:
                                    input_data = MultiTaskData()
                                    for pt in [p for p in pre_tasks if not p.is_skipped]:
                                        slot = graph[pt][task]['slot']
                                        input_data.add_dataset(
                                            pt.name,
                                            pt.celery_result.result.data.default_dataset,
                                            aliases=[slot] if slot is not None else None)

                                task.state = TaskState.Running
                                task.celery_result = celery_app.send_task(
                                    JobExecPath.Task,
                                    args=(task, workflow_id, input_data),
                                    queue=task.queue,
                                    routing_key=task.queue
                                )

                # flag task as completed
                elif task.is_running:
                    if task.celery_completed:
                        set_task_completed(task)
                    elif task.celery_failed:
                        task.state = TaskState.Aborted
                        signal.stop_workflow()

                # cleanup task results that are not required anymore
                elif task.is_completed:
                    if all([s.is_completed or s.is_stopped or s.is_aborted
                            for s in graph.successors(task)]):
                        if celery_app.conf.result_expires == 0:
                            task.clear_celery_result()
                        tasks.remove(task)

                # cleanup and remove stopped and aborted tasks
                elif task.is_stopped or task.is_aborted:
                    if celery_app.conf.result_expires == 0:
                        task.clear_celery_result()
                    tasks.remove(task)

    @staticmethod
    def validate(graph):
        """ Validate the graph by checking whether it is a directed acyclic graph.

        Args:
            graph (DiGraph): Reference to a DiGraph object from NetworkX.

        Raises:
            DirectedAcyclicGraphInvalid: If the graph is not a valid dag.
        """
        if not nx.is_directed_acyclic_graph(graph):
            raise DirectedAcyclicGraphInvalid()

    @staticmethod
    def make_graph(schema):
        """ Construct the task graph (dag) from a given schema.

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

        Returns:
            DiGraph: A reference to the fully constructed graph object.

        Raises:
            DirectedAcyclicGraphUndefined: If the schema is not defined.
        """
        if schema is None:
            raise DirectedAcyclicGraphUndefined()

        # sanitize the input schema such that it follows the structure:
        #    {parent: {child_1: slot_1, child_2: slot_2, ...}, ...}
        sanitized_schema = {}
        for parent, children in schema.items():
            child_dict = {}
            if children is not None:
                if isinstance(children, list):
                    if len(children) > 0:
                        child_dict = {child: None for child in children}
                    else:
                        child_dict = {None: None}
                elif isinstance(children, dict):
                    for child, slot in children.items():
                        child_dict[child] = slot if slot != '' else None
                else:
                    child_dict = {children: None}
            else:
                child_dict = {None: None}

            sanitized_schema[parent] = child_dict

        # build the graph from the sanitized schema
        graph = nx.DiGraph()
        for parent, children in sanitized_schema.items():
            for child, slot in children.items():
                if child is not None:
                    graph.add_edge(parent, child, slot=slot)
                else:
                    graph.add_node(parent)

        return graph

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
        new_dag._schema = deepcopy(self._schema, memo)
        return new_dag
