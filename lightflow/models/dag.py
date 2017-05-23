from time import sleep
from copy import deepcopy
import networkx as nx
from collections import defaultdict

from .task import BaseTask, BaseTaskStatus
from .task_data import MultiTaskData
from .exceptions import DirectedAcyclicGraphInvalid, ConfigNotDefinedError
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
    def __init__(self, name, *, autostart=True):
        """ Initialise the dag.

        Args:
            name (str): The name of the dag.
            autostart (bool): Set to True in order to start the processing of the tasks
                              upon the start of the workflow.
        """
        self._name = name
        self._autostart = autostart

        self._graph = nx.DiGraph()
        self._slots = defaultdict(dict)
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
        """ Returns the name of the workflow this dag belongs to. """
        return self._workflow_name

    @workflow_name.setter
    def workflow_name(self, name):
        """ Set the name of the workflow this dag belongs to.

        Args:
            name (str): The name of the workflow.
        """
        self._workflow_name = name

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
        # pre-checks
        if not nx.is_directed_acyclic_graph(self._graph):
            raise DirectedAcyclicGraphInvalid()

        if config is None:
            raise ConfigNotDefinedError()

        # create the celery app for submitting tasks
        celery_app = create_app(config)

        # the task queue for managing the current state of the tasks
        tasks = []
        stopped = False

        # add all tasks without predecessors to the task list and
        # set the dag_name for all tasks (which binds the task to this dag).
        linearised_graph = nx.topological_sort(self._graph)
        for node in linearised_graph:
            node.workflow_name = self.workflow_name
            node.dag_name = self.name
            if len(self._graph.predecessors(node)) == 0:
                node.set_state(BaseTaskStatus.Waiting)
                tasks.append(node)

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
                    if not stopped:
                        self._handle_waiting_task(task, data, celery_app, workflow_id)
                    else:
                        task.set_state(BaseTaskStatus.Stopped)

                # for each completed task, add all successor tasks to the task list
                # and flag them as waiting if they are not in the task list yet.
                elif task.is_running:
                    if task.celery_completed:
                        task.set_state(BaseTaskStatus.Completed)
                        for next_task in self._graph.successors(task):
                            if next_task not in tasks:
                                next_task.set_state(BaseTaskStatus.Waiting)
                                tasks.append(next_task)

                # cleanup task results that are not required anymore
                elif task.is_completed:
                    if all([s.is_completed or s.is_stopped or s.is_aborted
                            for s in self._graph.successors(task)]):
                        if celery_app.conf.result_expires == 0:
                            task.clear_celery_result()
                        tasks.remove(task)

                # cleanup and remove stopped and aborted tasks
                elif task.is_stopped or task.is_aborted:
                    if celery_app.conf.result_expires == 0:
                        task.clear_celery_result()
                    tasks.remove(task)

    def _handle_waiting_task(self, task, data, celery_app, workflow_id):
        """ Handle a waiting task.

        Wait for all predecessor tasks to be completed. Then check whether the task
        should be skipped by interrogating the predecessor tasks.
        
        Args:
            task (BaseTask): Reference to the task object that has a waiting state.
            data (MultiTaskData): Reference to the DAG's MultiTaskData object.
            celery_app (Celery): Reference to an celery application object. Used for
                                 queueing new tasks.
            workflow_id (str): The unique ID of the workflow that runs this dag.

        """
        pre_tasks = self._graph.predecessors(task)
        if all([p.is_completed or p.is_skipped for p in pre_tasks]):

            # check whether the task should be skipped
            run_task = task.has_to_run or len(pre_tasks) == 0
            for pre in pre_tasks:
                if run_task:
                    break

                # predecessor task is skipped and flag should not be propagated
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
                task.set_state(BaseTaskStatus.Completed)
            else:
                # compose the input data from the predecessor tasks output data
                # skipped predecessor tasks do not contribute to the input data
                if len(pre_tasks) == 0:
                    input_data = data
                else:
                    input_data = MultiTaskData()
                    for pre_task in [p for p in pre_tasks if
                                     not p.is_skipped]:
                        if task in self._slots:
                            aliases = [self._slots[task][pre_task]]
                        else:
                            aliases = None

                        input_data.add_dataset(pre_task.name,
                                               pre_task.celery_result.result.data,
                                               aliases=aliases)

                task.set_state(BaseTaskStatus.Running)
                task.celery_result = celery_app.send_task(
                    JobExecPath.Task,
                    args=(task, workflow_id, input_data),
                    queue=task.queue,
                    routing_key=task.queue
                )

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
