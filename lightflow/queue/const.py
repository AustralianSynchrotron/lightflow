

class JobExecPath:
    Workflow = 'lightflow.queue.jobs.execute_workflow'
    Dag = 'lightflow.queue.jobs.execute_dag'
    Task = 'lightflow.queue.jobs.execute_task'


class JobStatus:
    Active = 0
    Registered = 1
    Reserved = 2


class JobType:
    Workflow = 'workflow'
    Dag = 'dag'
    Task = 'task'


class JobEventName:
    Started = 'task-lightflow-started'
    Succeeded = 'task-lightflow-succeeded'
    Stopped = 'task-lightflow-stopped'
    Aborted = 'task-lightflow-aborted'
