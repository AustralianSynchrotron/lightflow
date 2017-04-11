

class JobExecPath:
    Workflow = 'lightflow.queue.jobs.execute_workflow'
    Dag = 'lightflow.queue.jobs.execute_dag'
    Task = 'lightflow.queue.jobs.execute_task'


class JobStatus:
    Active = 0
    Scheduled = 1


class JobType:
    Workflow = 'workflow'
    Dag = 'dag'
    Task = 'task'


class JobEventName:
    Started = 'task-lightflow-started'
    Succeeded = 'task-lightflow-succeeded'
