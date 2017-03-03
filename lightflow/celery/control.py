from celery.result import AsyncResult


class BrokerStats:
    def __init__(self, hostname, port, transport, virtual_host):
        self.hostname = hostname
        self.port = port
        self.transport = transport
        self.virtual_host = virtual_host

    @classmethod
    def from_celery(cls, broker_dict):
        return BrokerStats(
            hostname=broker_dict['hostname'],
            port=broker_dict['port'],
            transport=broker_dict['transport'],
            virtual_host=broker_dict['virtual_host']
        )


class QueueStats:
    def __init__(self, name, routing_key):
        self.name = name
        self.routing_key = routing_key

    @classmethod
    def from_celery(cls, queue_dict):
        return QueueStats(
            name=queue_dict['name'],
            routing_key=queue_dict['routing_key']
        )


class WorkerStats:
    def __init__(self, name, broker, pid, process_pids,
                 concurrency, total_running, queues):

        #dict: A dictionary of all workers, with the unique worker name as key and
        #      the fields as follows:
        #      'broker': the broker the worker is using
        #        'transport': the transport protocol of the broker
        #        'hostname': the broker hostname
        #        'port': the broker port
        #        'virtual_host': the virtual host, e.g. the database number in redis.
        #        'proc': the worker process
        #        'pid': the PID of the worker
        #        'processes': the PIDs of the concurrent task processes

        self.name = name
        self.broker = broker
        self.pid = pid
        self.process_pids = process_pids
        self.concurrency = concurrency
        self.total_running = total_running
        self.queues = queues

    @classmethod
    def from_celery(cls, name, worker_dict, queues):
        return WorkerStats(
            name=name,
            broker=BrokerStats.from_celery(worker_dict['broker']),
            pid=worker_dict['pid'],
            process_pids=worker_dict['pool']['processes'],
            concurrency=worker_dict['pool']['max-concurrency'],
            total_running=worker_dict['pool']['writes']['total'],
            queues=queues
        )


class TaskStats:
    def __init__(self, name, task_id, task_type, workflow_id, acknowledged, func_name,
                 hostname, worker_name, worker_pid, routing_key):
        self.name = name
        self.id = task_id
        self.type = task_type
        self.workflow_id = workflow_id
        self.acknowledged = acknowledged
        self.func_name = func_name
        self.hostname = hostname
        self.worker_name = worker_name
        self.worker_pid = worker_pid
        self.routing_key = routing_key

    @classmethod
    def from_celery(cls, worker_name, task_dict, celery_app):
        async_result = AsyncResult(id=task_dict['id'], app=celery_app)
        a_info = async_result.info

        return TaskStats(
            name=a_info.get('name', '') if a_info is not None else '',
            task_id=task_dict['id'],
            task_type=a_info.get('type', '') if a_info is not None else '',
            workflow_id=a_info.get('workflow_id', '') if a_info is not None else '',
            acknowledged=task_dict['acknowledged'],
            func_name=task_dict['type'],
            hostname=task_dict['hostname'],
            worker_name=worker_name,
            worker_pid=task_dict['worker_pid'],
            routing_key=task_dict['delivery_info']['routing_key']
        )
