from celery.result import AsyncResult
from time import sleep

from .const import JobExecPath, JobType


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
                 concurrency, job_count, queues):

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
        self.job_count = job_count
        self.queues = queues

    @classmethod
    def from_celery(cls, name, worker_dict, queues):
        return WorkerStats(
            name=name,
            broker=BrokerStats.from_celery(worker_dict['broker']),
            pid=worker_dict['pid'],
            process_pids=worker_dict['pool']['processes'],
            concurrency=worker_dict['pool']['max-concurrency'],
            job_count=worker_dict['pool']['writes']['total'],
            queues=queues
        )


class JobStats:
    def __init__(self, name, job_id, job_type, workflow_id, acknowledged, func_name,
                 hostname, worker_name, worker_pid, routing_key):
        self.name = name
        self.id = job_id
        self.type = job_type
        self.workflow_id = workflow_id
        self.acknowledged = acknowledged
        self.func_name = func_name
        self.hostname = hostname
        self.worker_name = worker_name
        self.worker_pid = worker_pid
        self.routing_key = routing_key

    @classmethod
    def from_celery(cls, worker_name, job_dict, celery_app):
        async_result = AsyncResult(id=job_dict['id'], app=celery_app)
        a_info = async_result.info

        return JobStats(
            name=a_info.get('name', '') if a_info is not None else '',
            job_id=job_dict['id'],
            job_type=a_info.get('type', '') if a_info is not None else '',
            workflow_id=a_info.get('workflow_id', '') if a_info is not None else '',
            acknowledged=job_dict['acknowledged'],
            func_name=job_dict['type'],
            hostname=job_dict['hostname'],
            worker_name=worker_name,
            worker_pid=job_dict['worker_pid'],
            routing_key=job_dict['delivery_info']['routing_key']
        )


class JobEvent:
    def __init__(self, uuid, job_type, event_type, *, label=''):
        self.label = label
        self.uuid = uuid
        self.type = job_type
        self.event = event_type


class JobStartedEvent(JobEvent):
    def __init__(self, uuid, job_type, event_type, hostname, pid, name, workflow_id, start_time):
        super().__init__(uuid, job_type, event_type, label='started')
        self.hostname = hostname
        self.pid = pid
        self.name = name
        self.workflow_id = workflow_id
        self.start_time = start_time

    @classmethod
    def from_event(cls, event):
        return cls(
            uuid=event['uuid'],
            job_type=event['job_type'],
            event_type=event['type'],
            hostname=event['hostname'],
            pid=event['pid'],
            name=event['name'],
            workflow_id=event['workflow_id'],
            start_time=event['start_time']
        )


class JobSucceededEvent(JobEvent):
    def __init__(self, uuid, job_type, event_type, hostname, pid, name, workflow_id, end_time):
        super().__init__(uuid, job_type, event_type, label='succeeded')
        self.hostname = hostname
        self.pid = pid
        self.name = name
        self.workflow_id = workflow_id
        self.end_time = end_time

    @classmethod
    def from_event(cls, event):
        return cls(
            uuid=event['uuid'],
            job_type=event['job_type'],
            event_type=event['type'],
            hostname=event['hostname'],
            pid=event['pid'],
            name=event['name'],
            workflow_id=event['workflow_id'],
            end_time=event['end_time']
        )
