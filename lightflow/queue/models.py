from celery.result import AsyncResult

from lightflow.models.exceptions import JobStatInvalid


class BrokerStats:
    """ Represents the broker information returned from the celery stats. """
    def __init__(self, hostname, port, transport, virtual_host):
        """ Initialize the broker stats object.

        Args:
            hostname (str): The broker hostname.
            port (int): The broker port.
            transport (str): The transport protocol of the broker.
            virtual_host (str): The virtual host, e.g. the database number in redis.
        """
        self.hostname = hostname
        self.port = port
        self.transport = transport
        self.virtual_host = virtual_host

    @classmethod
    def from_celery(cls, broker_dict):
        """ Create a BrokerStats object from the dictionary returned by celery.

        Args:
            broker_dict (dict): The dictionary as returned by celery.

        Returns:
            BrokerStats: A fully initialized BrokerStats object.
        """
        return BrokerStats(
            hostname=broker_dict['hostname'],
            port=broker_dict['port'],
            transport=broker_dict['transport'],
            virtual_host=broker_dict['virtual_host']
        )

    def to_dict(self):
        """ Return a dictionary of the broker stats.

        Returns:
            dict: Dictionary of the stats.
        """
        return {
            'hostname': self.hostname,
            'port': self.port,
            'transport': self.transport,
            'virtual_host': self.virtual_host
        }


class QueueStats:
    """ Represents the queue information returned from the celery stats. """
    def __init__(self, name, routing_key):
        """ Initialize the queue stats object.

        Args:
            name (str): The name of the queue.
            routing_key (str): The routing key of the queue.
        """
        self.name = name
        self.routing_key = routing_key

    @classmethod
    def from_celery(cls, queue_dict):
        """ Create a QueueStats object from the dictionary returned by celery.

        Args:
            queue_dict (dict): The dictionary as returned by celery.

        Returns:
            QueueStats: A fully initialized QueueStats object.
        """
        return QueueStats(
            name=queue_dict['name'],
            routing_key=queue_dict['routing_key']
        )

    def to_dict(self):
        """ Return a dictionary of the queue stats.

        Returns:
            dict: Dictionary of the stats.
        """
        return {
            'name': self.name,
            'routing_key': self.routing_key
        }


class WorkerStats:
    """ Represents the worker information returned from the celery stats. """
    def __init__(self, name, broker, pid, process_pids,
                 concurrency, job_count, queues):
        """ Initialize the worker stats object.

        Args:
            name (str): The name of the worker.
            broker (BrokerStats): A reference to a BrokerStats Object the worker is using.
            pid (int): The PID of the worker.
            process_pids (int): The PIDs of the concurrent task processes.
            concurrency (int): The number of concurrent processes.
            job_count (int): The number of jobs this worker has processed so far.
            queues (list): A list of QueueStats objects that represent the queues this
                           worker is listening on.
        """
        self.name = name
        self.broker = broker
        self.pid = pid
        self.process_pids = process_pids
        self.concurrency = concurrency
        self.job_count = job_count
        self.queues = queues

    @classmethod
    def from_celery(cls, name, worker_dict, queues):
        """ Create a WorkerStats object from the dictionary returned by celery.

        Args:
            name (str): The name of the worker.
            worker_dict (dict): The dictionary as returned by celery.
            queues (list): A list of QueueStats objects that represent the queues this
                           worker is listening on.

        Returns:
            WorkerStats: A fully initialized WorkerStats object.
        """
        return WorkerStats(
            name=name,
            broker=BrokerStats.from_celery(worker_dict['broker']),
            pid=worker_dict['pid'],
            process_pids=worker_dict['pool']['processes'],
            concurrency=worker_dict['pool']['max-concurrency'],
            job_count=worker_dict['pool']['writes']['total'],
            queues=queues
        )

    def to_dict(self):
        """ Return a dictionary of the worker stats.

        Returns:
            dict: Dictionary of the stats.
        """
        return {
            'name': self.name,
            'broker': self.broker.to_dict(),
            'pid': self.pid,
            'process_pids': self.process_pids,
            'concurrency': self.concurrency,
            'job_count': self.job_count,
            'queues': [q.to_dict() for q in self.queues]
        }


class JobStats:
    """ Represents the job (=celery task) information returned from the celery stats. """
    def __init__(self, name, job_id, job_type, workflow_id, acknowledged, func_name,
                 hostname, worker_name, worker_pid, routing_key):
        """ Initialize the job stats object.

        Args:
            name (str): The name of the job.
            job_id (str): The internal ID of the job.
            job_type (str): The type of the job (workflow, dag, task).
            workflow_id (str): The id of the workflow that started this job.
            acknowledged (bool): True of the job was acknowledged by the message system.
            func_name (str): The name of the function that represents this job.
            hostname (str): The name of the host this job runs on.
            worker_name (str): The name of the worker this job runs on.
            worker_pid (int): The pid of the process this jobs runs on.
            routing_key (str): The routing key for this job.
        """
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
        """ Create a JobStats object from the dictionary returned by celery.

        Args:
            worker_name (str): The name of the worker this jobs runs on.
            job_dict (dict): The dictionary as returned by celery.
            celery_app: Reference to a celery application object.

        Returns:
            JobStats: A fully initialized JobStats object.
        """
        if not isinstance(job_dict, dict) or 'id' not in job_dict:
            raise JobStatInvalid('The job description is missing important fields.')

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

    def to_dict(self):
        """ Return a dictionary of the job stats.

        Returns:
            dict: Dictionary of the stats.
        """
        return {
            'name': self.name,
            'id': self.id,
            'type': self.type,
            'workflow_id': self.workflow_id,
            'acknowledged': self.acknowledged,
            'func_name': self.func_name,
            'hostname': self.hostname,
            'worker_name': self.worker_name,
            'worker_pid': self.worker_pid,
            'routing_key': self.routing_key
        }


class JobEvent:
    """ The base class for job events from celery. """
    def __init__(self, uuid, job_type, event_type, hostname, pid,
                 name, workflow_id, event_time, duration):
        """ Initialize the job event object.

        Args:
            uuid (str): The internal event id.
            job_type (str): The type of job that caused this event (workflow, dag, task).
            event_type (str): The internal event type name.
            hostname (str): The name of the host on which the job is running.
            pid (int): The pid of the process that runs the job.
            name (str): The name of the workflow, dag or task that caused this event.
            workflow_id (str): The id of the workflow that hosts this job.
            event_time (datetime): The time when the event was triggered.
            duration (float, None): The duration it took to execute the job.
        """
        self.uuid = uuid
        self.type = job_type
        self.event = event_type
        self.hostname = hostname
        self.pid = pid
        self.name = name
        self.workflow_id = workflow_id
        self.event_time = event_time
        self.duration = duration

    @classmethod
    def from_event(cls, event):
        """ Create a JobEvent object from the event dictionary returned by celery.

        Args:
            event (dict): The dictionary as returned by celery.

        Returns:
            JobEvent: A fully initialized JobEvent object.
        """
        return cls(
            uuid=event['uuid'],
            job_type=event['job_type'],
            event_type=event['type'],
            hostname=event['hostname'],
            pid=event['pid'],
            name=event['name'],
            workflow_id=event['workflow_id'],
            event_time=event['time'],
            duration=event['duration']
        )


class JobStartedEvent(JobEvent):
    """ This event is triggered when a new job starts running. """
    def __init__(self, uuid, job_type, event_type, hostname, pid,
                 name, workflow_id, event_time, duration):
        super().__init__(uuid, job_type, event_type, hostname, pid,
                         name, workflow_id, event_time, duration)


class JobSucceededEvent(JobEvent):
    """ This event is triggered when a job completed successfully. """
    def __init__(self, uuid, job_type, event_type, hostname, pid,
                 name, workflow_id, event_time, duration):
        super().__init__(uuid, job_type, event_type, hostname, pid,
                         name, workflow_id, event_time, duration)


class JobStoppedEvent(JobEvent):
    """ This event is triggered when a job was stopped. """
    def __init__(self, uuid, job_type, event_type, hostname, pid,
                 name, workflow_id, event_time, duration):
        super().__init__(uuid, job_type, event_type, hostname, pid,
                         name, workflow_id, event_time, duration)


class JobAbortedEvent(JobEvent):
    """ This event is triggered when a job was aborted. """
    def __init__(self, uuid, job_type, event_type, hostname, pid,
                 name, workflow_id, event_time, duration):
        super().__init__(uuid, job_type, event_type, hostname, pid,
                         name, workflow_id, event_time, duration)
