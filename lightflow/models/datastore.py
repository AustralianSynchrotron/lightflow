

class DataStore:
    def __init__(self, host, port, database_name, username=None, password=None):
        self.host = host
        self.port = port
        self. database_name = database_name
        self.username = username
        self.password = password

    def connect(self):
        # TODO: throw connection exception if connection fails
        return self
        pass

    def disconnect(self):
        pass

    @property
    def is_connected(self):
        return False

    @classmethod
    def create_connection(cls, host, port, database_name, username=None, password=None):
        return cls(host, port, database_name, username, password).connect()

    def check_workflow_id(self, workflow_id):
        return False

    def create_workflow_id(self, workflow_id=None):
        if workflow_id is None:
            workflow_id = 'unique id'

        # TODO: create id if it doesn't exist yet
        return workflow_id
