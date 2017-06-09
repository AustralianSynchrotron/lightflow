import pickle
import uuid
from time import sleep
from redis import StrictRedis

SIGNAL_REDIS_PREFIX = 'lightflow'


class SignalConnection:
    """ The connection to the redis signal broker database. """
    def __init__(self, host, port, database, *, auto_connect=False, polling_time=0.5):
        """ Initialise the SignalConnection object.

        Args:
            host (str): The host of the redis database.
            port (int): The port of the redis database.
            database (int): The number of the database.
            auto_connect (bool): Set to True to connect to the redis broker database.
            polling_time (float): The polling time for signal requests in seconds.
        """
        self._host = host
        self._port = port
        self._database = database
        self._polling_time = polling_time

        self._connection = None
        if auto_connect:
            self.connect()

    @property
    def is_connected(self):
        """ Returns the status of the signal connection. """
        return self._connection is not None

    @property
    def connection(self):
        """ Returns the connection object or None if the connection is not open. """
        return self._connection

    @property
    def polling_time(self):
        """ Returns the polling time for signal requests in seconds. """
        return self._polling_time

    def connect(self):
        """ Connects to the redis database. """
        self._connection = StrictRedis(
            host=self._host,
            port=self._port,
            db=self._database)


class Request:
    """ The request that is sent from a client to the server.

    This implements a custom request protocol with:
        - action: A string representing the requested action that should be
                  executed by the server.
        - payload: A dictionary with data that is available to the action.
                   The content depends on the type of action.
        - uid: A unique ID that is used to tag the response that follows this request.
        """
    def __init__(self, action, *, payload=None):
        """ Initialise the request object.

        Args:
            action (str): A string representing the requested action that should be
                          executed by the server.
            payload (dict): A dictionary with data that is available to the action.
        """
        self.action = action
        self.payload = payload if payload is not None else {}
        self.uid = uuid.uuid4()


class Response:
    """ The response that is sent from the server to the client.

    This implements a custom response protocol with:
        - success: Specifies whether the request was successful.
        - payload: A dictionary with response data. The content depends
                   on the type of response.
        - uid: A unique ID that matches the id of the initial request.
    """
    def __init__(self, success, uid, *, payload=None):
        """ Initialise the response object.

        Args:
            success (bool): True if the request was successful.
            uid (str): Unique response id.
            payload (dict): A dictionary with the response data.
        """
        self.success = success
        self.uid = uid
        self.payload = payload if payload is not None else {}


class Server:
    """ The server for the signal system, listening for requests from clients.

    This implementation retrieves requests from a list stored in redis. Each request
    is implemented using the Request class and stored as a pickled object. The response
    is stored under a unique response id, so the client can pick up the response.
    """
    def __init__(self, connection, request_key):
        """ Initialises the signal server.

        Args:
            connection: Reference to a signal connection object.
            request_key (str): The key under which the list of requests is stored.
        """
        self._connection = connection
        self._request_key = '{}:{}'.format(SIGNAL_REDIS_PREFIX, request_key)

    def receive(self):
        """ Returns a single request.

        Takes the first request from the list of requests and returns it. If the list
        is empty, None is returned.

        Returns:
            Response: If a new request is available a Request object is returned,
                      otherwise None is returned.
        """
        pickled_request = self._connection.connection.lpop(self._request_key)
        return pickle.loads(pickled_request) if pickled_request is not None else None

    def send(self, response):
        """ Send a response back to the client that issued a request.

        Args:
            response (Response): Reference to the response object that should be sent.
        """
        self._connection.connection.set('{}:{}'.format(SIGNAL_REDIS_PREFIX, response.uid),
                                        pickle.dumps(response))

    def restore(self, request):
        """ Push the request back onto the queue.

        Args:
            request (Request): Reference to a request object that should be pushed back
                               onto the request queue.
        """
        self._connection.connection.rpush(self._request_key, pickle.dumps(request))

    def clear(self):
        """ Deletes the list of requests from the redis database. """
        self._connection.connection.delete(self._request_key)


class Client:
    """ The client for the signal system, sending requests to the server.

    This implementation sends requests to a list stored in redis. Each request
    is implemented using the Request class and stored as a pickled object. The response
    from the server is stored under the unique response id.
    """
    def __init__(self, connection, request_key):
        """ Initialises the signal client.

        Args:
            connection: Reference to a signal connection object.
            request_key (str): The key under which the list of requests is stored.
        """
        self._connection = connection
        self._request_key = '{}:{}'.format(SIGNAL_REDIS_PREFIX, request_key)

    def send(self, request):
        """ Send a request to the server and wait for its response.

        Args:
            request (Request): Reference to a request object that is sent to the server.

        Returns:
            Response: The response from the server to the request.
        """
        self._connection.connection.rpush(self._request_key, pickle.dumps(request))
        resp_key = '{}:{}'.format(SIGNAL_REDIS_PREFIX, request.uid)

        while True:
            if self._connection.polling_time > 0.0:
                sleep(self._connection.polling_time)

            response_data = self._connection.connection.get(resp_key)
            if response_data is not None:
                self._connection.connection.delete(resp_key)
                break

        return pickle.loads(response_data)
