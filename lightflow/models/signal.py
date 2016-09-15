import pickle
import uuid

SIGNAL_REDIS_PREFIX = 'lightflow'


class Request:
    """ The request that is sent from a client to the server.

    This implements a custom request protocol with:
        - action: A string representing the requested action that should be
                  executed by the server.
        - payload: A dictionary with data that is available to the action.
                   The content depends on the type of action.
        - uid: A unique ID that is used to tag the response that follows this request.
        """
    def __init__(self, action, payload=None):
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
    def __init__(self, success, uid, payload=None):
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
    is published using redis' PUB/SUB system under a unique request id.
    """
    def __init__(self, redis_db, request_key):
        """ Initialises the signal server.

        Args:
            redis_db: Reference to a fully initialised redis object.
            request_key (str): The key under which the list of requests is stored.
        """
        self._redis_db = redis_db
        self._request_key = '{}:{}'.format(SIGNAL_REDIS_PREFIX, request_key)

    def receive(self):
        """ Returns a single request.

        Takes the first request from the list of requests and returns it. If the list
        is empty, None is returned.

        Returns:
            Response: If a new request is available a Request object is returned,
                      otherwise None is returned.
        """
        pickled_request = self._redis_db.lpop(self._request_key)
        return pickle.loads(pickled_request) if pickled_request is not None else None

    def send(self, response):
        """ Send a response back to the client that issued a request.

        Args:
            response (Response): Reference to the response object that should be sent.
        """
        self._redis_db.publish('{}:{}'.format(SIGNAL_REDIS_PREFIX, response.uid),
                               pickle.dumps(response))

    def clear(self):
        """ Deletes the list of requests from the redis database. """
        self._redis_db.delete(self._request_key)


class Client:
    """ The client for the signal system, sending requests to the server.

    This implementation sends requests to a list stored in redis. Each request
    is implemented using the Request class and stored as a pickled object. The response
    from the server is retrieved by using redis' PUB/SUB system. The client subscribes to
    a channel that is unique for the initial request.
    """
    def __init__(self, redis_db, request_key):
        """ Initialises the signal client.

        Args:
            redis_db: Reference to a fully initialised redis object.
            request_key (str): The key under which the list of requests is stored.
        """
        self._redis_db = redis_db
        self._request_key = '{}:{}'.format(SIGNAL_REDIS_PREFIX, request_key)

    def send(self, request):
        """ Send a request to the server and wait for its response.

        Args:
            request (Request): Reference to a request object that is sent to the server.

        Returns:
            Response: The response from the server to the request.
        """
        channel = '{}:{}'.format(SIGNAL_REDIS_PREFIX, request.uid)

        self._redis_db.rpush(self._request_key, pickle.dumps(request))
        psub = self._redis_db.pubsub(ignore_subscribe_messages=True)
        psub.subscribe(channel)
        response = pickle.loads(next(psub.listen())['data'])

        psub.punsubscribe(channel)
        return response
