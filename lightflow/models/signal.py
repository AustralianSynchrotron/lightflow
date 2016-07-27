import zmq


class ConnectionInfo:
    """ The connection information used by the clients to connect to the server. """
    def __init__(self, ip_address, port, protocol):
        """ Initialise the connection information.

        Args:
            ip_address (str): The IP address of the server.
            port (int): The port number the server is listening on.
            protocol (str): The identifier of the messaging protocol.
        """
        self.ip_address = ip_address
        self.port = port
        self.protocol = protocol

    def to_dict(self):
        """ Return the connection information as a dictionary. """
        return {
            'ip_address': self.ip_address,
            'port': self.port,
            'protocol': self.protocol
        }

    @classmethod
    def from_dict(cls, data):
        """ Create a connection information object from a dictionary. """
        return ConnectionInfo(data['ip_address'], data['port'], data['protocol'])


class Server:
    """ The server for the signal system, listening for requests from clients.

    This implementation uses the ZeroMQ library and the REQ-REP pattern. A request
    to the server has to be answered with a reply. The port number the server is listening
    on is the first available port number from a range of port numbers.
    """
    def __init__(self, port_range=None, protocol='tcp', max_tries=100):
        """ Initialises the signal server.

        Args:
            port_range (list): A two-element list representing the port number range from
                               which the first available port is taken. The first element
                               in the list represents the lowest allowed port number and
                               the second element the highest allowed port number.
            protocol (str): The identifier of the messaging protocol.
            max_tries (int): The number of max tries the ZeroMQ library undertakes in
                             order to find an available port.
        """
        self._port_range = port_range if port_range is not None else [49152, 65536]
        self._protocol = protocol
        self._max_tries = max_tries

        self._port = None

        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.REP)

    @property
    def port(self):
        """ Returns the port the server is listening on. """
        return self._port

    def info(self):
        """ Returns the connection info required to connect a client to this server.

        Returns:
            ConnectionInfo: The information required to connect a client to this server.
        """
        return ConnectionInfo('127.0.0.1', self._port, self._protocol)

    def bind(self):
        """ Starts the server listening on all interfaces and the selected port. """
        self._port = self._zmq_socket.bind_to_random_port(
            '{}://*'.format(self._protocol),
            min_port=self._port_range[0],
            max_port=self._port_range[1],
            max_tries=self._max_tries)

    def receive(self):
        """ Asynchronously checks whether a new request is available.

        This call is non-blocking. If a new request is not available it returns None.

        Returns:
            Response: If a new request is available a Response object is returned,
                      otherwise None is returned.
        """
        try:
            return self._zmq_socket.recv_pyobj(flags=zmq.NOBLOCK)
        except zmq.ZMQError:
            return None

    def send(self, response):
        """ Send a response back to the client that issued a request.

        Args:
            response (Response): Reference to the response object that should be sent.
        """
        self._zmq_socket.send_pyobj(response)


class Client:
    """ The client for the signal system, sending requests to the server.

    This implementation uses the ZeroMQ library and the REQ-REP pattern. A request
    to the server has to be followed by waiting for the reply.
    """
    def __init__(self, ip_address, port, protocol='tcp'):
        """ Initialises the signal client.

        Args:
            ip_address (str): The ip address of the server.
            port (int): The port number on which the server is listening for requests.
            protocol (str): The identifier of the messaging protocol.
        """
        self._ip_address = ip_address
        self._port = port
        self._protocol = protocol

        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.REQ)

    @classmethod
    def from_connection(cls, connection):
        """ Creates a client object from a connection information object.

        Args:
            connection (ConnectionInfo): Reference to a connection information object
                                         that hosts the servers connection details.

        Returns:
            Client: A client object initialised, connected and ready to be used.
        """
        client = Client(connection.ip_address,
                        connection.port,
                        connection.protocol)
        client.connect()
        return client

    def connect(self):
        """ Connect the client to the server. """
        self._zmq_socket.connect('{}://{}:{}'.format(self._protocol,
                                                     self._ip_address,
                                                     self._port))

    def send(self, request):
        """ Send a request to the server and wait for its response.

        Args:
            request (Request): Reference to a request object that is sent to the server.

        Returns:
            Response: The response from the server to this request.
        """
        self._zmq_socket.send_pyobj(request)
        return self._zmq_socket.recv_pyobj()

    def info(self):
        """ Returns the connection information for this client. """
        return ConnectionInfo(self._ip_address, self._port, self._protocol)


class Request:
    """ The request that is sent from a client to the server.

    This implements a custom request protocol with:
        - action: A string representing the requested action that should be
                  executed by the server.
        - payload: A dictionary with data that is available to the action.
                   The content depends on the type of action.
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


class Response:
    """ The response that is sent from the server to the client.

    This implements a custom response protocol with:
        - success: Specifies whether the request was successful.
        - payload: A dictionary with response data. The content depends
                   on the type of request.
    """
    def __init__(self, success, payload=None):
        """ Initialise the response object.

        Args:
            success (bool): True if the request was successful.
            payload (dict): A dictionary with response data.
        """
        self.success = success
        self.payload = payload if payload is not None else {}
