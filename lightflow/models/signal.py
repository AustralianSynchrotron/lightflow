import zmq


class ConnectionInfo:
    def __init__(self, ip_address, port, protocol):
        self.ip_address = ip_address
        self.port = port
        self.protocol = protocol


class Server:
    def __init__(self, port_range=None, protocol='tcp', max_tries=100):
        self._port_range = port_range if port_range is not None else [49152, 65536]
        self._protocol = protocol
        self._max_tries = max_tries

        self._port = None

        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.REP)

    @property
    def port(self):
        return self._port

    def info(self):
        return ConnectionInfo('127.0.0.1', self._port, self._protocol)

    def bind(self):
        self._port = self._zmq_socket.bind_to_random_port(
            '{}://*'.format(self._protocol),
            min_port=self._port_range[0],
            max_port=self._port_range[1],
            max_tries=self._max_tries)

    def receive(self, block=False):
        try:
            return self._zmq_socket.recv_pyobj(flags=0 if block else zmq.NOBLOCK)
        except zmq.ZMQError:
            return None


class Client:
    def __init__(self, ip_address, port, protocol='tcp'):
        self._ip_address = ip_address
        self._port = port
        self._protocol = protocol

        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.REQ)

    @classmethod
    def from_connection(cls, connection):
        client = Client(connection.ip_address,
                        connection.port,
                        connection.protocol)
        client.connect()
        return client

    def connect(self):
        self._zmq_socket.connect('{}://{}:{}'.format(self._protocol,
                                                     self._ip_address,
                                                     self._port))

    def send(self, message, block=False):
        self._zmq_socket.send_pyobj(message,
                                    flags=0 if block else zmq.NOBLOCK)

    def info(self):
        return ConnectionInfo(self._ip_address, self._port, self._protocol)


class Request:
    def __init__(self, sender):
        self.sender = sender
