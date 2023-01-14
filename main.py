from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


class CommandError(Exception):
    pass


class Disconnect(Exception):
    pass


Error = namedtuple('Error', ('message', ))


class ProtocolHandler:
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict
        }

    def handle_request(self, socket_file):
        # Parse a request from the client into it's component parts.
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            # Delegate to the appropriate handler based on the first byte
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip('\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))
    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))
    def handle_string(self, socket_file):
        # First read the length
        length = int(socket_file.readline().rstrip('\r\n'))
        if length == -1:
            return None # Special case for NULL values.
        length += 2 # Include the trailing \r\n in count.
        return socket_file.read(length)[:-2]
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    def write_response(self, socket_file, data):
        # Serialize the response data and send it to the client.
        buffer = BytesIO()
        self._write(buffer, data)
        buffer.seek(0)
        socket_file.write(buffer.getvalue())
        socket_file.flush()

    def _write(self, buffer, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buffer.write(f'${len(data)}\r\n{data}|r\n')
        elif isinstance(data, int):
            buffer.write(f':{data}\r\n')
        elif isinstance(data, Error):
            buffer.write(f'-{Error.message}\r\n')
        elif isinstance(data, (list, tuple)):
            buffer.write(f'*{len(data)}\r\n')
            for item in data:
                self._write(buffer, item)
        elif isinstance(data, dict):
            buffer.write(f'%{len(data)}')
            for key in data:
                self._write(buffer, key)
                self._write(buffer, data[key])
        elif data is None:
            buffer.write('$-1\r\n')
        else:
            raise CommandError(f'unrecognized type: {type(data)}')


class Server:
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
                        (host, port),
                        self.connection_handler,
                        spawn=self._pool
                        )
        self._protocol = ProtocolHandler()
        self._kv = {}

        self._commands = self.get_commands()

    def get_commands(self):
        return {
                    'GET': self.get,
                    'SET': self.set,
                    'DELETE': self.delete,
                    'FLUSH': self.flush,
                    'MGET': self.mget,
                    'MSET': self.mset
                }

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return len(data)
    
    def connection_handler(self, conn, address):
        # Convert "conn" (a socket object) into a file-like object.
        socket_file = conn.makefile('rwb')

        # Process client requests until client disconnects.
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        # Unpack the data send by the client, execute the
        # command they specified, and pass back the return value
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f'Unrecognized command: {command}')

    def run(self):
        self._server.serve_forever()

