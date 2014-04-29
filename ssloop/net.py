import socket
import event
import threading
import loop as loop_
from loop import instance
import logging
import collections
import errno

STATE_CLOSED = 0
STATE_INITIALIZED = 1
STATE_CONNECTING = 2
STATE_STREAMING = 4
STATE_LISTENING = 8
STATE_CLOSING = 16

RECV_BUFSIZE = 4096

class Socket(event.EventEmitter):
    def __init__(self, loop=None, sock=None):
        super(Socket, self).__init__()
        self._socket = None
        self._loop =loop or instance()
        self._buffers = collections.deque()
        self._state = STATE_INITIALIZED
        self._connect_handler = None
        self._write_handler = None
        self._lock=threading.Lock()

        if sock is not None:
            self._socket = sock
            sock.setblocking(False)
            self._init_streaming()

    def __del__(self):
        self.close()

    def end(self):
        assert self._state in (STATE_INITIALIZED, STATE_CONNECTING, STATE_STREAMING)
        if self._state in (STATE_INITIALIZED, STATE_CONNECTING):
            self.close()
        else:
            if self._buffers:self._state = STATE_CLOSING
            else:self.close()

    def close(self):
        if self._state==STATE_CLOSED:return
        if self._state == STATE_CONNECTING and self._connect_handler:
            self._loop.remove_handler(self._connect_handler)
            self._connect_handler = None
        elif self._state == STATE_STREAMING or self._state == STATE_CLOSING:
            if self._read_handler:
                self._loop.remove_handler(self._read_handler)
                self._read_handler = None
            if self._write_handler:
                self._loop.remove_handler(self._write_handler)
                self._write_handler = None
        elif self._state == STATE_INITIALIZED:
            try:
                self._socket.close()
            except Exception,e:
                logging.error("socket close socket error:%s",e)
        self._state = STATE_CLOSED
        self.emit('close', self)

    def _error(self, error):
        self.emit('error', self, error)
        self.close()
        logging.error("socket error:%s",error)

    def _connect_cb(self):
        assert self._state == STATE_CONNECTING
        self._loop.remove_handler(self._connect_handler)
        self._connect_handler = None
        self._init_streaming()
        self.emit('connect', self)

    def _init_streaming(self):
        self._state = STATE_STREAMING
        self._read_handler = self._loop.add_fd(self._socket, loop_.MODE_IN, self._read_cb)

    def connect(self, address):
        assert self._state == STATE_INITIALIZED
        try:
            addrs = socket.getaddrinfo(address[0], address[1], 0, 0, socket.SOL_TCP)
            # support both IPv4 and IPv6 addresses
            if addrs:
                addr = addrs[0]
                self._socket = socket.socket(addr[0], addr[1], addr[2])
                self._socket.setblocking(False)
                self._socket.connect(addr[4])
            else:
                self._error(Exception('can not resolve hostname %s',address))
                return
        except socket.error as e:
            if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                self._error(e)
                return
        self._connect_handler = self._loop.add_fd(self._socket, loop_.MODE_OUT, self._connect_cb)
        self._state = STATE_CONNECTING

    def _read_cb(self):
        assert self._state == STATE_STREAMING
        self._read()

    def _read(self):
        cache = []
        data = False
        while 1:
            try:
                data = self._socket.recv(RECV_BUFSIZE)
                if not data:break
                cache.append(data)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):break
                else:
                    self._error(e)
                    return

        if cache:self.emit('data', self, ''.join(cache))
        if not data:
            self.emit('end', self)
            if self._state == STATE_STREAMING:
                self.close()

    def _write_cb(self):
        assert self._state in (STATE_STREAMING, STATE_CLOSING)
        with self._lock:
            result = self._write()
        if result:self.emit('drain', self)

    def _write(self):
        assert self._state in (STATE_STREAMING, STATE_CLOSING)
        while len(self._buffers) > 0:
            data = self._buffers.popleft()
            try:
                r = self._socket.send(data)
                if r < len(data):
                    self._buffers.appendleft(data[r:])
                    return False
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self._buffers.appendleft(data)
                else:self._error(e)
                return False

        if self._write_handler:
            self._loop.remove_handler(self._write_handler)
            self._write_handler = None
        if self._state == STATE_CLOSING:
            self.close()
        return True

    def write(self, data):
        with self._lock:
            self._buffers.append(data)
            if not self._write_handler:
                self._write_handler = self._loop.add_fd(self._socket, loop_.MODE_OUT, self._write_cb)
                if not self._write_handler:
                    self.close()
                    return False
            return self._write()


class Server(event.EventEmitter):
    def __init__(self, address, loop=None):
        super(Server, self).__init__()
        self._address = address
        self._loop = loop or instance()
        self._socket = None

        addrs = socket.getaddrinfo(address[0], address[1], 0, 0, socket.SOL_TCP)
        if addrs:
            addr = addrs[0]
            self._socket = socket.socket(addr[0], addr[1], addr[2])
            self._socket.setblocking(False)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._socket.bind(addr[4])
        else:
            self._error(Exception('can not resolve hostname %s' % address))

        self._state = STATE_INITIALIZED

    def __del__(self):
        self.close()

    def listen(self, backlog=128):
        assert self._state == STATE_INITIALIZED
        self._accept_handler = self._loop.add_fd(self._socket, loop_.MODE_IN, self._accept_cb)
        self._socket.listen(backlog)
        self._state = STATE_LISTENING

    def _accept_cb(self):
        assert self._state == STATE_LISTENING
        conn, addr = self._socket.accept()
        sock = Socket(loop=self._loop, sock=conn)
        self.emit('connection', self, sock)

    def _error(self, error):
        self.emit('error', self, error)
        self.close()
        logging.error("server error:%s",error)

    def close(self):
        if self._state in (STATE_INITIALIZED, STATE_LISTENING):
            if self._accept_handler:
                self._loop.remove_handler(self._accept_handler)
                self._accept_handler=None
            if self._socket is not None:
                try:
                    self._socket.close()
                except Exception,e:
                    logging.error("server close socket error:%s",e)
            self._state = STATE_CLOSED
            self.emit('close', self)
