# -*- coding: utf-8 -*-

import logging
from collections import deque
import socket
import errno
import event
from loop import instance, MODE_IN, MODE_OUT
from buffer import Buffer
from dns import DNSResolver

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20

RECV_BUFSIZE = 0xffff

class Socket(event.EventEmitter):
    def __init__(self, loop=None, socket=None, address=None, dns_resolver = None):
        super(Socket, self).__init__()
        self._loop =loop or instance()
        self._socket = socket
        self._address = address
        self._dns_resolver = dns_resolver or DNSResolver.default()
        self._connect_handler = None
        self._read_handler = None
        self._write_handler = None
        self._rbuffers = Buffer()
        self._wbuffers = deque()
        self._state = STATE_INITIALIZED

        if self._socket:
            self._state = STATE_STREAMING
            self._socket.setblocking(False)
            self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)

    @property
    def address(self):
        return self._address

    @property
    def socket(self):
        return self._socket

    def __del__(self):
        self.close()

    def end(self):
        if self._state not in (STATE_INITIALIZED, STATE_CONNECTING, STATE_STREAMING):return
        if self._state in (STATE_INITIALIZED, STATE_CONNECTING):
            self.close()
        else:
            if self._wbuffers:
                self._state = STATE_CLOSING
            else:
                self.close()

    def close(self):
        if self._state == STATE_CLOSED:
            return

        if self._state == STATE_CONNECTING and self._connect_handler:
            self._loop.remove_handler(self._connect_handler)
            self._connect_handler = None
        elif self._state in (STATE_STREAMING, STATE_CLOSING):
            if self._read_handler:
                self._loop.remove_handler(self._read_handler)
                self._read_handler = None
            if self._write_handler:
                self._loop.remove_handler(self._write_handler)
                self._write_handler = None
            try:
                self._socket.close()
            except Exception,e:
                logging.error("socket close socket error:%s",e)

        self._state = STATE_CLOSED
        def on_close():
            self.emit('close', self)
            self.remove_all_listeners()
        self._loop.sync(on_close)

    def _error(self, error):
        self._loop.sync(self.emit,'error', self, error)
        self.close()
        logging.error("socket error:%s",error)

    def _connect_cb(self):
        if self._state != STATE_CONNECTING:
            return
        self._loop.remove_handler(self._connect_handler)
        self._connect_handler = None

        self._state = STATE_STREAMING
        self._read_handler = self._loop.add_fd(self._socket, MODE_IN, self._read_cb)
        self._loop.sync(self.emit, 'connect', self)

    def _timeout_cb(self):
        if self._state == STATE_CONNECTING:
            self._error(Exception("connect time out"))

    def connect(self, address, timeout=5):
        if self._state != STATE_INITIALIZED:
            return

        def do_connect(hostname, ip):
            if self._state == STATE_CLOSED:
                return

            if not ip:
                return self._error(Exception('can not resolve hostname %s',address))

            try:
                addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_TCP)
                # support both IPv4 and IPv6 addresses
                if addrinfo:
                    addr = addrinfo[0]
                    self._socket = socket.socket(addr[0], addr[1], addr[2])
                    self._socket.setblocking(False)
                    self._socket.connect(addr[4])
                    self._address = addr[4]
                else:
                    self._error(Exception('can not resolve hostname %s',address))
                    return
            except socket.error as e:
                if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                    self._error(e)
                    return
            self._connect_handler = self._loop.add_fd(self._socket, MODE_OUT, self._connect_cb)

        self._dns_resolver.resolve(address[0], do_connect)
        self._loop.timeout(timeout,self._timeout_cb)
        self._state = STATE_CONNECTING

    def _read_cb(self):
        if self._state in (STATE_STREAMING, STATE_CLOSING):
            self._read()

    def _read(self):
        data = False
        while self._state in (STATE_STREAMING, STATE_CLOSING):
            try:
                data = self._socket.recv(RECV_BUFSIZE)
                if not data:
                    break
                self._rbuffers.write(data)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):break
                else:
                    self._error(e)
                    return

        if self._rbuffers and ("data" in self._events or "data" in self._events_once):
            self._loop.sync(self.emit,'data', self, self._rbuffers)

        if not data:
            self._loop.sync(self.emit,'end', self)
            if self._state in (STATE_STREAMING, STATE_CLOSING):
                self.close()

    def _write_cb(self):
        if self._state not in (STATE_STREAMING, STATE_CLOSING):return
        if self._write():
            self._loop.sync(self.emit,'drain', self)

    def _write(self):
        while self._state in (STATE_STREAMING, STATE_CLOSING) and self._wbuffers:
            data = self._wbuffers.popleft()
            try:
                r = self._socket.send(data)
                if r < len(data):
                    self._wbuffers.appendleft(data[r:])
                    return False
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self._wbuffers.appendleft(data)
                else:
                    self._error(e)
                return False

        if self._write_handler:
            self._loop.remove_handler(self._write_handler)
            self._write_handler = None
        if self._state == STATE_CLOSING:
            self.close()
        return True

    def write(self, data):
        if self._state !=STATE_STREAMING:
            return False
        self._wbuffers.append(data.read(-1) if isinstance(data, Buffer) else data)
        if not self._write():
            if not self._write_handler:
                self._write_handler = self._loop.add_fd(self._socket, MODE_OUT, self._write_cb)
                if not self._write_handler:
                    self._error(Exception("write data add fd error"))
                    return False
            return True
        self._loop.sync(self.emit,'drain', self)
        return True


class Server(event.EventEmitter):
    def __init__(self, loop=None, dns_resolver = None):
        super(Server, self).__init__()
        self._loop = loop or instance()
        self._dns_resolver = dns_resolver or DNSResolver.default()
        self._socket = None
        self._state = STATE_INITIALIZED

    def __del__(self):
        self.close()

    def listen(self, address, backlog=128):
        if self._state != STATE_INITIALIZED:
            return

        def do_listen(hostname, ip):
            if not ip:
                return self._error(Exception('can not resolve hostname %s' % address))

            addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_TCP)
            if addrinfo:
                addr = addrinfo[0]
                self._socket = socket.socket(addr[0], addr[1], addr[2])
                self._socket.setblocking(False)
                self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self._socket.bind(addr[4])
                self._accept_handler = self._loop.add_fd(self._socket, MODE_IN, self._accept_cb)
                self._socket.listen(backlog)
            else:
                self._error(Exception('can not resolve hostname %s' % address))

        self._dns_resolver.resolve(address[0], do_listen)
        self._state = STATE_LISTENING

    def _accept_cb(self):
        if self._state != STATE_LISTENING:
            return

        connection, address = self._socket.accept()
        socket = Socket(loop=self._loop, socket=connection, address=address)
        self._loop.sync(self.emit, "connection", self, socket)

    def _error(self, error):
        self._loop.sync(self.emit,'error', self, error)
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
            def on_close():
                self.emit('close', self)
                self.remove_all_listeners()
            self._loop.sync(on_close)