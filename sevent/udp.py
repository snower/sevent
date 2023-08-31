# -*- coding: utf-8 -*-
# 2014/12/28
# create by: snower

import time
import socket
import errno
from .utils import is_py3, get_logger
from .event import EventEmitter, null_emit_callback
from .loop import instance, MODE_IN, MODE_OUT
from .dns import DNSResolver
from .buffer import Buffer, BaseBuffer, cbuffer, RECV_BUFFER_SIZE
from .errors import SocketClosed, ResolveError, AddressError, ConnectError

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_BINDING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


class Socket(EventEmitter):
    MAX_BUFFER_SIZE = None
    RECV_BUFFER_SIZE = RECV_BUFFER_SIZE

    @classmethod
    def config(cls, max_buffer_size=None, recv_buffer_size=RECV_BUFFER_SIZE, **kwargs):
        cls.MAX_BUFFER_SIZE = max_buffer_size
        cls.RECV_BUFFER_SIZE = recv_buffer_size

    def __init__(self, loop=None, dns_resolver=None, max_buffer_size=None):
        EventEmitter.__init__(self)
        self._loop = loop or instance()
        self._dns_resolver = dns_resolver

        self._socket = None
        self._fileno = 0
        self._socket_family = 2
        self._read_handler = None
        self._write_handler = None
        self._has_drain_event = False
        self._is_enable_broadcast = False

        self._max_buffer_size = max_buffer_size or self.MAX_BUFFER_SIZE
        self._rbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._wbuffers = Buffer(max_buffer_size=self._max_buffer_size)
        self._address_cache = {}
        self._state = STATE_INITIALIZED
        self.ignore_write_closed_error = False

    @property
    def socket(self):
        return self._socket

    @property
    def buffer(self):
        return self._rbuffers, self._wbuffers

    def _connect(self, address, callback):
        def resolve_callback(hostname, ip):
            if not ip:
                return self._loop.add_async(self._error, ResolveError('can not resolve hostname %s' % str(address)))

            try:
                addrinfo = socket.getaddrinfo(ip, address[1], 0, 0, socket.SOL_UDP)
                if not addrinfo:
                    return self._loop.add_async(self._error, AddressError('address info unknown %s' % str(address)))

                addr = addrinfo[0]
                self._socket = socket.socket(addr[0], addr[1], addr[2])
                self._fileno = self._socket.fileno()
                self._socket_family = addr[0]
                self._socket.setblocking(False)
                self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if self._is_enable_broadcast:
                    self.enable_broadcast()
                self._state = STATE_STREAMING

                self._read_handler = self._loop.add_fd(self._fileno, MODE_IN, self._read_cb)

                self._rbuffers.on("drain", lambda _: self.drain())
                self._rbuffers.on("regain", lambda _: self.regain())

                self._write_handler = None
                self._loop.add_async(callback, (ip, address[1]))
            except socket.error as e:
                if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                    return self._loop.add_async(self._error, ConnectError(address, e, "connect error %s %s" % (str(address), e)))
            except Exception as e:
                return self._loop.add_async(self._error, e)

        self._state = STATE_CONNECTING
        if not self._dns_resolver:
            self._dns_resolver = DNSResolver.default()
        return self._dns_resolver.resolve(address[0], resolve_callback)

    def __del__(self):
        self.close()

    def on(self, event_name, callback):
        EventEmitter.on(self, event_name, callback)

        if event_name == "drain":
            self._has_drain_event = True

    def once(self, event_name, callback):
        EventEmitter.once(self, event_name, callback)

        if event_name == "drain":
            self._has_drain_event = True

    def off(self, event_name, callback):
        EventEmitter.off(self, event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

    def noce(self, event_name, callback):
        EventEmitter.noce(self, event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

    def remove_listener(self, event_name, callback):
        EventEmitter.remove_listener(self, event_name, callback)

        if not self._events[event_name] and not self._events_once[event_name]:
            if event_name == "drain":
                self._has_drain_event = False

    def on_data(self, callback):
        self.on("data", callback)

    def on_close(self, callback):
        self.on("close", callback)

    def on_error(self, callback):
        self.on("error", callback)

    def on_drain(self, callback):
        self.on("drain", callback)

    def off_data(self, callback):
        self.on("data", callback)

    def off_close(self, callback):
        self.on("close", callback)

    def off_error(self, callback):
        self.on("error", callback)

    def off_drain(self, callback):
        self.on("drain", callback)

    def once_data(self, callback):
        self.once("data", callback)

    def once_close(self, callback):
        self.once("close", callback)

    def once_error(self, callback):
        self.once("error", callback)

    def once_drain(self, callback):
        self.once("drain", callback)

    def noce_data(self, callback):
        self.once("data", callback)

    def noce_close(self, callback):
        self.once("close", callback)

    def noce_error(self, callback):
        self.once("error", callback)

    def noce_drain(self, callback):
        self.once("drain", callback)

    def enable_broadcast(self):
        if self._socket:
            try:
                self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            except Exception as e:
                get_logger().warning('broadcast error: %s', e)
                self._is_enable_broadcast = False
                return
        self._is_enable_broadcast = True

    @property
    def is_enable_broadcast(self):
        return self._is_enable_broadcast

    def end(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if not self._write_handler:
                self._loop.add_async(self.close)
            else:
                self._state = STATE_CLOSING

    def close(self):
        if self._state == STATE_CLOSED:
            return
        if self._read_handler:
            try:
                self._loop.remove_fd(self._fileno, self._read_cb)
            except Exception as e:
                get_logger().error("socket close remove_fd error:%s", e)
            self._read_handler = False
        if self._write_handler:
            try:
                self._loop.remove_fd(self._fileno, self._write_cb)
            except Exception as e:
                get_logger().error("socket close remove_fd error:%s", e)
            self._write_handler = False
        self._state = STATE_CLOSED

        def on_close():
            if self._socket:
                try:
                    self._loop.clear_fd(self._fileno)
                except Exception as e:
                    get_logger().error("server close clear_fd error: %s", e)
                try:
                    self._socket.close()
                except Exception as e:
                    get_logger().error("socket close socket error:%s", e)

            try:
                self.emit_close(self)
            except Exception as e:
                get_logger().exception("tcp emit close error:%s", e)
            self.remove_all_listeners()
            self._rbuffers.close()
            self._wbuffers.close()
            self._rbuffers = None
            self._wbuffers = None
            self._address_cache = None
        self._loop.add_async(on_close)

    def _error(self, error):
        self._loop.add_async(self.emit_error, self, error)
        self._loop.add_async(self.close)
        if self.emit_error == null_emit_callback:
            get_logger().error("socket error: %s", error)

    def drain(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if self._read_handler:
                try:
                    self._loop.remove_fd(self._fileno, self._read_cb)
                except Exception as e:
                    return self._error(e)
                self._read_handler = False

    def regain(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if not self._read_handler:
                try:
                    self._read_handler = self._loop.add_fd(self._fileno, MODE_IN, self._read_cb)
                except Exception as e:
                    self._error(e)

    def _read_cb(self):
        if self._state in (STATE_STREAMING, STATE_BINDING):
            if self._read():
                self._loop.add_async(self.emit_data, self, self._rbuffers)

    if cbuffer is None:
        def _read(self):
            last_data_len = self._rbuffers._len
            while self._read_handler:
                try:
                    data, address = self._socket.recvfrom(self.RECV_BUFFER_SIZE)
                    BaseBuffer.write(self._rbuffers, data, address)
                except socket.error as e:
                    if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                        break
                    else:
                        self._error(e)
                        if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                            self._rbuffers.do_drain()
                        return False
                except Exception as e:
                    self._error(e)
                    return
                else:
                    if self._rbuffers._len > self._rbuffers._len:
                        break

            if last_data_len < self._rbuffers._len:
                if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                    self._rbuffers.do_drain()
                return True
            return False
    else:
        def _read(self):
            try:
                r = self._rbuffers.socket_recvfrom(self._fileno, self._socket_family, self._rbuffers._drain_size)
            except Exception as e:
                self._error(e)
                if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                    self._rbuffers.do_drain()
                return False

            if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                self._rbuffers.do_drain()
            return r

    def _write_cb(self):
        if self._state != STATE_CLOSED:
            if self._write():
                if self._has_drain_event:
                    self._loop.add_async(self.emit_drain, self)
                if self._write_handler:
                    try:
                        self._loop.remove_fd(self._fileno, self._write_cb)
                    except Exception as e:
                        return self._error(e)
                    self._write_handler = False

                if self._state == STATE_CLOSING:
                    self.close()

    if cbuffer is None:
        def _write(self):
            while self._wbuffers:
                data = self._wbuffers
                try:
                    if data._buffer_index > 0:
                        r = self._socket.sendto(memoryview(data._buffer)[data._buffer_index:], data._buffer_odata)
                    else:
                        r = self._socket.sendto(data._buffer, data._buffer_odata)
                    data._buffer_index += r
                    data._len -= r

                    if data._buffer_index >= data._buffer_len:
                        if data._len > 0:
                            data._buffer = data._buffers.popleft()
                            data._buffer_odata = data._buffers_odata.popleft()
                            data._buffer_index, data._buffer_len = 0, len(data._buffer)
                        else:
                            data._buffer_index, data._buffer_len, data._buffer, data._buffer_odata = 0, 0, b'', None
                            if data._full and data._len < data._regain_size:
                                data.do_regain()
                        continue
                    else:
                        if data._full and data._len < data._regain_size:
                            data.do_regain()
                        return False
                except socket.error as e:
                    if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        self._error(e)
                    if data._full and data._len < data._regain_size:
                        data.do_regain()
                    return False
                except Exception as e:
                    self._error(e)
                    return
            return True
    else:
        def _write(self):
            try:
                self._wbuffers.socket_sendto(self._fileno, self._socket_family)
                if self._wbuffers:
                    if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                        self._wbuffers.do_regain()
                    return False
            except Exception as e:
                self._error(e)
                if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                    self._wbuffers.do_regain()
                return False

            if self._wbuffers._full and self._wbuffers._len < self._wbuffers._regain_size:
                self._wbuffers.do_regain()
            return True

    def write(self, data):
        if self._state == STATE_CLOSED:
            if self.ignore_write_closed_error:
                return False
            raise SocketClosed()

        def do_write():
            if self._state not in (STATE_STREAMING, STATE_BINDING):
                if self._state == STATE_INITIALIZED:
                    self._connect(self._wbuffers.head_data(), lambda address: do_write())
                return False

            if not self._write_handler:
                if self._write():
                    if self._has_drain_event:
                        self._loop.add_async(self.emit_drain, self)
                    return True
                else:
                    if self._wbuffers._len > self._wbuffers._drain_size and not self._wbuffers._full:
                        self._wbuffers.do_drain()
                try:
                    self._write_handler = self._loop.add_fd(self._fileno, MODE_OUT, self._write_cb)
                except Exception as e:
                    self._error(e)
            return False

        if data.__class__ == Buffer:
            BaseBuffer.extend(self._wbuffers, data)
            if data._full and data._len < data._regain_size:
                data.do_regain()
            return do_write()

        data, address = data
        if address[0] not in self._address_cache:
            def resolve_callback(hostname, ip):
                if not ip:
                    return self._error(ResolveError('can not resolve hostname %s' % str(address)))
                self._address_cache[hostname] = ip
                BaseBuffer.write(self._wbuffers, data, (ip, address[1]))
                return do_write()

            if not self._dns_resolver:
                self._dns_resolver = DNSResolver.default()
            return self._dns_resolver.resolve(address[0], resolve_callback)

        BaseBuffer.write(self._wbuffers, data, (self._address_cache[address[0]], address[1]))
        return do_write()

    @classmethod
    def link(cls, socket, address, timeout=900):
        if socket._state == STATE_CLOSED:
            raise SocketClosed()

        socket.ignore_write_closed_error = True
        linked_maps = {}
        setattr(socket, "link_timeout_timer", None)

        def on_link_data(s, data):
            while data:
                buf, addr = data.next()
                socket.write((buf, s.link_remote_address))
                s.link_data_time = time.time()

        def on_link_close(s):
            if s.link_remote_address in linked_maps:
                linked_maps.pop(s.link_remote_address)

        def on_data(s, data):
            while data:
                buf, addr = data.next()
                if addr in linked_maps:
                    link_socket = linked_maps[addr]
                    link_socket.link_data_time = time.time()
                else:
                    link_socket = Socket()
                    setattr(link_socket, "link_remote_address", addr)
                    setattr(link_socket, "link_data_time", time.time())
                    link_socket.ignore_write_closed_error = True
                    link_socket.on_data(on_link_data)
                    link_socket.on_close(on_link_close)
                    linked_maps[addr] = link_socket
                link_socket.write((buf, address))

        def on_close(s):
            for addr in list(linked_maps.keys()):
                link_socket = linked_maps[addr]
                link_socket.end()
            linked_maps.clear()
            if socket.link_timeout_timer:
                instance().cancel_timeout(socket.link_timeout_timer)
                socket.link_timeout_timer = None
        socket.on_data(on_data)
        socket.on_close(on_close)

        if timeout:
            def on_timeout():
                try:
                    now = time.time()
                    for addr in list(linked_maps.keys()):
                        link_socket = linked_maps[addr]
                        if now - link_socket.link_data_time < timeout:
                            continue
                        link_socket.end()
                        linked_maps.pop(addr)
                finally:
                    socket.link_timeout_timer = instance().add_timeout(timeout / 5, on_timeout)
            socket.link_timeout_timer = instance().add_timeout(timeout / 5, on_timeout)


class Server(Socket):
    def __init__(self, loop=None):
        Socket.__init__(self, loop)

    def on_bind(self, callback):
        self.on("bind", callback)

    def once_bind(self, callback):
        self.on("bind", callback)

    def bind(self, address):
        if self._state != STATE_INITIALIZED:
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            return

        def do_bind(address):
            try:
                self._socket.bind(address)
                self._state = STATE_BINDING
                self._loop.add_async(self.emit_bind, self)
            except Exception as e:
                self._loop.add_async(self._error, e)

        self._connect(address, do_bind)

    def __del__(self):
        self.close()


if is_py3:
    from .coroutines.udp import warp_coroutine
    Socket, Server = warp_coroutine(Socket, Server)