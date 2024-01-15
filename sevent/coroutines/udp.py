# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower


import greenlet
from ..errors import SocketClosed
from ..event import EventEmitter

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_BINDING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer):
    class Socket(BaseSocket):
        _send_greenlet = None
        _recv_greenlet = None
        _close_error_registed = False

        def _on_close_handle(self, socket):
            if self._send_greenlet is not None:
                child_gr, self._send_greenlet = self._send_greenlet, None
                child_gr.throw(SocketClosed())
            if self._recv_greenlet is not None:
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                child_gr.throw(SocketClosed())

        def _on_error_handle(self, socket, e):
            if self._send_greenlet is not None:
                if not self._events["drain"] and not self._events_once["drain"]:
                    self._has_drain_event = False
                child_gr, self._send_greenlet = self._send_greenlet, None
                child_gr.throw(e)
            if self._recv_greenlet is not None:
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                child_gr.throw(e)

        async def sendto(self, data):
            assert self._send_greenlet is None, "already sending"
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            if self.write(data):
                return

            self._send_greenlet = greenlet.getcurrent()
            main = self._send_greenlet.parent
            assert main is not None, "must be running in async func"

            def on_drain(socket):
                EventEmitter.off(self, "drain", on_drain)
                if not self._events["drain"] and not self._events_once["drain"]:
                    self._has_drain_event = False
                child_gr, self._send_greenlet = self._send_greenlet, None
                return child_gr.switch()
            EventEmitter.on(self, "drain", on_drain)
            self._has_drain_event = True
            return main.switch()

        async def recvfrom(self, size=0):
            assert self._recv_greenlet is None, "already recving"
            if self._state == STATE_CLOSED:
                raise SocketClosed()

            if self._rbuffers and len(self._rbuffers) >= size:
                return self._rbuffers

            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            self._recv_greenlet = greenlet.getcurrent()
            main = self._recv_greenlet.parent
            assert main is not None, "must be running in async func"

            def on_data(socket, buffer):
                if len(buffer) < size:
                    return
                EventEmitter.off(self, "data", on_data)
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                return child_gr.switch(buffer)
            EventEmitter.on(self, "data", on_data)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return
            if self._close_error_registed:
                EventEmitter.off(self, "close", self._on_close_handle)
                EventEmitter.off(self, "error", self._on_error_handle)
                self._close_error_registed = False

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            self.end()
            return main.switch()

        @classmethod
        async def linkof(cls, socket, address, timeout=900):
            if socket._state == STATE_CLOSED:
                raise SocketClosed()
            if hasattr(socket, "_close_error_registed") and socket._close_error_registed:
                EventEmitter.off(socket, "close", socket._on_close_handle)
                EventEmitter.off(socket, "error", socket._on_error_handle)
                socket._close_error_registed = False

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def do_closed(s):
                if socket._state != STATE_CLOSED:
                    return
                child_gr.switch()

            BaseSocket.link(socket, address, timeout)
            EventEmitter.on(socket, "close", do_closed)
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return
            if self._close_error_registed:
                EventEmitter.off(self, "close", self._on_close_handle)
                EventEmitter.off(self, "error", self._on_error_handle)
                self._close_error_registed = False

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            return main.switch()

    class Server(BaseServer):
        _bind_greenlet = None
        _close_error_registed = False

        def _on_close_handle(self, socket):
            if self._bind_greenlet is not None:
                child_gr, self._bind_greenlet = self._bind_greenlet, None
                child_gr.throw(SocketClosed())

        def _on_error_handle(self, socket, e):
            if self._bind_greenlet is not None:
                child_gr, self._bind_greenlet = self._bind_greenlet, None
                child_gr.throw(e)

        async def bindof(self, address):
            assert self._bind_greenlet is None, "already binding"
            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return
            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            self._bind_greenlet = greenlet.getcurrent()
            main = self._bind_greenlet.parent
            assert main is not None, "must be running in async func"

            def on_bind(server):
                EventEmitter.off(self, "bind", on_bind)
                child_gr, self._bind_greenlet = self._bind_greenlet, None
                return child_gr.switch()
            EventEmitter.on(self, "bind", on_bind)
            self.bind(address)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return
            if self._close_error_registed:
                EventEmitter.off(self, "close", self._on_close_handle)
                EventEmitter.off(self, "error", self._on_error_handle)
                self._close_error_registed = False

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            self.end()
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return
            if self._close_error_registed:
                EventEmitter.off(self, "close", self._on_close_handle)
                EventEmitter.off(self, "error", self._on_error_handle)
                self._close_error_registed = False

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            return main.switch()

    return Socket, Server
