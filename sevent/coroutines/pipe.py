# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import greenlet
from ..errors import SocketClosed
from ..event import EventEmitter

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer):
    class PipeSocket(BaseSocket):
        _connect_greenlet = None
        _send_greenlet = None
        _recv_greenlet = None
        _close_error_registed = False
        _recv_size = 0

        def _on_close_handle(self, socket):
            if self._connect_greenlet is not None:
                EventEmitter.off(self, "connect", self._on_connect_handle)
                child_gr, self._connect_greenlet = self._connect_greenlet, None
                child_gr.throw(SocketClosed())
            if self._send_greenlet is not None:
                EventEmitter.off(self, "drain", self._on_send_handle)
                child_gr, self._send_greenlet = self._send_greenlet, None
                child_gr.throw(SocketClosed())
            if self._recv_greenlet is not None:
                EventEmitter.off(self, "data", self._on_recv_handle)
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                child_gr.throw(SocketClosed())

        def _on_error_handle(self, socket, e):
            if self._connect_greenlet is not None:
                EventEmitter.off(self, "connect", self._on_connect_handle)
                child_gr, self._connect_greenlet = self._connect_greenlet, None
                child_gr.throw(e)
            if self._send_greenlet is not None:
                EventEmitter.off(self, "drain", self._on_send_handle)
                if not self._events["drain"] and not self._events_once["drain"]:
                    self._has_drain_event = False
                child_gr, self._send_greenlet = self._send_greenlet, None
                child_gr.throw(e)
            if self._recv_greenlet is not None:
                EventEmitter.off(self, "data", self._on_recv_handle)
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                child_gr.throw(e)

        def _on_connect_handle(self, socket):
            if self._connect_greenlet is None:
                return
            EventEmitter.off(self, "connect", self._on_connect_handle)
            if self._close_error_registed:
                EventEmitter.off(self, "close", self._on_close_handle)
                EventEmitter.off(self, "error", self._on_error_handle)
                self._close_error_registed = False
            child_gr, self._connect_greenlet = self._connect_greenlet, None
            return child_gr.switch()

        def _on_send_handle(self, socket):
            if self._send_greenlet is None:
                return
            EventEmitter.off(self, "drain", self._on_send_handle)
            if not self._events["drain"] and not self._events_once["drain"]:
                self._has_drain_event = False
            child_gr, self._send_greenlet = self._send_greenlet, None
            return child_gr.switch()

        def _on_recv_handle(self, socket, buffer):
            if self._recv_greenlet is None:
                return
            if len(buffer) < self._recv_size:
                return
            EventEmitter.off(self, "data", self._on_recv_handle)
            child_gr, self._recv_greenlet = self._recv_greenlet, None
            return child_gr.switch(buffer)

        async def connectof(self, address, timeout=5):
            assert self._connect_greenlet is None, "already connecting"
            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return
            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            self._connect_greenlet = greenlet.getcurrent()
            main = self._connect_greenlet.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "connect", self._on_connect_handle)
            self.connect(address, timeout)
            return main.switch()

        async def send(self, data):
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

            EventEmitter.on(self, "drain", self._on_send_handle)
            self._has_drain_event = True
            return main.switch()

        async def recv(self, size=0):
            assert self._recv_greenlet is None, "already recving"
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            if self._rbuffers and len(self._rbuffers) >= size:
                return self._rbuffers

            self._recv_greenlet = greenlet.getcurrent()
            main = self._recv_greenlet.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "data", self._on_recv_handle)
            self._recv_size = size
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            self.end()
            return main.switch()

        async def linkof(self, socket):
            assert self._connect_greenlet is None, "already connecting"
            assert self._send_greenlet is None, "already sending"
            assert self._recv_greenlet is None, "already recving"
            if self._state not in (STATE_STREAMING, STATE_CONNECTING):
                raise SocketClosed()
            if socket._state not in (STATE_STREAMING, STATE_CONNECTING):
                raise SocketClosed()
            if self._close_error_registed:
                EventEmitter.off(self, "close", self._on_close_handle)
                EventEmitter.off(self, "error", self._on_error_handle)
                self._close_error_registed = False
            if hasattr(socket, "_close_error_registed") and socket._close_error_registed:
                EventEmitter.off(socket, "close", socket._on_close_handle)
                EventEmitter.off(socket, "error", socket._on_error_handle)
                socket._close_error_registed = False

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def do_closed(s):
                if self._state != STATE_CLOSED:
                    return
                if socket._state != STATE_CLOSED:
                    return
                child_gr.switch()

            EventEmitter.on(self, "close", do_closed)
            EventEmitter.on(socket, "close", do_closed)
            BaseSocket.link(self, socket)
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            return main.switch()

    class PipeServer(BaseServer):
        _listen_greenlet = None
        _accept_greenlet = None
        _close_error_registed = False

        def _on_close_handle(self, socket):
            if self._listen_greenlet is not None:
                EventEmitter.off(self, "listen", self._on_listen_handle)
                child_gr, self._listen_greenlet = self._listen_greenlet, None
                child_gr.throw(SocketClosed())
            if self._accept_greenlet is not None:
                EventEmitter.off(self, "connection", self._on_accept_handle)
                child_gr, self._accept_greenlet = self._accept_greenlet, None
                child_gr.throw(SocketClosed())

        def _on_error_handle(self, socket, e):
            if self._listen_greenlet is not None:
                EventEmitter.off(self, "listen", self._on_listen_handle)
                child_gr, self._listen_greenlet = self._listen_greenlet, None
                child_gr.throw(e)
            if self._accept_greenlet is not None:
                EventEmitter.off(self, "connection", self._on_accept_handle)
                child_gr, self._accept_greenlet = self._accept_greenlet, None
                child_gr.throw(e)

        def _on_listen_handle(self, server):
            if self._listen_greenlet is None:
                return
            EventEmitter.off(self, "listen", self._on_listen_handle)
            child_gr, self._listen_greenlet = self._listen_greenlet, None
            return child_gr.switch()

        def _on_accept_handle(self, server, connection):
            if self._accept_greenlet is None:
                return
            EventEmitter.off(self, "connection", self._on_accept_handle)
            child_gr, self._accept_greenlet = self._accept_greenlet, None
            return child_gr.switch(connection)

        async def listenof(self, address, backlog=128):
            assert self._listen_greenlet is None, "already listening"
            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return
            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            self._listen_greenlet = greenlet.getcurrent()
            main = self._listen_greenlet.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "listen", self._on_listen_handle)
            self.listen(address, backlog)
            return main.switch()

        async def accept(self):
            assert self._accept_greenlet is None, "already accepting"
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            if not self._close_error_registed:
                EventEmitter.on(self, "close", self._on_close_handle)
                EventEmitter.on(self, "error", self._on_error_handle)
                self._close_error_registed = True

            self._accept_greenlet = greenlet.getcurrent()
            main = self._accept_greenlet.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "connection", self._on_accept_handle)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda server: child_gr.switch())
            self.close()
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda server: child_gr.switch())
            return main.switch()

    return PipeSocket, PipeServer
