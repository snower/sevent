# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import greenlet
from ..errors import SocketClosed

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer, BaseWarpSocket, BaseWarpServer):
    class Socket(BaseSocket):
        _connect_greenlet = None
        _send_greenlet = None
        _recv_greenlet = None
        _close_error_registed = False
        _recv_size = 0

        def _on_close_handle(self, socket):
            if self._connect_greenlet is not None:
                self.off("connect", self._on_connect_handle)
                child_gr, self._connect_greenlet = self._connect_greenlet, None
                child_gr.throw(SocketClosed())
            if self._send_greenlet is not None:
                self.off("drain", self._on_send_handle)
                child_gr, self._send_greenlet = self._send_greenlet, None
                child_gr.throw(SocketClosed())
            if self._recv_greenlet is not None:
                self.off("data", self._on_recv_handle)
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                child_gr.throw(SocketClosed())

        def _on_error_handle(self, socket, e):
            if self._connect_greenlet is not None:
                self.off("connect", self._on_connect_handle)
                child_gr, self._connect_greenlet = self._connect_greenlet, None
                child_gr.throw(e)
            if self._send_greenlet is not None:
                self.off("drain", self._on_send_handle)
                child_gr, self._send_greenlet = self._send_greenlet, None
                child_gr.throw(e)
            if self._recv_greenlet is not None:
                self.off("data", self._on_recv_handle)
                child_gr, self._recv_greenlet = self._recv_greenlet, None
                child_gr.throw(e)

        def _on_connect_handle(self, socket):
            if self._connect_greenlet is None:
                return
            self.off("connect", self._on_connect_handle)
            if self._close_error_registed:
                self.off("close", self._on_close_handle)
                self.off("error", self._on_error_handle)
                self._close_error_registed = False
            child_gr, self._connect_greenlet = self._connect_greenlet, None
            return child_gr.switch()

        def _on_send_handle(self, socket):
            if self._send_greenlet is None:
                return
            self.off("drain", self._on_send_handle)
            child_gr, self._send_greenlet = self._send_greenlet, None
            return child_gr.switch()

        def _on_recv_handle(self, socket, buffer):
            if self._recv_greenlet is None:
                return
            if len(buffer) < self._recv_size:
                return
            self.off("data", self._on_recv_handle)
            child_gr, self._recv_greenlet = self._recv_greenlet, None
            return child_gr.switch(buffer)

        async def connectof(self, address, timeout=5):
            assert self._connect_greenlet is None, "already connecting"
            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return
            if not self._close_error_registed:
                self.on("close", self._on_close_handle)
                self.on("error", self._on_error_handle)
                self._close_error_registed = True

            self._connect_greenlet = greenlet.getcurrent()
            main = self._connect_greenlet.parent
            assert main is not None, "must be running in async func"

            self.on("connect", self._on_connect_handle)
            self.connect(address, timeout)
            return main.switch()

        async def send(self, data):
            assert self._send_greenlet is None, "already sending"
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            if not self._close_error_registed:
                self.on("close", self._on_close_handle)
                self.on("error", self._on_error_handle)
                self._close_error_registed = True

            if self.write(data):
                return

            self._send_greenlet = greenlet.getcurrent()
            main = self._send_greenlet.parent
            assert main is not None, "must be running in async func"
            self.on("drain", self._on_send_handle)
            return main.switch()

        async def recv(self, size=0):
            assert self._recv_greenlet is None, "already recving"
            if self._state == STATE_CLOSED:
                raise SocketClosed()

            if self._rbuffers and len(self._rbuffers) >= size:
                return self._rbuffers

            if not self._close_error_registed:
                self.on("close", self._on_close_handle)
                self.on("error", self._on_error_handle)
                self._close_error_registed = True

            self._recv_greenlet = greenlet.getcurrent()
            main = self._recv_greenlet.parent
            assert main is not None, "must be running in async func"

            self.on("data", self._on_recv_handle)
            self._recv_size = size
            if self._rbuffers._drain_size < size:
                drain_size, self._rbuffers._drain_size = self._rbuffers._drain_size, size
                try:
                    if self._rbuffers._full:
                        self._rbuffers.do_regain()
                    return main.switch()
                finally:
                    self._rbuffers._drain_size = drain_size
                    if self._rbuffers._len > self._rbuffers._drain_size and not self._rbuffers._full:
                        self._rbuffers.do_drain()
                    self._recv_size = 0
            try:
                if self._rbuffers._full:
                    self._rbuffers.do_regain()
                return main.switch()
            finally:
                self._recv_size = 0

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on("close", lambda socket: child_gr.switch())
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
                self.off("close", self._on_close_handle)
                self.off("error", self._on_error_handle)
                self._close_error_registed = False
            if hasattr(socket, "_close_error_registed") and socket._close_error_registed:
                socket.off("close", socket._on_close_handle)
                socket.off("error", socket._on_error_handle)
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

            self.on("close", do_closed)
            socket.on("close", do_closed)
            BaseSocket.link(self, socket)
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on("close", lambda socket: child_gr.switch())
            return main.switch()

    class Server(BaseServer):
        _listen_greenlet = None
        _accept_greenlet = None
        _close_error_registed = False

        def _on_close_handle(self, socket):
            if self._listen_greenlet is not None:
                self.off("listen", self._on_listen_handle)
                child_gr, self._listen_greenlet = self._listen_greenlet, None
                child_gr.throw(SocketClosed())
            if self._accept_greenlet is not None:
                self.off("connection", self._on_accept_handle)
                child_gr, self._accept_greenlet = self._accept_greenlet, None
                child_gr.throw(SocketClosed())

        def _on_error_handle(self, socket, e):
            if self._listen_greenlet is not None:
                self.off("listen", self._on_listen_handle)
                child_gr, self._listen_greenlet = self._listen_greenlet, None
                child_gr.throw(e)
            if self._accept_greenlet is not None:
                self.off("connection", self._on_accept_handle)
                child_gr, self._accept_greenlet = self._accept_greenlet, None
                child_gr.throw(e)

        def _on_listen_handle(self, server):
            if self._listen_greenlet is None:
                return
            self.off("listen", self._on_listen_handle)
            child_gr, self._listen_greenlet = self._listen_greenlet, None
            return child_gr.switch()

        def _on_accept_handle(self, server, connection):
            if self._accept_greenlet is None:
                return
            self.off("connection", self._on_accept_handle)
            child_gr, self._accept_greenlet = self._accept_greenlet, None
            return child_gr.switch(connection)

        async def listenof(self, address, backlog=128):
            assert self._listen_greenlet is None, "already listening"
            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return
            if not self._close_error_registed:
                self.on("close", self._on_close_handle)
                self.on("error", self._on_error_handle)
                self._close_error_registed = True

            self._listen_greenlet = greenlet.getcurrent()
            main = self._listen_greenlet.parent
            assert main is not None, "must be running in async func"

            self.on("listen", self._on_listen_handle)
            self.listen(address, backlog)
            return main.switch()

        async def accept(self):
            assert self._accept_greenlet is None, "already accepting"
            if self._state == STATE_CLOSED:
                raise SocketClosed()
            if not self._close_error_registed:
                self.on("close", self._on_close_handle)
                self.on("error", self._on_error_handle)
                self._close_error_registed = True

            self._accept_greenlet = greenlet.getcurrent()
            main = self._accept_greenlet.parent
            assert main is not None, "must be running in async func"

            self.on("connection", self._on_accept_handle)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on("close", lambda server: child_gr.switch())
            self.close()
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on("close", lambda server: child_gr.switch())
            return main.switch()


    class WarpSocket(BaseWarpSocket, Socket):
        def __init__(self, socket=None, loop=None, dns_resolver=None, max_buffer_size=None):
            BaseWarpSocket.__init__(self, socket or Socket(loop=loop, dns_resolver=dns_resolver,
                                                           max_buffer_size=max_buffer_size),
                                    loop, dns_resolver, max_buffer_size)


    class WarpServer(BaseWarpServer, Server):
        def __init__(self, socket=None, loop=None, dns_resolver=None):
            BaseWarpServer.__init__(self, socket or Server(loop=loop, dns_resolver=dns_resolver), loop, dns_resolver)

        def handshake(self, socket):
            self._loop.call_async(self.handshakeof, socket)

        async def handshakeof(self, socket):
            max_buffer_size = socket._max_buffer_size if hasattr(socket, "_max_buffer_size") else None
            self.emit_connection(self, WarpSocket(socket, loop=self._loop, max_buffer_size=max_buffer_size))


    return Socket, Server, WarpSocket, WarpServer
