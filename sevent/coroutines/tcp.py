# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower

import greenlet
from ..errors import SocketClosed
from ..event import EventEmitter, null_emit_callback

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_LISTENING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer):
    class Socket(BaseSocket):
        async def connectof(self, address, timeout=5):
            assert self.emit_connect == null_emit_callback, "already connecting"

            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_connect(socket):
                EventEmitter.off(self, "connect", on_connect)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.switch()

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                EventEmitter.off(self, "connect", on_connect)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.throw(e)

            EventEmitter.on(self, "connect", on_connect)
            EventEmitter.on(self, "close", on_close)
            EventEmitter.on(self, "error", on_error)
            self.connect(address, timeout)
            return main.switch()

        async def send(self, data):
            assert self.emit_drain == null_emit_callback, "already sending"

            if self._state == STATE_CLOSED:
                raise SocketClosed()

            if self.write(data):
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_drain(socket):
                EventEmitter.off(self, "drain", on_drain)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                if not self._events["drain"] and not self._events_once["drain"]:
                    self._has_drain_event = False
                return child_gr.switch()

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                EventEmitter.off(self, "drain", on_drain)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                if not self._events["drain"] and not self._events_once["drain"]:
                    self._has_drain_event = False
                return child_gr.throw(e)

            EventEmitter.on(self, "drain", on_drain)
            EventEmitter.on(self, "close", on_close)
            EventEmitter.on(self, "error", on_error)
            self._has_drain_event = True
            return main.switch()

        async def recv(self, size=0):
            assert self.emit_data == null_emit_callback, "already recving"

            if self._state == STATE_CLOSED:
                raise SocketClosed()

            if self._rbuffers and len(self._rbuffers) >= size:
                return self._rbuffers

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_data(socket, buffer):
                if len(buffer) < size:
                    return
                EventEmitter.off(self, "data", on_data)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.switch(buffer)

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                EventEmitter.off(self, "data", on_data)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.throw(e)

            EventEmitter.on(self, "data", on_data)
            EventEmitter.on(self, "close", on_close)
            EventEmitter.on(self, "error", on_error)
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
            if self._state not in (STATE_STREAMING, STATE_CONNECTING):
                raise SocketClosed()
            if socket._state not in (STATE_STREAMING, STATE_CONNECTING):
                raise SocketClosed()

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def do_closed(s):
                if self._state != STATE_CLOSED:
                    return
                if socket._state != STATE_CLOSED:
                    return
                child_gr.switch()

            BaseSocket.link(self, socket)
            EventEmitter.on(self, "close", do_closed)
            EventEmitter.on(socket, "close", do_closed)
            return main.switch()

        async def join(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            return main.switch()

    class Server(BaseServer):
        async def listenof(self, address, backlog=128):
            assert self.emit_listen == null_emit_callback, "already accepting"

            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_listen(server):
                EventEmitter.off(self, "listen", on_listen)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.switch()

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                EventEmitter.off(self, "listen", on_listen)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.throw(e)

            EventEmitter.on(self, "listen", on_listen)
            EventEmitter.on(self, "close", on_close)
            EventEmitter.on(self, "error", on_error)
            self.listen(address, backlog)
            return main.switch()

        async def accept(self):
            assert self.emit_connection == null_emit_callback, "already accepting"

            if self._state == STATE_CLOSED:
                raise SocketClosed()

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_connection(server, connection):
                EventEmitter.off(self, "connection", on_connection)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.switch(connection)

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                EventEmitter.off(self, "connection", on_connection)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.throw(e)

            EventEmitter.on(self, "connection", on_connection)
            EventEmitter.on(self, "close", on_close)
            EventEmitter.on(self, "error", on_error)
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

    return Socket, Server
