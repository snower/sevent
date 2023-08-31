# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower


import greenlet
from ..errors import SocketClosed
from ..event import EventEmitter, null_emit_callback

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_BINDING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket, BaseServer):
    class Socket(BaseSocket):
        async def sendto(self, data):
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

        async def recvfrom(self, size=0):
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

        @classmethod
        async def linkof(cls, socket, address, timeout=900):
            if socket._state == STATE_CLOSED:
                raise SocketClosed()

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

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            return main.switch()

    class Server(BaseServer):
        async def bindof(self, address):
            assert self.emit_bind == null_emit_callback, "already accepting"

            if self._state != STATE_INITIALIZED:
                if self._state == STATE_CLOSED:
                    raise SocketClosed()
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            def on_bind(server):
                EventEmitter.off(self, "bind", on_bind)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.switch()

            def on_close(socket):
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                EventEmitter.off(self, "bind", on_bind)
                EventEmitter.off(self, "close", on_close)
                EventEmitter.off(self, "error", on_error)
                return child_gr.throw(e)

            EventEmitter.on(self, "bind", on_bind)
            EventEmitter.on(self, "close", on_close)
            EventEmitter.on(self, "error", on_error)
            self.bind(address)
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

        async def join(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            EventEmitter.on(self, "close", lambda socket: child_gr.switch())
            return main.switch()

    return Socket, Server
