# -*- coding: utf-8 -*-
# 2020/5/8
# create by: snower


import greenlet
from ..errors import SocketClosed
from ..event import null_emit_callback

STATE_INITIALIZED = 0x01
STATE_CONNECTING = 0x02
STATE_STREAMING = 0x04
STATE_BINDING = 0x08
STATE_CLOSING = 0x10
STATE_CLOSED = 0x20


def warp_coroutine(BaseSocket):
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

            emit_drain = self.emit_drain
            emit_close = self.emit_close
            emit_error = self.emit_error

            def on_drain(socket):
                if self.emit_drain != on_drain:
                    return

                self.emit_drain = emit_drain
                self.emit_close = emit_close
                self.emit_error = emit_error
                self._has_drain_event = False
                return child_gr.switch()

            def on_close(socket):
                if self.emit_close != on_close:
                    return

                self.emit_drain = emit_drain
                self.emit_close = emit_close
                self.emit_error = emit_error
                self._has_drain_event = False
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                if self.emit_error != on_error:
                    return

                self.emit_drain = emit_drain
                self.emit_close = emit_close
                self.emit_error = emit_error
                self._has_drain_event = False
                return child_gr.throw(e)

            setattr(self, "emit_drain", on_drain)
            setattr(self, "emit_close", on_close)
            setattr(self, "emit_error", on_error)
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

            emit_data = self.emit_data
            emit_close = self.emit_close
            emit_error = self.emit_error

            def on_data(socket, buffer):
                if len(buffer) < size:
                    return

                if self.emit_data != on_data:
                    return

                self.emit_data = emit_data
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.switch(buffer)

            def on_close(socket):
                if self.emit_close != on_close:
                    return

                self.emit_data = emit_data
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(SocketClosed())

            def on_error(socker, e):
                if self.emit_error != on_error:
                    return

                self.emit_data = emit_data
                self.emit_close = emit_close
                self.emit_error = emit_error
                return child_gr.throw(e)

            setattr(self, "emit_data", on_data)
            setattr(self, "emit_close", on_close)
            setattr(self, "emit_error", on_error)
            return main.switch()

        async def closeof(self):
            if self._state == STATE_CLOSED:
                return

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"

            self.on_close(lambda socket: child_gr.switch())
            return main.switch()

    return Socket
