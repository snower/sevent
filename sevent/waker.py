# -*- coding: utf-8 -*-
# 20/2/14
# create by: snower

import os
import socket


def set_close_exec(fd):
    import fcntl
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


def _set_nonblocking(fd):
    import fcntl
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


class PipeWaker(object):
    def __init__(self):
        r, w = os.pipe()
        _set_nonblocking(r)
        _set_nonblocking(w)
        set_close_exec(r)
        set_close_exec(w)
        self.reader = os.fdopen(r, "rb", 0)
        self.writer = os.fdopen(w, "wb", 0)

    def fileno(self):
        return self.reader.fileno()

    def wake(self):
        try:
            self.writer.write(b"x")
        except (IOError, ValueError):
            pass

    def consume(self):
        try:
            while True:
                result = self.reader.read()
                if not result:
                    break
        except IOError:
            pass

    def close(self):
        self.reader.close()
        try:
            self.writer.close()
        except Exception:
            pass


class SocketWaker(object):
    def __init__(self):
        self.writer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.writer.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        count = 0
        while 1:
            count += 1
            a = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            a.bind(("127.0.0.1", 0))
            a.listen(1)
            connect_address = a.getsockname()
            try:
                self.writer.connect(connect_address)
                break
            except socket.error:
                if count >= 10:
                    a.close()
                    self.writer.close()
                    raise socket.error("Cannot bind trigger!")
                a.close()

        self.reader, addr = a.accept()
        self.reader.setblocking(0)
        self.writer.setblocking(0)
        a.close()

    def fileno(self):
        return self.reader.fileno()

    def wake(self):
        try:
            self.writer.send(b"x")
        except (IOError, socket.error, ValueError):
            pass

    def consume(self):
        try:
            while True:
                result = self.reader.recv(1024)
                if not result:
                    break
        except (IOError, socket.error):
            pass

    def close(self):
        self.reader.close()
        try:
            self.writer.close()
        except Exception:
            pass


def Waker():
    if os.name == 'nt':
        return SocketWaker()
    try:
        return PipeWaker()
    except:
        return SocketWaker()