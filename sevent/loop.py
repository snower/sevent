# -*- coding: utf-8 -*-

import select
import time
import bisect
import logging
from collections import defaultdict

''' You can only use instance(). Don't create a Loop() '''

_ssloop_cls = None
_ssloop = None


def instance():
    global _ssloop
    if _ssloop is not None:
        return _ssloop
    else:
        init()
        _ssloop = _ssloop_cls()
        return _ssloop

def current():
    return _ssloop


def init():
    global _ssloop_cls

    if 'epoll' in select.__dict__:
        import impl.epoll_loop
        logging.debug('using epoll')
        _ssloop_cls = impl.epoll_loop.EpollLoop
    elif 'kqueue' in select.__dict__:
        import impl.kqueue_loop
        logging.debug('using kqueue')
        _ssloop_cls = impl.kqueue_loop.KqueueLoop
    else:
        import impl.select_loop
        logging.debug('using select')
        _ssloop_cls = impl.select_loop.SelectLoop


# these values are defined as the same as poll
MODE_NULL = 0x00
MODE_IN = 0x01
MODE_OUT = 0x04
MODE_ERR = 0x08
MODE_HUP = 0x10
MODE_NVAL = 0x20


class Handler(object):
    def __init__(self, callback, fd=None, mode=None, deadline=None, error=None,args=(),kwargs={}):
        '''deadline here is absolute timestamp'''
        self.callback = callback
        self.fd = fd
        self.mode = mode
        self.deadline = deadline
        self.error = error  # a message describing the error
        self.args=args
        self.kwargs=kwargs

    def __cmp__(self, other):
        if self.deadline and other.deadline:
            return cmp(self.deadline,other.deadline)
        return cmp(id(self),id(other))

    def __call__(self):
        self.callback(*self.args,**self.kwargs)


class SSLoop(object):
    def __init__(self):
        self._handlers = []
        self._timeout_handlers = []
        self._fd_handlers = defaultdict(list)
        self._stopped = False
        self._on_error = None

    def time(self):
        return time.time()

    def _poll(self, timeout):
        '''timeout here is timespan, -1 means forever'''
        raise NotImplementedError()

    def _add_fd(self, fd, mode):
        raise NotImplementedError()

    def _remove_fd(self, fd):
        raise NotImplementedError()

    def _modify_fd(self, fd, mode):
        raise NotImplementedError()

    def _call_handler(self, handler):
        try:
            handler()
        except Exception,e:
            if self._on_error is not None and callable(self._on_error):
                self._on_error(e)
            logging.exception("loop callback error:%s",e)

    def _get_fd_mode(self, fd):
        mode = MODE_NULL
        handlers = self._fd_handlers[fd]
        if not handlers:return None
        for handler in handlers:
            mode |= handler.mode
        return mode

    def _update_fd(self, fd):
        mode = self._get_fd_mode(fd)
        if mode is not None:self._modify_fd(fd, mode)

    def start(self):
        while not self._stopped:
            cur_time = self.time()
            while len(self._timeout_handlers) > 0:
                handler = self._timeout_handlers[0]
                if handler.deadline <= cur_time:
                    self._timeout_handlers.pop(0)
                    self._call_handler(handler)
                else:
                    break

            timeout = 0 if self._handlers else (self._timeout_handlers[0].deadline - self.time() if len(self._timeout_handlers) else 0.5)
            fds_ready = self._poll(timeout)
            for fd, mode in fds_ready:
                handlers = self._fd_handlers[fd]
                for handler in handlers:
                    if handler.mode & mode != 0:
                        self._call_handler(handler)

            # call handlers without fd
            handlers = self._handlers
            self._handlers = []
            for handler in handlers:
                self._call_handler(handler)

    def stop(self):
        self._stopped = True

    def sync(self, callback,*args,**kwargs):
        handler = Handler(callback,args=args,kwargs=kwargs)
        self._handlers.append(handler)
        return handler

    def timeout(self, timeout, callback,*args,**kwargs):
        handler = Handler(callback, deadline=self.time() + timeout,args=args,kwargs=kwargs)
        bisect.insort(self._timeout_handlers, handler)
        return handler

    def add_fd(self, fd, mode, callback):
        if not (isinstance(fd, int) or isinstance(fd, long)):
            try:
                fd = fd.fileno()
            except:
                return False
        handler = Handler(callback, fd=fd, mode=mode)
        handlers = self._fd_handlers[fd]
        handlers.append(handler)
        if len(handlers) == 1:
            self._add_fd(fd, mode)
        else:
            self._update_fd(fd)
        return handler

    def update_handler_mode(self, handler, mode):
        handler.mode = mode
        self._update_fd(handler.fd)

    def remove_handler(self, handler):
        if handler.deadline:
            self._timeout_handlers.remove(handler)
        elif handler.fd:
            fd = handler.fd
            handlers = self._fd_handlers[fd]
            handlers.remove(handler)
            if not handlers:
                self._remove_fd(fd)
                del self._fd_handlers[fd]
            else:
                self._update_fd(fd)
        else:
            self._handlers.remove(handler)
