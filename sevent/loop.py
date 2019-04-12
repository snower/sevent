# -*- coding: utf-8 -*-

import select
import time
import heapq
import logging
import threading
from collections import defaultdict, deque
from .utils import is_int

''' You can only use instance(). Don't create a Loop() '''

_ioloop_lock = threading.RLock()
_ioloop_cls = None
_ioloop = None


def instance():
    global _ioloop_cls, _ioloop
    if _ioloop is not None:
        return _ioloop

    with _ioloop_lock:
        if _ioloop is not None:
            return _ioloop
        else:
            if 'epoll' in select.__dict__:
                from .impl import epoll_loop
                logging.debug('using epoll')
                _ioloop_cls = epoll_loop.EpollLoop
            elif 'kqueue' in select.__dict__:
                from .impl import kqueue_loop
                logging.debug('using kqueue')
                _ioloop_cls = kqueue_loop.KqueueLoop
            else:
                from .impl import select_loop
                logging.debug('using select')
                _ioloop_cls = select_loop.SelectLoop

            _ioloop = _ioloop_cls()
            return _ioloop


def current():
    return _ioloop


# these values are defined as the same as poll
MODE_NULL = 0x00
MODE_IN = 0x01
MODE_OUT = 0x04
MODE_ERR = 0x08
MODE_HUP = 0x10
MODE_NVAL = 0x20


class TimeoutHandler(object):
    def __init__(self, callback, deadline, args, kwargs):
        '''deadline here is absolute timestamp'''
        self.callback = callback
        self.deadline = deadline
        self.args=args
        self.kwargs=kwargs
        self.canceled = False

    def __cmp__(self, other):
        return (self.deadline > other.deadline) - (self.deadline < other.deadline)

    def __eq__(self, other):
        return self.deadline == other.deadline

    def __gt__(self, other):
        return self.deadline > other.deadline

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __ge__(self, other):
        return self.deadline >= other.deadline

    def __le__(self, other):
        return self.deadline <= other.deadline

    def __ne__(self, other):
        return self.deadline != other.deadline

    def __call__(self):
        self.callback(*self.args, **self.kwargs)


class IOLoop(object):
    def __init__(self):
        self._handlers = deque()
        self._run_handlers = deque()
        self._timeout_handlers = []
        self._fd_handlers = defaultdict(list)
        self._stopped = False

    def _poll(self, timeout):
        raise NotImplementedError()

    def _add_fd(self, fd, mode):
        raise NotImplementedError()

    def _remove_fd(self, fd):
        raise NotImplementedError()

    def _modify_fd(self, fd, mode):
        raise NotImplementedError()

    def add_fd(self, fd, mode, callback):
        if not is_int(fd):
            try:
                fd = fd.fileno()
            except:
                return False
        handlers = self._fd_handlers[fd]
        handlers.append((callback, fd, mode))
        if len(handlers) == 1:
            self._add_fd(fd, mode)
        else:
            new_mode = MODE_NULL
            for hcallback, hfd, hmode in handlers:
                new_mode |= hmode
            self._modify_fd(fd, new_mode)
        return True

    def update_fd(self, fd, mode, callback):
        if not is_int(fd):
            try:
                fd = fd.fileno()
            except:
                return False

        handlers = self._fd_handlers[fd]
        if not handlers:
            return False
        new_handlers = []
        new_mode = MODE_NULL
        for hcallback, hfd, hmode in handlers:
            if hcallback == callback:
                new_mode |= mode
                new_handlers.append((hcallback, hfd, mode))
            else:
                new_mode |= hmode
                new_handlers.append((hcallback, hfd, hmode))
        self._modify_fd(fd, new_mode)
        self._fd_handlers[fd] = new_handlers
        return True

    def remove_fd(self, fd, callback):
        if not is_int(fd):
            try:
                fd = fd.fileno()
            except:
                return False

        handlers = self._fd_handlers[fd]
        if not handlers:
            return False
        if len(handlers) == 1:
            if handlers[0][0] == callback:
                self._remove_fd(fd)
                del self._fd_handlers[fd]
        else:
            new_handlers = []
            new_mode = MODE_NULL
            for hcallback, hfd, hmode in handlers:
                if hcallback != callback:
                    new_mode |= hmode
                    new_handlers.append((hcallback, hfd, hmode))
            self._modify_fd(fd, new_mode)
            self._fd_handlers[fd] = new_handlers
        return True

    def start(self):
        while not self._stopped:
            timeout = 1
            if self._timeout_handlers:
                cur_time = time.time()
                while self._timeout_handlers:
                    handler = self._timeout_handlers[0]
                    if handler.canceled:
                        heapq.heappop(self._timeout_handlers)
                    elif handler.deadline <= cur_time:
                        heapq.heappop(self._timeout_handlers)
                        try:
                            handler.callback(*handler.args, **handler.kwargs)
                        except Exception as e:
                            logging.exception("loop callback timeout error:%s", e)
                    else:
                        timeout = self._timeout_handlers[0].deadline - cur_time
                        if timeout > 1:
                            timeout = 1
                        break

            if self._handlers:
                timeout = 0
                
            fds_ready = self._poll(timeout)
            for fd, mode in fds_ready:
                handlers = self._fd_handlers[fd]
                for hcallback, hfd, hmode in handlers:
                    if hmode & mode != 0:
                        try:
                            hcallback()
                        except Exception as e:
                            logging.exception("loop callback error:%s", e)

            # call handlers without fd
            self._handlers, self._run_handlers = self._run_handlers, self._handlers
            while self._run_handlers:
                callback, args, kwargs = self._run_handlers.popleft()
                try:
                    callback(*args, **kwargs)
                except Exception  as e:
                    logging.exception("loop callback error:%s", e)

    def stop(self):
        self._stopped = True

    def add_async(self, callback, *args, **kwargs):
        self._handlers.append((callback, args, kwargs))

    def add_timeout(self, timeout, callback, *args, **kwargs):
        handler = TimeoutHandler(callback, time.time() + timeout, args, kwargs)
        heapq.heappush(self._timeout_handlers, handler)
        return handler

    def cancel_timeout(self, handler):
        if handler.__class__ == TimeoutHandler:
            handler.canceled = True
        else:
            self._timeout_handlers.remove(handler)