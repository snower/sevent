# -*- coding: utf-8 -*-

import select
import time
import bisect
import threading
from collections import defaultdict
from .waker import Waker
from .utils import is_py3, get_logger

''' You can only use instance(). Don't create a Loop() '''

_thread_local = threading.local()
_thread_local._sevent_ioloop = None
_ioloop_lock = threading.RLock()
_ioloop_cls = None
_ioloop = None
_mul_ioloop = False


def instance():
    global _ioloop_cls, _ioloop, _mul_ioloop
    if _thread_local._sevent_ioloop is not None:
        return _thread_local._sevent_ioloop

    with _ioloop_lock:
        if _thread_local._sevent_ioloop is not None:
            return _thread_local._sevent_ioloop

        if 'epoll' in select.__dict__:
            from .impl import epoll_loop
            get_logger().debug('using epoll')
            _ioloop_cls = epoll_loop.EpollLoop
        elif 'kqueue' in select.__dict__:
            from .impl import kqueue_loop
            get_logger().debug('using kqueue')
            _ioloop_cls = kqueue_loop.KqueueLoop
        else:
            from .impl import select_loop
            get_logger().debug('using select')
            _ioloop_cls = select_loop.SelectLoop

        _thread_local._sevent_ioloop = _ioloop_cls()
        if _ioloop is None:
            _ioloop = _thread_local._sevent_ioloop
        else:
            _mul_ioloop = True
        return _thread_local._sevent_ioloop


def current():
    if not _mul_ioloop:
        return _ioloop
    return _thread_local._sevent_ioloop


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
        self.args = args
        self.kwargs = kwargs
        self.canceled = False

    def __cmp__(self, other):
        return cmp(self.deadline, other.deadline)

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
        self._handlers = []
        self._run_handlers = []
        self._timeout_handlers = []
        self._fd_handlers = defaultdict(list)
        self._stopped = False
        self._waker = Waker()

    def _poll(self, timeout):
        raise NotImplementedError()

    def _add_fd(self, fd, mode):
        raise NotImplementedError()

    def _remove_fd(self, fd):
        raise NotImplementedError()

    def _modify_fd(self, fd, mode):
        raise NotImplementedError()

    def add_fd(self, fd, mode, callback):
        handlers = self._fd_handlers[fd]
        new_handlers = []
        if not handlers:
            new_handlers.append((callback, fd, mode))
            self._add_fd(fd, mode)
        else:
            new_mode = MODE_NULL
            for hcallback, hfd, hmode in handlers:
                if hcallback != callback:
                    new_mode |= hmode
                    new_handlers.append((hcallback, hfd, hmode))
            new_handlers.append((callback, fd, mode))
            new_mode |= mode
            self._modify_fd(fd, new_mode)
        self._fd_handlers[fd] = new_handlers
        return True

    def update_fd(self, fd, mode, callback):
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

    def clear_fd(self, fd):
        if fd in self._fd_handlers:
            if self._fd_handlers[fd]:
                del self._fd_handlers[fd]
                self._remove_fd(fd)
                return True
            del self._fd_handlers[fd]
        return False

    def start(self):
        self.add_fd(self._waker.fileno(), MODE_IN, self._waker.consume)

        while not self._stopped:
            timeout = 3600

            if self._timeout_handlers:
                cur_time = time.time()
                if self._timeout_handlers[0].deadline <= cur_time:
                    while self._timeout_handlers:
                        handler = self._timeout_handlers[0]
                        if handler.canceled:
                            self._timeout_handlers.pop(0)
                        elif handler.deadline <= cur_time:
                            self._timeout_handlers.pop(0)
                            try:
                                handler.callback(*handler.args, **handler.kwargs)
                            except Exception as e:
                                get_logger().exception("loop callback timeout error:%s", e)
                        elif self._handlers:
                            timeout = 0
                            break
                        else:
                            timeout = self._timeout_handlers[0].deadline - cur_time
                            break
                elif self._handlers:
                    timeout = 0
                else:
                    timeout = self._timeout_handlers[0].deadline - cur_time
            elif self._handlers:
                timeout = 0

            fds_ready = self._poll(timeout)
            for fd, mode in fds_ready:
                for hcallback, hfd, hmode in self._fd_handlers[fd]:
                    if hmode & mode != 0:
                        try:
                            hcallback()
                        except Exception as e:
                            get_logger().exception("loop callback error:%s", e)

            # call handlers without fd
            self._handlers, self._run_handlers = self._run_handlers, self._handlers
            for callback, args, kwargs in self._run_handlers:
                try:
                    callback(*args, **kwargs)
                except Exception as e:
                    get_logger().exception("loop callback error:%s", e)
            self._run_handlers = []

    def stop(self):
        self._stopped = True
        self._waker.wake()

    def add_async(self, callback, *args, **kwargs):
        self._handlers.append((callback, args, kwargs))

    def add_async_safe(self, callback, *args, **kwargs):
        self._handlers.append((callback, args, kwargs))
        self._waker.wake()

    def add_timeout(self, timeout, callback, *args, **kwargs):
        handler = TimeoutHandler(callback, time.time() + timeout, args, kwargs)
        if not self._timeout_handlers or handler.deadline >= self._timeout_handlers[-1].deadline:
            self._timeout_handlers.append(handler)
        else:
            bisect.insort(self._timeout_handlers, handler)
        return handler

    def cancel_timeout(self, handler):
        if handler.__class__ == TimeoutHandler:
            handler.callback = None
            handler.args = None
            handler.kwargs = None
            handler.canceled = True
        else:
            try:
                self._timeout_handlers.remove(handler)
            except ValueError:
                pass

        while self._timeout_handlers:
            if not self._timeout_handlers[0].canceled:
                break
            self._timeout_handlers.pop(0)

    def wakeup(self, *args, **kwargs):
        if args and callable(args[0]):
            self.add_async(args[0], *args[1:], **kwargs)
        self._waker.wake()


if is_py3:
    from .coroutines.loop import warp_coroutine
    IOLoop = warp_coroutine(IOLoop)
