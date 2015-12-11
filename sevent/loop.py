# -*- coding: utf-8 -*-

import select
import time
import heapq
import logging
from collections import defaultdict, deque

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


class TimeoutHandler(object):
    def __init__(self, callback, deadline, args, kwargs):
        '''deadline here is absolute timestamp'''
        self.callback = callback
        self.deadline = deadline
        self.args=args
        self.kwargs=kwargs

    def __cmp__(self, other):
        return cmp(self.deadline, other.deadline)

    def __call__(self):
        self.callback(*self.args, **self.kwargs)


class SSLoop(object):
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
        if not isinstance(fd, (int, long)):
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
        if not isinstance(fd, (int, long)):
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
        if not isinstance(fd, (int, long)):
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
                    if handler.deadline <= cur_time:
                        heapq.heappop(self._timeout_handlers)
                        try:
                            handler()
                        except Exception, e:
                            logging.exception("loop callback error:%s", e)
                    else:
                        timeout = self._timeout_handlers[0].deadline - time.time()
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
                        except Exception, e:
                            logging.exception("loop callback error:%s", e)

            # call handlers without fd
            self._handlers, self._run_handlers = self._run_handlers, self._handlers
            for _ in xrange(len(self._run_handlers)):
                callback, args, kwargs = self._run_handlers.popleft()
                try:
                    callback(*args, **kwargs)
                except Exception, e:
                    logging.exception("loop callback error:%s", e)

    def stop(self):
        self._stopped = True

    def async(self, callback, *args, **kwargs):
        self._handlers.append((callback, args, kwargs))

    def timeout(self, timeout, callback, *args, **kwargs):
        handler = TimeoutHandler(callback, time.time() + timeout, args, kwargs)
        heapq.heappush(self._timeout_handlers, handler)
        return handler

    def cancel_timeout(self, handler):
        self._timeout_handlers.remove(handler)
