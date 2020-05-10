# -*- coding: utf-8 -*-
# 2020/5/10
# create by: snower

import logging
import greenlet

_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


class CancelledError(Exception): pass


class InvalidStateError(Exception): pass


class Future(object):
    _state = _PENDING
    _result = None
    _exception = None
    _source_traceback = None
    _log_traceback = False

    def __init__(self):
        self._callbacks = []

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._callbacks)

    def cancel(self):
        self._log_traceback = False
        if self._state != _PENDING:
            return False
        self._state = _CANCELLED
        self._schedule_callbacks()
        return True

    def _schedule_callbacks(self):
        callbacks = self._callbacks[:]
        if not callbacks:
            return

        self._callbacks[:] = []
        for callback in callbacks:
            try:
                callback(self)
            except Exception as e:
                logging.exception("future schedule callback error:%s", e)

    def cancelled(self):
        return self._state == _CANCELLED

    def done(self):
        return self._state != _PENDING

    def result(self):
        if self._state == _CANCELLED:
            raise CancelledError
        if self._state != _FINISHED:
            raise InvalidStateError('Result is not ready.')
        self._log_traceback = False
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self):
        if self._state == _CANCELLED:
            raise CancelledError
        if self._state != _FINISHED:
            raise InvalidStateError('Exception is not set.')
        self._log_traceback = False
        return self._exception

    def add_done_callback(self, fn):
        if self._state != _PENDING:
            fn(self)
        else:
            self._callbacks.append(fn)

    def remove_done_callback(self, fn):
        filtered_callbacks = [f for f in self._callbacks if f != fn]
        removed_count = len(self._callbacks) - len(filtered_callbacks)
        if removed_count:
            self._callbacks[:] = filtered_callbacks
        return removed_count

    def set_result(self, result):
        if self._state != _PENDING:
            raise InvalidStateError('{}: {!r}'.format(self._state, self))
        self._result = result
        self._state = _FINISHED
        self._schedule_callbacks()

    def set_exception(self, exception):
        if self._state != _PENDING:
            raise InvalidStateError('{}: {!r}'.format(self._state, self))
        if isinstance(exception, type):
            exception = exception()
        if type(exception) is StopIteration:
            raise TypeError("StopIteration interacts badly with generators "
                            "and cannot be raised into a Future")
        self._exception = exception
        self._state = _FINISHED
        self._schedule_callbacks()

    def __next__(self):
        if not self.done():
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "must be running in async func"
            self._callbacks.append(lambda future: child_gr.switch())
            main.switch()

        result = self.result()
        e = StopIteration()
        e.value = result
        raise e

    def __iter__(self):
        return self

    __await__ = __iter__
