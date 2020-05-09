# -*- coding: utf-8 -*-

import logging
from collections import defaultdict
from .utils import is_py3


def null_emit_callback(*args, **kwargs):
    return None


class EventEmitter(object):
    def __init__(self):
        self._events = defaultdict(set)
        self._events_once = defaultdict(set)

    def on(self, event_name, callback):
        self._events[event_name].add(callback)

        if len(self._events[event_name]) == 1 and not self._events_once[event_name]:
            setattr(self, "emit_" + event_name, callback)
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def once(self, event_name, callback):
        self._events_once[event_name].add(callback)

        if not self._events[event_name] and len(self._events_once[event_name]) == 1:
            def emit_callback(*args, **kwargs):
                try:
                    return callback(*args, **kwargs)
                finally:
                    setattr(self, "emit_" + event_name, null_emit_callback)
                    self._events_once[event_name] = set()
            setattr(self, "emit_" + event_name, emit_callback)
        else:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))

    def remove_listener(self, event_name, callback):
        try:
            self._events[event_name].remove(callback)
        except KeyError:
            pass

        try:
            self._events_once[event_name].remove(callback)
        except KeyError:
            pass

        if len(self._events[event_name]) == 1 and not self._events_once[event_name]:
            setattr(self, "emit_" + event_name, list(self._events[event_name])[0])
        elif not self._events[event_name] and len(self._events_once[event_name]) == 1:
            callback = list(self._events_once[event_name])[0]

            def emit_callback(*args, **kwargs):
                try:
                    return callback(*args, **kwargs)
                finally:
                    setattr(self, "emit_" + event_name, null_emit_callback)
                    self._events_once[event_name] = set()
            setattr(self, "emit_" + event_name, emit_callback)
        elif self._events[event_name] and self._events_once[event_name]:
            setattr(self, "emit_" + event_name, self.emit_callback(event_name))
        else:
            setattr(self, "emit_" + event_name, null_emit_callback)

    def remove_all_listeners(self, event_name=None):
        if event_name is None:
            for event_name in set(list(self._events.keys()) + list(self._events_once.keys())):
                setattr(self, "emit_" + event_name, null_emit_callback)
            self._events = defaultdict(set)
            self._events_once = defaultdict(set)
        else:
            setattr(self, "emit_" + event_name, null_emit_callback)
            self._events[event_name] = set()
            self._events_once[event_name] = set()

    def emit_callback(self, event_name):
        def _(*args, **kwargs):
            for cb in self._events[event_name]:
                try:
                    cb(*args, **kwargs)
                except Exception as e:
                    logging.exception('error when calling callback:%s',e)

            callbacks = self._events_once[event_name]
            if callbacks:
                self._events_once[event_name] = set()
                while callbacks:
                    cb = callbacks.pop()
                    try:
                        cb(*args, **kwargs)
                    except Exception as e:
                        logging.exception('error when calling callback:%s',e)
        return _

    def emit(self, event_name, *args, **kwargs):
        for cb in self._events[event_name]:
            try:
                cb(*args, **kwargs)
            except Exception as e:
                logging.exception('error when calling callback:%s', e)

        callbacks = self._events_once[event_name]
        if callbacks:
            self._events_once[event_name] = set()
            while callbacks:
                cb = callbacks.pop()
                try:
                    cb(*args, **kwargs)
                except Exception as e:
                    logging.exception('error when calling callback:%s', e)

    def __getattr__(self, item):
        if item[:5] == "emit_":
            event_name = item[5:]
            if len(self._events[event_name]) == 1 and not self._events_once[event_name]:
                callback = list(self._events[event_name])[0]
                setattr(self, "emit_" + event_name, callback)
                return callback

            if not self._events[event_name] and len(self._events_once[event_name]) == 1:
                callback = list(self._events_once[event_name])[0]

                def emit_callback(*args, **kwargs):
                    try:
                        return callback(*args, **kwargs)
                    finally:
                        setattr(self, "emit_" + event_name, null_emit_callback)
                        self._events_once[event_name] = set()

                setattr(self, "emit_" + event_name, emit_callback)
                return emit_callback

            if self._events[event_name] and self._events_once[event_name]:
                callback = self.emit_callback(event_name)
                setattr(self, "emit_" + event_name, callback)
                return callback

            setattr(self, "emit_" + event_name, null_emit_callback)
            return null_emit_callback

        elif item[:3] == "on_":
            return lambda *args, **kwargs: self.on(item[3:], *args, **kwargs)
        return object.__getattribute__(self, item)


if is_py3:
    from .coroutines.event import warp_coroutine
    EventEmitter = warp_coroutine(EventEmitter)
