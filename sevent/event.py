# -*- coding: utf-8 -*-

import logging
from collections import defaultdict


class EventEmitter(object):
    def __init__(self):
        self._events = defaultdict(set)
        self._events_once = defaultdict(set)

    def on(self, event_name, callback):
        self._events[event_name].add(callback)

        if len(self._events[event_name]) == 1 and not self._events_once[event_name]:
            setattr(self, "emit_" + event_name, callback)
        else:
            setattr(self, "emit_" + event_name, self.emit)

    def once(self, event_name, callback):
        self._events_once[event_name].add(callback)

        if not self._events[event_name] and len(self._events_once[event_name]) == 1:
            def emit(*args, **kwargs):
                try:
                    return callback(*args, **kwargs)
                finally:
                    self._events_once[event_name] = set()
                    setattr(self, "emit_" + event_name, lambda *args, **kwargs: None)
            setattr(self, "emit_" + event_name, emit)
        else:
            setattr(self, "emit_" + event_name, self.emit)

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
            def emit(*args, **kwargs):
                try:
                    return callback(*args, **kwargs)
                finally:
                    self._events_once[event_name] = set()
                    setattr(self, "emit_" + event_name, lambda *args, **kwargs: None)
            setattr(self, "emit_" + event_name, emit)
        elif self._events[event_name] and self._events_once[event_name]:
            setattr(self, "emit_" + event_name, self.emit)
        else:
            setattr(self, "emit_" + event_name, lambda *args, **kwargs: None)

    def remove_all_listeners(self, event_name=None):
        if event_name is None:
            self._events = defaultdict(set)
            self._events_once = defaultdict(set)
            for event_name in set(self._events.keys() + self._events_once.keys()):
                setattr(self, "emit_" + event_name, lambda *args, **kwargs: None)
        else:
            self._events[event_name] = set()
            self._events_once[event_name] = set()
            setattr(self, "emit_" + event_name, lambda *args, **kwargs: None)

    def emit(self, event_name, *args, **kwargs):
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

    def __getattr__(self, item):
        if item[:5] == "emit_":
            event_name = item[5:]
            if len(self._events[event_name]) == 1 and not self._events_once[event_name]:
                callback = list(self._events[event_name])[0]
                setattr(self, "emit_" + event_name, callback)
                return callback

            if not self._events[event_name] and len(self._events_once[event_name]) == 1:
                callback = list(self._events_once[event_name])[0]

                def emit(*args, **kwargs):
                    try:
                        return callback(*args, **kwargs)
                    finally:
                        self._events_once[event_name] = set()
                        setattr(self, "emit_" + event_name, lambda *args, **kwargs: None)

                setattr(self, "emit_" + event_name, emit)
                return emit

            callback = lambda *args, **kwargs: None
            setattr(self, "emit_" + event_name, callback)
            return callback
        return super(EventEmitter, self).__getattr__(self, item)