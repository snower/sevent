import logging
from collections import defaultdict


class EventEmitter(object):
    def __init__(self):
        self._events = defaultdict(list)
        self._events_once = defaultdict(list)

    def on(self, event_name, callback):
        self._events[event_name].append(callback)

    def once(self, event_name, callback):
        self._events_once[event_name].append(callback)

    def remove_listener(self, event_name, callback):
        try:self._events[event_name].remove(callback)
        except ValueError:pass
        try:self._events_once[event_name].remove(callback)
        except ValueError:pass

    def remove_all_listeners(self, event_name=None):
        self._events[event_name] = []
        self._events_once[event_name] = []

    def emit(self, event_name, *args, **kwargs):
        callbacks=self._events[event_name]
        for cb in callbacks:
            try:cb(*args, **kwargs)
            except Exception,e:logging.exception('error when calling callback:%s',e)
        callbacks=self._events_once[event_name]
        while callbacks:
            cb=callbacks.pop()
            try:cb(*args, **kwargs)
            except Exception,e:logging.exception('error when calling callback:%s',e)
