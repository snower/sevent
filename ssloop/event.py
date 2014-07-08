import logging
from collections import defaultdict


class EventEmitter(object):
    def __init__(self):
        self._events = defaultdict(dict)
        self._events_once = defaultdict(dict)

    def on(self, event_name, callback):
        self._events[event_name][id(callback)]=callback

    def once(self, event_name, callback):
        self._events_once[event_name][id(callback)]=callback

    def remove_listener(self, event_name, callback):
        if event_name in self._events and id(callback) in self._events[event_name]:
            del self._events[event_name][id(callback)]
        if event_name in self._events_once and id(callback) in self._events_once[event_name]:
            del self._events_once[event_name][id(callback)]

    def remove_all_listeners(self, event_name=None):
        self._events[event_name] = {}
        self._events_once[event_name] = {}

    def emit(self, event_name, *args, **kwargs):
        callbacks=self._events[event_name].values()
        for cb in callbacks:
            try:
                cb(*args, **kwargs)
            except Exception,e:
                logging.exception('error when calling callback:%s',e)

        callbacks=self._events_once[event_name].values()
        self._events_once[event_name]={}
        while callbacks:
            cb=callbacks.pop()
            try:
                cb(*args, **kwargs)
            except Exception,e:
                logging.exception('error when calling callback:%s',e)
