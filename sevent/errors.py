# -*- coding: utf-8 -*-
# 2020/1/16
# create by: snower


class SeventException(Exception):
    pass


class SocketClosed(SeventException):
    pass


class ResolveError(SeventException):
    pass


class ConnectTimeout(SeventException):
    pass


class AddressError(SeventException):
    pass


class ConnectError(SeventException):
    def __init__(self, address, socket_error, *args, **kwargs):
        super(ConnectError, self).__init__(*args, **kwargs)

        self.address = address
        self.socket_error = socket_error
