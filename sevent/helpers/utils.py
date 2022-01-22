# -*- coding: utf-8 -*-
# 2022/1/22
# create by: snower

import sevent

def create_server(address, *args, **kwargs):
    if "pipe" in address:
        server = sevent.pipe.PipeServer()
    else:
        server = sevent.tcp.Server()
    server.enable_reuseaddr()
    server.listen(address, *args, **kwargs)
    return server

def create_socket(address):
    if "pipe" in address:
        conn = sevent.pipe.PipeSocket()
    else:
        conn = sevent.tcp.Socket()
    conn.enable_nodelay()
    return conn