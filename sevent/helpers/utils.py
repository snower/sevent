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
        if isinstance(address, (tuple, list)):
            pipe_address = "pipe#%s" % (address[1] if len(address) >= 2 else address[-1])
        elif not isinstance(address, str):
            pipe_address = "pipe#%s" % address
        else:
            pipe_address = address
        if pipe_address in sevent.pipe.PipeServer._bind_servers:
            conn = sevent.pipe.PipeSocket()
        else:
            conn = sevent.tcp.Socket()
    else:
        conn = sevent.tcp.Socket()
    conn.enable_nodelay()
    return conn