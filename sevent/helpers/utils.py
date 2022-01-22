# -*- coding: utf-8 -*-
# 2022/1/22
# create by: snower

import struct
import signal
import socket
import sevent

def config_signal():
    signal.signal(signal.SIGINT, lambda signum, frame: sevent.current().stop())
    signal.signal(signal.SIGTERM, lambda signum, frame: sevent.current().stop())

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

def format_data_len(date_len):
    if date_len < 1024:
        return "%dB" % date_len
    elif date_len < 1024*1024:
        return "%.3fK" % (date_len/1024.0)
    elif date_len < 1024*1024*1024:
        return "%.3fM" % (date_len/(1024.0*1024.0))
    elif date_len < 1024*1024*1024*1024:
        return "%.3fG" % (date_len/(1024.0*1024.0*1024.0))
    return "%.3fT" % (date_len/(1024.0*1024.0*1024.0*1024.0))

def is_subnet(ip, subnet):
    try:
        ip = struct.unpack("!I", socket.inet_pton(socket.AF_INET, ip))[0]
        if isinstance(subnet[0], tuple) or isinstance(subnet[1], tuple):
            return False
        return (ip & subnet[1]) == (subnet[0] & subnet[1])
    except:
        ip = (struct.unpack("!QQ", socket.inet_pton(socket.AF_INET6, ip)))
        if not isinstance(subnet[0], tuple) or len(subnet[0]) != 2 or not isinstance(subnet[1], tuple) or len(subnet[1]) != 2:
            return False
        return ((ip[0] & subnet[1][0]) == (subnet[0][0] & subnet[1][0])) and ((ip[1] & subnet[1][1]) == (subnet[0][1] & subnet[1][1]))