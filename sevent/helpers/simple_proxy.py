# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import sys
import time
import re
import struct
import socket
import argparse
import logging
import traceback
import threading
import sevent
from .utils import create_server, create_socket, config_signal, format_data_len

def parse_allow_deny_host(host_name):
    try:
        if ":" in host_name[host_name.index("]") + 1]:
            host_name = host_name.replace("[", "\[").replace("]", "\]").replace(".", "\.").replace("*", ".+?")
        else:
            host_name = host_name.replace("[", "\[").replace("]", "\]").replace(".", "\.").replace("*", ".*?") + ":.*?"
    except:
        if ":" in host_name:
            host_name = host_name.replace("[", "\[").replace("]", "\]").replace(".", "\.").replace("*", ".+?")
        else:
            host_name = host_name.replace("[", "\[").replace("]", "\]").replace(".", "\.").replace("*", ".*?") + ":.*?"
    return re.compile("^" + host_name + "$")

def parse_allow_deny_host_filename(filename):
    host_names = set([])
    with open(filename, "r", encoding="utf-8") as fp:
        for line in fp:
            host_names.add(parse_allow_deny_host(line.strip()))
    return host_names

def check_allow_deny_domain(allow_host_name_res, deny_host_name_res, address):
    if not allow_host_name_res and not deny_host_name_res:
        return True
    check_host_name = "%s:%d" % (address[0], address[1])
    if allow_host_name_res:
        for allow_host_name_re in allow_host_name_res:
            if allow_host_name_re.match(check_host_name):
                return True
        return False
    if deny_host_name_res:
        for deny_host_name_re in deny_host_name_res:
            if deny_host_name_re.match(check_host_name):
                return False
        return True
    return True

def warp_write(conn, status, key):
    origin_write = conn.write
    def _(data):
        status[key] += len(data)
        return origin_write(data)
    return _

def http_protocol_parse_address(data, default_port=80):
    address = data.split(b":")
    if len(address) != 2:
        return address[0].decode("utf-8"), default_port
    return address[0].decode("utf-8"), int(address[1].decode("utf-8"))

async def http_protocol_parse(conn, buffer, allow_host_names=None, deny_host_names=None):
    data = buffer.read()
    index = data.find(b" ")
    method = data[:index].decode("utf-8")
    data = data[index + 1:]

    if method.lower() == "connect":
        index = data.find(b" ")
        host, port = http_protocol_parse_address(data[:index], 443)
        if not check_allow_deny_domain(allow_host_names, deny_host_names, (host, port)):
            await conn.send(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            return False, host, port, b''
        await conn.send(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        return True, host, port, b''

    if method.lower() in ("get", "post", "put", "options", "head", "delete", "patch"):
        data = data[7:]
        index = data.find(b"/")
        host, port = http_protocol_parse_address(data[:index], 80)
        if not check_allow_deny_domain(allow_host_names, deny_host_names, (host, port)):
            await conn.send(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            return False, host, port, b''
        return True, host, port, method.encode("utf-8") + b' ' + data[index:]
    return False, '', 0, None

async def socks5_protocol_parse(conn, buffer, allow_host_names=None, deny_host_names=None):
    buffer.read()
    await conn.send(b'\x05\00')
    buffer = await conn.recv()
    data = buffer.read()
    if data[1] != 1:
        return False, '', 0, None

    if data[3] == 1:
        host, port = socket.inet_ntoa(data[4:8]), struct.unpack('>H', data[8:10])[0]
        if not check_allow_deny_domain(allow_host_names, deny_host_names, (host, port)):
            await conn.send(b"".join([b'\x05\x02\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
            return False, host, port, data[10:]
        await conn.send(b"".join([b'\x05\x00\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
        return True, host, port, data[10:]

    if data[3] == 4:
        host, port = socket.inet_ntop(socket.AF_INET6, data[4:20]), struct.unpack('>H', data[20:22])[0]
        if not check_allow_deny_domain(allow_host_names, deny_host_names, (host, port)):
            await conn.send(b"".join([b'\x05\x02\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
            return False, host, port, data[22:]
        await conn.send(b"".join([b'\x05\x00\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
        return True, host, port, data[22:]

    elif data[3] == 3:
        host_len = data[4] + 5
        host, port = data[5: host_len].decode("utf-8"), struct.unpack('>H', data[host_len: host_len+2])[0]
        if not check_allow_deny_domain(allow_host_names, deny_host_names, (host, port)):
            await conn.send(b"".join([b'\x05\x02\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
            return False, host, port, data[host_len + 2:]
        await conn.send(b"".join([b'\x05\x00\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
        return True, host, port, data[host_len + 2:]
    return False, '', 0, None

async def tcp_proxy(conns, server, conn, status, allow_host_names=None, deny_host_names=None):
    start_time = time.time()
    host, port, protocol = '', 0, ''
    conn.write, pconn = warp_write(conn, status, "recv_len"), None
    try:
        conn.enable_nodelay()
        buffer = await conn.recv()
        if buffer[0] == 5:
            protocol = 'socks5'
            is_allowed, host, port, data = await socks5_protocol_parse(conn, buffer, allow_host_names, deny_host_names)
        else:
            protocol = 'http'
            is_allowed, host, port, data = await http_protocol_parse(conn, buffer, allow_host_names, deny_host_names)
        if not host or not port:
            logging.info("connected %s:%s -> %s:%s empty address",
                         conn.address[0], conn.address[1], server.address[0], server.address[1])
            return
        if not is_allowed:
            logging.info("connected %s %s:%s -> %s:%s -> %s:%d not allow", protocol,
                         conn.address[0], conn.address[1], server.address[0], server.address[1], host, port)
            return

        logging.info("connected %s %s:%s -> %s:%s -> %s:%d", protocol,
                     conn.address[0], conn.address[1], server.address[0], server.address[1], host, port)
        pconn = create_socket((host, port))
        pconn.write = warp_write(pconn, status, "send_len")
        await pconn.connectof((host, port))
        if data:
            await pconn.send(data)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("error %s %s:%s -> %s:%s -> %s:%d %s %.2fms\r%s", protocol,
                     conn.address[0], conn.address[1], server.address[0], server.address[1], host, port, e,
                     (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("closed %s %s:%s -> %s:%s -> %s:%d %s %s %.2fms", protocol,
                 conn.address[0], conn.address[1], server.address[0], server.address[1], host, port, format_data_len(status["send_len"]),
                 format_data_len(status["recv_len"]), (time.time() - start_time) * 1000)

async def check_timeout(conns, timeout):
    def run_check():
        while True:
            try:
                now = time.time()
                for conn_id, (conn, status) in list(conns.items()):
                    if status['check_recv_len'] != status['recv_len'] or status['check_send_len'] != status['send_len']:
                        status["check_recv_len"] = status["recv_len"]
                        status["check_send_len"] = status["send_len"]
                        status['last_time'] = now
                        continue

                    if now - status['last_time'] >= timeout:
                        sevent.current().add_async_safe(conn.close)
                        conns.pop(conn_id, None)
            finally:
                time.sleep(min(float(timeout) / 2.0, 30))

    if timeout > 0:
        check_thread = threading.Thread(target=run_check)
        check_thread.daemon = True
        check_thread.start()
    await sevent.Future()

async def tcp_accept(server, timeout, allow_host_names=None, deny_host_names=None):
    conns = {}
    sevent.current().call_async(check_timeout, conns, timeout)
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        sevent.current().call_async(tcp_proxy, conns, server, conn, status, allow_host_names, deny_host_names)
        conns[id(conn)] = (conn, status)

def main(argv):
    parser = argparse.ArgumentParser(description='simple http and socks5 proxy server')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='local bind host (default: 0.0.0.0)')
    parser.add_argument('-p', dest='port', default=8088, type=int, help='local bind port (default: 8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-a', dest='allow_hosts', default=[], action="append", type=str,help='allow forward host name')
    parser.add_argument('-d', dest='deny_hosts', default=[], action="append", type=str, help='deny forward host name')
    parser.add_argument('-A', dest='allow_filename', default="", type=str, help='allow forward host name config filename')
    parser.add_argument('-D', dest='deny_filename', default="", type=str, help='deny forward host name config filename')
    args = parser.parse_args(args=argv)
    config_signal()

    allow_host_names, deny_host_names = [], []
    if args.allow_hosts:
        for allow_host in args.allow_hosts:
            allow_host_names.extend([parse_allow_deny_host(h) for h in allow_host.split(",") if h.strip()])
    if args.deny_hosts:
        for deny_host in args.deny_hosts:
            deny_host_names.extend([parse_allow_deny_host(h) for h in deny_host.split(",") if h.strip()])
    if args.allow_filename:
        allow_host_names.extend(parse_allow_deny_host_filename(args.allow_filename))
    if args.deny_filename:
        deny_host_names.extend(parse_allow_deny_host_filename(args.deny_filename))

    logging.info("listen server at %s:%d", args.bind, args.port)
    server = create_server((args.bind, args.port))
    sevent.current().call_async(tcp_accept, server, args.timeout, allow_host_names or None, deny_host_names or None)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    try:
        main(sys.argv[1:])
        sevent.instance().start()
    except KeyboardInterrupt:
        exit(0)