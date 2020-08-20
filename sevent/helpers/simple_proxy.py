# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import time
import struct
import socket
import argparse
import logging
import traceback
import sevent

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

def warp_write(conn, status, key):
    origin_write = conn.write
    def _(data):
        status[key] += len(data)
        return origin_write(data)
    return _

def http_protocol_parse_address(data, default_port=80):
    address = data.split(b":")
    if len(address) != 2:
        return (address[0].decode("utf-8"), default_port)
    return (address[0].decode("utf-8"), int(address[1].decode("utf-8")))

async def http_protocol_parse(conn, buffer):
    data = buffer.read()
    index = data.find(b" ")
    method = data[:index].decode("utf-8")
    data = data[index + 1:]

    if method.lower() == "connect":
        index = data.find(b" ")
        host, port = http_protocol_parse_address(data[:index], 443)
        await conn.send(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        return (host, port, b'')

    if method.lower() in ("get", "post", "put", "options", "head", "delete", "patch"):
        data = data[7:]
        index = data.find(b"/")
        host, port = http_protocol_parse_address(data[:index], 80)
        return (host, port, method.encode("utf-8") + b' ' + data[index:])
    return ('', 0, None)

async def socks5_protocol_parse(conn, buffer):
    buffer.read()
    await conn.send(b'\x05\00')
    buffer = await conn.recv()
    data = buffer.read()
    if data[1] != 1:
        return ('', 0, None)

    if data[3] == 1:
        await conn.send(b"".join([b'\x05\x00\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
        return (socket.inet_ntoa(data[4:8]),
                struct.unpack('>H', data[8:10])[0], data[10:])

    if data[3] == 4:
        await conn.send(b"".join([b'\x05\x00\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
        return (socket.inet_ntop(socket.AF_INET6, data[4:20]),
                struct.unpack('>H', data[20:22])[0], data[22:])

    elif data[3] == 3:
        host_len = data[4] + 5
        await conn.send(b"".join([b'\x05\x00\x00\x01', socket.inet_aton('0.0.0.0'), struct.pack(">H", 0)]))
        return (data[5: host_len].decode("utf-8"),
                struct.unpack('>H', data[host_len: host_len+2])[0],
                data[host_len+2:])

    return ('', 0, None)

async def tcp_proxy(conns, conn, status):
    start_time = time.time()
    host, port, protocol = '', 0, ''
    conn.write, pconn = warp_write(conn, status, "recv_len"), None
    try:
        conn.enable_nodelay()
        buffer = await conn.recv()
        if buffer[0] == 5:
            protocol = 'socks5'
            host, port, data = await socks5_protocol_parse(conn, buffer)
        else:
            protocol = 'http'
            host, port, data = await http_protocol_parse(conn, buffer)
        if not host or not port:
            logging.info("empty address")
            return

        logging.info("connected %s %s:%d", protocol, host, port)
        pconn = sevent.tcp.Socket()
        pconn.enable_nodelay()
        pconn.write = warp_write(pconn, status, "send_len")
        await pconn.connectof((host, port))
        if data:
            await pconn.send(data)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("error %s %s:%d %s %.2fms\r%s", protocol, host, port, e,
                     (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("closed %s %s:%d %s %s %.2fms", protocol, host, port, format_data_len(status["send_len"]),
                 format_data_len(status["recv_len"]), (time.time() - start_time) * 1000)

async def check_timeout(conns, timeout):
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
                    conn.close()
                    conns.pop(conn_id, None)
        finally:
            await sevent.current().sleep(30)

async def tcp_accept(server, timeout):
    conns = {}
    sevent.current().call_async(check_timeout, conns, timeout)
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        sevent.current().call_async(tcp_proxy, conns, conn, status)
        conns[id(conn)] = (conn, status)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')

    parser = argparse.ArgumentParser(description='simple http and socks5 proxy server')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='local bind host (default: 0.0.0.0)')
    parser.add_argument('-p', dest='port', default=8088, type=int, help='local bind port (default: 8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    args = parser.parse_args()

    logging.info("listen server at %s:%d", args.bind, args.port)
    server = sevent.tcp.Server()
    server.enable_reuseaddr()
    server.listen((args.bind, args.port))
    try:
        sevent.run(tcp_accept, server, args.timeout)
    except KeyboardInterrupt:
        exit(0)