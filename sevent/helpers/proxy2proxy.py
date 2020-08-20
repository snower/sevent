# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import time
import argparse
import logging
import traceback
import sevent
from .simple_proxy import format_data_len, warp_write, http_protocol_parse, socks5_protocol_parse
from .tcp2proxy import http_build_protocol, socks5_build_protocol, socks5_read_protocol

async def socks5_proxy(proxy_host, proxy_port, remote_host, remote_port):
    pconn = None
    try:
        pconn = sevent.tcp.Socket()
        pconn.enable_nodelay()
        await pconn.connectof((proxy_host, proxy_port))
        await pconn.send(b"\x05\x01\x00")
        buffer = await pconn.recv()
        if buffer.read() != b'\x05\00':
            logging.info("protocol hello error")
            return

        protocol_data = socks5_build_protocol(remote_host, remote_port)
        await pconn.send(protocol_data)
        buffer = await pconn.recv()
        if buffer.read(3) != b'\x05\x00\x00':
            logging.info("protocol error")
            return
        if not socks5_read_protocol(buffer):
            logging.info("protocol error")
            return
    except sevent.errors.SocketClosed:
        pconn = None
    except Exception as e:
        if pconn: pconn.close()
        raise e
    return pconn

async def http_proxy(proxy_host, proxy_port, remote_host, remote_port):
    pconn = None
    try:
        pconn = sevent.tcp.Socket()
        pconn.enable_nodelay()
        await pconn.connectof((proxy_host, proxy_port))

        protocol_data = http_build_protocol(remote_host, remote_port)
        await pconn.send(protocol_data)
        buffer = await pconn.recv()
        if buffer.read(12).lower() != b"http/1.1 200":
            logging.info("protocol error")
            return
        buffer.read()
    except sevent.errors.SocketClosed:
        pconn = None
    except Exception as e:
        if pconn: pconn.close()
        raise e
    return pconn

async def tcp_proxy(conns, conn, proxy_type, proxy_host, proxy_port, status):
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

        logging.info("connected %s %s:%d -> %s %s:%d -> %s:%d", protocol, conn.address[0], conn.address[1],
                     proxy_type, proxy_host, proxy_port, host, port)
        if proxy_type == "http":
            pconn = await http_proxy(proxy_host, proxy_port, host, port)
        else:
            pconn = await socks5_proxy(proxy_host, proxy_port, host, port)
        pconn.write = warp_write(pconn, status, "send_len")
        await pconn.connectof((host, port))
        if data:
            await pconn.send(data)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("error %s %s:%d -> %s %s:%d -> %s:%d %s %.2fms\r%s", protocol, conn.address[0], conn.address[1],
                     proxy_type, proxy_host, proxy_port, host, port, e,
                     (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("closed %s %s:%d -> %s %s:%d -> %s:%d %s %s %.2fms", protocol, conn.address[0], conn.address[1],
                 proxy_type, proxy_host, proxy_port, host, port, format_data_len(status["send_len"]),
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

async def tcp_accept(server, args):
    proxy_info = args.proxy_host.split(":")
    if len(proxy_info) == 1:
        if not proxy_info[0].isdigit():
            proxy_host, proxy_port = proxy_info[0], 8088
        else:
            proxy_host, proxy_port = "127.0.0.1", int(proxy_info[0])
    else:
        proxy_host, proxy_port = proxy_info[0], int(proxy_info[1])

    logging.info("use %s proxy %s:%d", args.proxy_type, proxy_host, proxy_port)
    conns = {}
    sevent.current().call_async(check_timeout, conns, args.timeout)
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        sevent.current().call_async(tcp_proxy, conns, conn, args.proxy_type, proxy_host, proxy_port, status)
        conns[id(conn)] = (conn, status)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')

    parser = argparse.ArgumentParser(description='simple http and socks5 proxy forward to http or socks5 uplink proxy')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='local bind host (default: 0.0.0.0)')
    parser.add_argument('-p', dest='port', default=8088, type=int, help='local bind port (default: 8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-T', dest='proxy_type', default="http", choices=("http", "socks5"), help='proxy type (default: http)')
    parser.add_argument('-P', dest='proxy_host', default="127.0.0.1:8088", help='proxy host, accept format [proxy_host:proxy_port]  (default: 127.0.0.1:8088)')
    args = parser.parse_args()

    server = sevent.tcp.Server()
    server.enable_reuseaddr()
    server.listen((args.bind, args.port))
    logging.info("listen server at %s:%d", args.bind, args.port)
    try:
        sevent.run(tcp_accept, server, args)
    except KeyboardInterrupt:
        exit(0)