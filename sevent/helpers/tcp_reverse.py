# -*- coding: utf-8 -*-
# 2021/1/31
# create by: snower

import os
import time
import struct
import logging
import traceback
import argparse
import threading
import signal
import socket
import hashlib
import sevent
from .simple_proxy import http_protocol_parse, socks5_protocol_parse

def config_signal():
    signal.signal(signal.SIGINT, lambda signum, frame: sevent.current().stop())
    signal.signal(signal.SIGTERM, lambda signum, frame: sevent.current().stop())

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

def gen_sign_key(key):
    t = struct.pack("I", int(time.time()))
    oncestr = os.urandom(16)
    return t + oncestr + hashlib.md5(t + oncestr + sevent.utils.ensure_bytes(key)).digest()

def check_sign_key(key, sign_key):
    return sign_key[20:] == hashlib.md5(sign_key[:20] + sevent.utils.ensure_bytes(key)).digest()

async def parse_forward_address(conn, proxy_type):
    if not proxy_type or proxy_type == "raw":
        return None

    if proxy_type == "socks5":
        buffer = await conn.recv()
        host, port, data = await socks5_protocol_parse(conn, buffer)
        if not host or not port:
            raise Exception("parse error")
        buffer.write(data + buffer.read())
        return (host, port)

    if proxy_type == "http":
        buffer = await conn.recv()
        host, port, data = await http_protocol_parse(conn, buffer)
        if not host or not port:
            raise Exception("parse error")
        buffer.write(data + buffer.read())
        return (host, port)

    if proxy_type == "redirect":
        address_data = conn.socket.getsockopt(socket.SOL_IP, 80, 16)
        host, port = socket.inet_ntoa(address_data[4:8]), struct.unpack(">H", address_data[2:4])[0]
        return (host, port)
    raise Exception("parse error")

async def write_forward_address(conn, forward_address):
    await conn.send(b"".join([struct.pack("!H", len(forward_address[0])),
                               sevent.utils.ensure_bytes(forward_address[0]), struct.pack("!H", forward_address[1])]))

async def read_forward_address(conn):
    host_len, = struct.unpack("!H", (await conn.recv(2)).read(2))
    host = sevent.utils.ensure_unicode((await conn.recv(host_len)).read(host_len))
    port, = struct.unpack("!H", (await conn.recv(2)).read(2))
    return (host, port)

async def tcp_forward(conn, forward_address, conns, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        conn.enable_nodelay()
        pconn = sevent.tcp.Socket()
        pconn.enable_nodelay()
        await pconn.connectof(forward_address)
        pconn.write = warp_write(pconn, status, "send_len")
        conns[id(conn)] = (conn, pconn, status)
        logging.info("tcp forward connected %s:%d -> %s:%d", conn.address[0], conn.address[1], forward_address[0], forward_address[1])
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("tcp forward error %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1],
                     forward_address[0], forward_address[1], e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("tcp forward closed %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1],
                 forward_address[0], forward_address[1], format_data_len(status["send_len"]), format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

async def reverse_port_forward(remote_conn, local_conn, status, forward_address):
    start_time = time.time()

    try:
        if forward_address:
            await remote_conn.send(b'\x01')
            await write_forward_address(remote_conn, forward_address)
        else:
            await remote_conn.send(b'\x00')
        local_conn.write = warp_write(local_conn, status, "recv_len")
        remote_conn.write = warp_write(remote_conn, status, "send_len")
        logging.info("tcp forward connected %s:%d -> %s:%d", local_conn.address[0], local_conn.address[1],
                     remote_conn.address[0], remote_conn.address[1])
        await local_conn.linkof(remote_conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("tcp forward error %s:%d -> %s:%d %s %.2fms\r%s", local_conn.address[0], local_conn.address[1],
                     remote_conn.address[0], remote_conn.address[1], e, (time.time() - start_time) * 1000,
                     traceback.format_exc())
        return
    finally:
        remote_conn.close()
        local_conn.close()
        conns.pop(id(remote_conn), None)

    logging.info("tcp forward closed %s:%d -> %s:%d %s %s %.2fms", local_conn.address[0], local_conn.address[1],
                 remote_conn.address[0], remote_conn.address[1], format_data_len(status["send_len"]),
                 format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

async def handle_remote_connection(conn, forward_address, key, proxy_type, conns, status):
    setattr(conn, "_connected_time", time.time())
    def on_close(conn):
        if conn not in status["remote_conn"]:
            return
        status["remote_conn"].remove(conn)
        logging.info("remote conn waited close %s:%d", conn.address[0], conn.address[1])

    status["remote_conn"].append(conn)
    conn.on_close(on_close)
    try:
        connect_type, sign_key_len = struct.unpack("!BB", (await conn.recv(2)).read(2))
        sign_key = (await conn.recv(sign_key_len)).read(sign_key_len) if sign_key_len > 0 else b''
    except sevent.errors.SocketClosed:
        return
    if not check_sign_key(key, sign_key):
        await conn.closeof()
        logging.info("remote conn auth fail %s:%d %s", conn.address[0], conn.address[1], sign_key)
        return

    if connect_type == 1:
        setattr(conn, "_authed_time", time.time())
        if status["local_conn"]:
            forward_status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0,
                              "check_send_len": 0}
            local_conn = status["local_conn"].pop(0)
            sevent.go(reverse_port_forward, conn, local_conn, forward_status, local_conn._connected_forward_address)
            conns[id(conn)] = (conn, local_conn, forward_status)
            if conn not in status["remote_conn"]:
                return
            status["remote_conn"].remove(conn)
            return
        logging.info("remote conn waiting %s:%d", conn.address[0], conn.address[1])
        return

    if connect_type == 2 or connect_type == 3:
        try:
            if connect_type == 3:
                forward_address = await read_forward_address(conn)
            await conn.send(b'\x00')
            forward_status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0,
                              "check_send_len": 0}
            sevent.current().call_async(tcp_forward, conn, forward_address, conns, forward_status)
        except sevent.errors.SocketClosed:
            pass
        except Exception as e:
            logging.info("tcp forward error %s:%d -> %s:%d %s\r%s", conn.address[0], conn.address[1],
                         forward_address[0], forward_address[1], e, traceback.format_exc())
        return

    await conn.closeof()
    logging.info("remote conn unsupport connect type %s:%d %s", conn.address[0], conn.address[1], auth_key)

async def handle_local_connection(conn, forward_address, key, proxy_type, conns, status):
    try:
        forward_address = (await parse_forward_address(conn, proxy_type)) if proxy_type else None
    except Exception as e:
        logging.info("parse proxy forward address error %s", e)
        return

    setattr(conn, "_connected_forward_address", forward_address)
    setattr(conn, "_connected_time", time.time())
    if status["remote_conn"]:
        forward_status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0,
                          "check_send_len": 0}
        remote_conn = status["remote_conn"].pop(0)
        sevent.go(reverse_port_forward, remote_conn, conn, forward_status, forward_address)
        conns[id(remote_conn)] = (remote_conn, conn, forward_status)
        return

    def on_close(conn):
        if conn not in status["local_conn"]:
            return
        status["local_conn"].remove(conn)
        logging.info("local conn waited close %s:%d", conn.address[0], conn.address[1])
    status["local_conn"].append(conn)
    conn.on_close(on_close)
    logging.info("local conn waiting %s:%d", conn.address[0], conn.address[1])

async def run_server(server, forward_address, key, proxy_type, conns, status, handle):
    while True:
        try:
            conn = await server.accept()
            sevent.current().call_async(handle, conn, forward_address, key, proxy_type, conns, status)
        except sevent.errors.SocketClosed as e:
            sevent.current().call_async(sevent.current().stop)
            raise e

async def run_connect(remote_address, forward_address, key, conns, status):
    while True:
        start_time = time.time()
        try:
            conn = sevent.tcp.Socket()
            conn.enable_nodelay()
            await conn.connectof(remote_address)
            sign_key = gen_sign_key(key)
            await conn.send(struct.pack("!BB", 1, len(sign_key)) + sign_key)
            connect_type = (await conn.recv(1)).read(1)
            if connect_type == b'\x01':
                forward_address = await read_forward_address(conn)
            forward_status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0,
                              "check_send_len": 0}
            sevent.current().call_async(tcp_forward, conn, forward_address, conns, forward_status)
        except sevent.errors.SocketClosed as e:
            logging.info("connect error %s:%d %s", remote_address[0], remote_address[1], e)
            if time.time() - start_time < 5:
                await sevent.sleep(5)
        except (sevent.errors.ResolveError, ConnectionRefusedError) as e:
            logging.info("connect error %s:%d %s", remote_address[0], remote_address[1], e)
            await sevent.sleep(5)
        except Exception as e:
            sevent.current().call_async(sevent.current().stop)
            raise e

async def handle_local_connect(conn, remote_address, key, proxy_type, conns, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        forward_address = (await parse_forward_address(conn, proxy_type)) if proxy_type else None

        pconn = sevent.tcp.Socket()
        pconn.enable_nodelay()
        await pconn.connectof(remote_address)
        sign_key = gen_sign_key(key)
        if forward_address:
            await pconn.send(struct.pack("!BB", 3, len(sign_key)) + sign_key)
            await write_forward_address(pconn, forward_address)
        else:
            await pconn.send(struct.pack("!BB", 2, len(sign_key)) + sign_key)
        (await pconn.recv(1)).read(1)
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("tcp forward connected %s:%d -> %s:%d", conn.address[0], conn.address[1], remote_address[0], remote_address[1])
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("tcp forward error %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1],
                     remote_address[0], remote_address[1], e, (time.time() - start_time) * 1000,
                     traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("tcp forward closed %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1],
                 remote_address[0], remote_address[1], format_data_len(status["send_len"]),
                 format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

async def run_local_server(server, remote_address, key, proxy_type, conns):
    while True:
        try:
            conn = await server.accept()
            status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
            sevent.current().call_async(handle_local_connect, conn, remote_address, key, proxy_type, conns, status)
            conns[id(conn)] = (conn, conn, status)
        except sevent.errors.SocketClosed as e:
            sevent.current().call_async(sevent.current().stop)
            raise e

async def check_timeout(conns, conn_status, timeout):
    def run_check():
        while True:
            try:
                now = time.time()
                for conn in tuple(conn_status["remote_conn"]):
                    if not hasattr(conn, "_authed_time"):
                        if now - conn._connected_time >= 30:
                            sevent.current().add_async_safe(conn.close)
                    elif now - conn._authed_time >= 180:
                        sevent.current().add_async_safe(conn.close)

                for conn in tuple(conn_status["local_conn"]):
                    if now - conn._connected_time >= 180:
                        sevent.current().add_async_safe(conn.close)

                for conn_id, (conn, pconn, status) in list(conns.items()):
                    if status['check_recv_len'] != status['recv_len'] or status['check_send_len'] != status['send_len']:
                        status["check_recv_len"] = status["recv_len"]
                        status["check_send_len"] = status["send_len"]
                        status['last_time'] = now
                        continue

                    if now - status['last_time'] >= timeout:
                        sevent.current().add_async_safe(conn.close)
                        sevent.current().add_async(pconn.close)
                        conns.pop(conn_id, None)
            finally:
                time.sleep(min(float(timeout) / 2.0, 30))

    if timeout > 0:
        check_thread = threading.Thread(target=run_check)
        check_thread.setDaemon(True)
        check_thread.start()
    await sevent.Future()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='tcp reverse port forward')
    parser.add_argument('-c', dest='is_client_mode', nargs='?', const=True, default=False, type=bool, help='is client mode (defualt: False)')
    parser.add_argument('-k', dest='key', default='', type=str, help='auth key (defualt: "")')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='server mode bind host (default: 0.0.0.0)')
    parser.add_argument('-r', dest='remote_port', default=8088, type=int, help='server mode remote bind port (default: 8088)')
    parser.add_argument('-l', dest='local_port', default=0, type=int, help='server mode  local bind port (default: 8089)')
    parser.add_argument('-H', dest='host', default="127.0.0.1", help='client mode connect server host (default: 127.0.0.1)')
    parser.add_argument('-P', dest='port', default=8088, type=int, help='client mode connect server port (default: 8088)')
    parser.add_argument('-f', dest='forward_host', default="127.0.0.1:80", help='client mode forward host , accept format [remote_host:remote_port] (default: 127.0.0.1:80)')
    parser.add_argument('-T', dest='proxy_type', default="", choices=("raw", "http", "socks5", "redirect"), help='local listen proxy type (default: raw)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    config_signal()
    conns, status = {}, {"remote_conn": [], "local_conn": []}

    forward_info = args.forward_host.split(":")
    if len(forward_info) == 1:
        if not forward_info[0].isdigit():
            forward_host, forward_port = forward_info[0], 8088
        else:
            forward_host, forward_port = "127.0.0.1", int(forward_info[0])
    else:
        forward_host, forward_port = forward_info[0], int(forward_info[1])

    try:
        if not args.is_client_mode:
            remote_server = sevent.tcp.Server()
            local_server = sevent.tcp.Server()
            remote_server.enable_reuseaddr()
            local_server.enable_reuseaddr()
            remote_server.listen((args.bind, args.remote_port))
            local_server.listen((args.bind, args.local_port or 8089))
            logging.info("listen %s %d -> %d", args.bind, args.local_port or 8089, args.remote_port)

            sevent.instance().call_async(run_server, remote_server, (forward_host, forward_port),
                                         sevent.utils.ensure_bytes(args.key), args.proxy_type,
                                         conns, status, handle_remote_connection)
            sevent.instance().call_async(run_server, local_server, (forward_host, forward_port),
                                         sevent.utils.ensure_bytes(args.key), args.proxy_type,
                                         conns, status, handle_local_connection)
        else:
            if args.local_port:
                local_server = sevent.tcp.Server()
                local_server.enable_reuseaddr()
                local_server.listen((args.bind, args.local_port))
                logging.info("listen %s %d", args.bind, args.local_port)
                sevent.instance().call_async(run_local_server, local_server, (args.host, args.port),
                                             sevent.utils.ensure_bytes(args.key), args.proxy_type, conns)

            logging.info("connect %s:%d -> %s:%d", args.host, args.port, forward_host, forward_port)
            sevent.instance().call_async(run_connect, (args.host, args.port), (forward_host, forward_port),
                                         sevent.utils.ensure_bytes(args.key), conns, status)

        sevent.run(check_timeout, conns, status, args.timeout)
    except KeyboardInterrupt:
        exit(0)