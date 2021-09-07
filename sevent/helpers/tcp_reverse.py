# -*- coding: utf-8 -*-
# 2021/1/31
# create by: snower

import time
import struct
import logging
import traceback
import argparse
import threading
import signal
import sevent

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

async def reverse_port_forward(remote_conn, local_conn, status):
    start_time = time.time()

    try:
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

async def handle_remote_connection(conn, key, conns, status):
    setattr(conn, "_connected_time", time.time())
    def on_close(conn):
        if conn not in status["remote_conn"]:
            return
        status["remote_conn"].remove(conn)
        logging.info("remote conn waited close %s:%d", conn.address[0], conn.address[1])

    status["remote_conn"].append(conn)
    conn.on_close(on_close)
    try:
        _, auth_key_len = struct.unpack("!BB", (await conn.recv(2)).read(2))
        auth_key = (await conn.recv(auth_key_len)).read(auth_key_len) if auth_key_len > 0 else b''
    except sevent.errors.SocketClosed:
        return
    if auth_key != key:
        await conn.closeof()
        logging.info("remote conn auth fail %s:%d %s", conn.address[0], conn.address[1], auth_key)
        return

    setattr(conn, "_authed_time", time.time())
    if status["local_conn"]:
        forward_status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0,
                          "check_send_len": 0}
        local_conn = status["local_conn"].pop(0)
        sevent.go(reverse_port_forward, conn, local_conn, forward_status)
        conns[id(conn)] = (conn, local_conn, forward_status)
        if conn not in status["remote_conn"]:
            return
        status["remote_conn"].remove(conn)
        return
    logging.info("remote conn waiting %s:%d", conn.address[0], conn.address[1])

async def handle_local_connection(conn, key, conns, status):
    setattr(conn, "_connected_time", time.time())
    if status["remote_conn"]:
        forward_status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0,
                          "check_send_len": 0}
        remote_conn = status["remote_conn"].pop(0)
        sevent.go(reverse_port_forward, remote_conn, conn, forward_status)
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

async def run_server(server, key, conns, status, handle):
    while True:
        try:
            conn = await server.accept()
            sevent.current().call_async(handle, conn, key, conns, status)
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
            await conn.send(struct.pack("!BB", 1, len(key)) + key)
            (await conn.recv(1)).read(1)
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
    parser.add_argument('-l', dest='local_port', default=8089, type=int, help='server mode  local bind port (default: 8089)')
    parser.add_argument('-H', dest='host', default="127.0.0.1", help='client mode bind host (default: 0.0.0.0)')
    parser.add_argument('-P', dest='port', default=8088, type=int, help='client mode bind port (default: 8088)')
    parser.add_argument('-f', dest='forward_host', default="127.0.0.1:80", help='client mode forward host , accept format [remote_host:remote_port] (default: 127.0.0.1:80)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    config_signal()
    conns, status = {}, {"remote_conn": [], "local_conn": []}
    try:
        if not args.is_client_mode:
            remote_server = sevent.tcp.Server()
            local_server = sevent.tcp.Server()
            remote_server.enable_reuseaddr()
            local_server.enable_reuseaddr()
            remote_server.listen((args.bind, args.remote_port))
            local_server.listen((args.bind, args.local_port))
            logging.info("listen %s %d -> %d", args.bind, args.local_port, args.remote_port)

            sevent.instance().call_async(run_server, remote_server, sevent.utils.ensure_bytes(args.key),
                                         conns, status, handle_remote_connection)
            sevent.instance().call_async(run_server, local_server, sevent.utils.ensure_bytes(args.key),
                                         conns, status, handle_local_connection)
        else:
            forward_info = args.forward_host.split(":")
            if len(forward_info) == 1:
                if not forward_info[0].isdigit():
                    forward_host, forward_port = forward_info[0], 8088
                else:
                    forward_host, forward_port = "127.0.0.1", int(forward_info[0])
            else:
                forward_host, forward_port = forward_info[0], int(forward_info[1])
            logging.info("connect %s:%d -> %s:%d", args.host, args.port, forward_host, forward_port)
            sevent.instance().call_async(run_connect, (args.host, args.port), (forward_host, forward_port),
                                         sevent.utils.ensure_bytes(args.key), conns, status)

        sevent.run(check_timeout, conns, status, args.timeout)
    except KeyboardInterrupt:
        exit(0)