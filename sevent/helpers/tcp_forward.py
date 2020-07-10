# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import time
import traceback
import logging
import argparse
import sevent

def format_data_len(date_len):
    if date_len < 1024:
        return "%dB" % date_len
    elif date_len < 1024*1024:
        return "%.3fK" % (date_len/1024.0)
    elif date_len < 1024*1024*1024:
        return "%.3fM" % (date_len/(1024.0*1024.0))

def warp_write(conn, status, key):
    origin_write = conn.write
    def _(data):
        status[key] += len(data)
        return origin_write(data)
    return _

def parse_forward(forwards):
    forward_hosts = []
    for forward in forwards:
        hosts, i = ['0.0.0.0', 0, '127.0.0.1', 0], 0
        forward_info = forward.split(":")
        for f in forward_info:
            if i >= 4:
                break

            if i in (0, 2):
                if not f.isdigit():
                    hosts[i] = f
                    i += 1
                    continue
                hosts[i + 1] = int(f)
                i += 2
                continue
            else:
                if f.isdigit():
                    hosts[i] = int(f)
                    i += 1
                    continue
                hosts[i + 1] = f
                i += 2
                continue
        forward_hosts.append(tuple(hosts))
    return forward_hosts

async def tcp_forward(conns, conn, forward_host, forward_port, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        pconn = sevent.tcp.Socket()
        await pconn.connectof((forward_host, forward_port))
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("http proxy connect %s:%d -> %s:%d", conn.address[0], conn.address[1], forward_host, forward_port)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("http proxy error %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1],
                     forward_host, forward_port, e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("http proxy connected %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1],
                 forward_host, forward_port, format_data_len(status["send_len"]), format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

async def tcp_forward_server(conns, server, forward_host, forward_port):
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        sevent.current().call_async(tcp_forward, conns, conn, forward_host, forward_port, status)
        conns[id(conn)] = (conn, status)

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

async def tcp_forward_servers(servers, timeout):
    conns = {}
    for server, (forward_host, forward_port) in servers:
        sevent.current().call_async(tcp_forward_server, conns, server, forward_host, forward_port)
    await check_timeout(conns, timeout)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')

    parser = argparse.ArgumentParser(description="tcp port forward")
    parser.add_argument('-L', dest='forwards', default=[], action="append", type=str, help='forward host (example: 0.0.0.0:80:127.0.0.1:8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='timeout (default: 7200)')
    args = parser.parse_args()

    if not args.forwards:
        exit(0)

    forwards = parse_forward(args.forwards)
    forward_servers = []
    for (bind, port, forward_host, forward_port) in forwards:
        server = sevent.tcp.Server()
        server.enable_reuseaddr()
        server.listen((bind, port))
        forward_servers.append((server, (forward_host, forward_port)))
        logging.info("port forward listen %s:%s to %s:%s", bind, port, forward_host, forward_port)

    sevent.run(tcp_forward_servers, forward_servers, args.timeout)