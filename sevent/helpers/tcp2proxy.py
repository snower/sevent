# -*- coding: utf-8 -*-
# 2020/7/10
# create by: snower

import time
import logging
import struct
import socket
import traceback
import argparse
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

def socks5_build_protocol(remote_host, remote_port):
    try:
        protocol_data = b"".join(
            [b"\x05\x01\x00\x01", socket.inet_aton(remote_host), struct.pack(">H", remote_port)])
    except:
        try:
            protocol_data = b"".join([b"\x05\x01\x00\x04", socket.inet_pton(socket.AF_INET6, remote_host),
                                      struct.pack(">H", remote_port)])
        except:
            protocol_data = b"".join([b"\x05\x01\x00\x03", struct.pack(">B", len(remote_host)),
                                      bytes(remote_host, "utf-8") if isinstance(remote_host, str) else remote_host,
                                      struct.pack(">H", remote_port)])
    return protocol_data

def socks5_read_protocol(buffer):
    cmd = buffer.read(1)
    if cmd == b'\x01':
        return buffer.read(6)
    if cmd == b'\x04':
        return buffer.read(18)
    if cmd == b'\x03':
        addr_len = ord(buffer.read(1))
        return buffer.read(addr_len + 2)
    return False

async def socks5_proxy(conns, conn, proxy_host, proxy_port, remote_host, remote_port, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        conn.enable_nodelay()
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
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("socks5 proxy connected %s:%d -> %s:%d -> %s:%d", conn.address[0], conn.address[1], proxy_host, proxy_port,
                     remote_host, remote_port)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("socks5 proxy error %s:%d -> %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1], proxy_host, proxy_port,
                     remote_host, remote_port, e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("socks5 proxy closed %s:%d -> %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1], proxy_host, proxy_port,
                 remote_host, remote_port, format_data_len(status["send_len"]), format_data_len(status["recv_len"]), (time.time() - start_time) * 1000)

def http_build_protocol(remote_host, remote_port):
    remote = (bytes(remote_host, "utf-8") if isinstance(remote_host, str) else remote_host) + b':' + bytes(str(remote_port), "utf-8")
    return b"CONNECT " + remote + b" HTTP/1.1\r\nHost: " + remote + b"\r\nUser-Agent: sevent\r\nProxy-Connection: Keep-Alive\r\n\r\n"

async def http_proxy(conns, conn, proxy_host, proxy_port, remote_host, remote_port, status):
    start_time = time.time()
    conn.write, pconn = warp_write(conn, status, "recv_len"), None

    try:
        conn.enable_nodelay()
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
        pconn.write = warp_write(pconn, status, "send_len")
        logging.info("tcp2proxy connected %s:%d -> %s:%d -> %s:%d", conn.address[0], conn.address[1], proxy_host, proxy_port,
                     remote_host, remote_port)
        await pconn.linkof(conn)
    except sevent.errors.SocketClosed:
        pass
    except Exception as e:
        logging.info("tcp2proxy error %s:%d -> %s:%d -> %s:%d %s %.2fms\r%s", conn.address[0], conn.address[1], proxy_host, proxy_port,
                     remote_host, remote_port, e, (time.time() - start_time) * 1000, traceback.format_exc())
        return
    finally:
        conn.close()
        if pconn: pconn.close()
        conns.pop(id(conn), None)

    logging.info("tcp2proxy closed %s:%d -> %s:%d -> %s:%d %s %s %.2fms", conn.address[0], conn.address[1], proxy_host, proxy_port,
                 remote_host, remote_port, format_data_len(status["send_len"]), format_data_len(status["recv_len"]),
                 (time.time() - start_time) * 1000)

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

    forward_info = args.forward_host.split(":")
    if len(forward_info) == 1:
        if not forward_info[0].isdigit():
            forward_host, forward_port = forward_info[0], 8088
        else:
            forward_host, forward_port = "127.0.0.1", int(forward_info[0])
    else:
        forward_host, forward_port = forward_info[0], int(forward_info[1])

    logging.info("use %s proxy %s:%d forward to %s:%d", args.proxy_type, proxy_host, proxy_port, forward_host, forward_port)
    conns = {}
    sevent.current().call_async(check_timeout, conns, args.timeout)
    while True:
        conn = await server.accept()
        status = {"recv_len": 0, "send_len": 0, "last_time": time.time(), "check_recv_len": 0, "check_send_len": 0}
        if args.proxy_type == "http":
            sevent.current().call_async(http_proxy, conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
        else:
            sevent.current().call_async(socks5_proxy, conns, conn, proxy_host, proxy_port, forward_host, forward_port, status)
        conns[id(conn)] = (conn, status)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')

    parser = argparse.ArgumentParser(description='forword tcp port to remote host from http or socks5 proxy')
    parser.add_argument('-b', dest='bind', default="0.0.0.0", help='local bind host (default: 0.0.0.0)')
    parser.add_argument('-p', dest='port', default=8088, type=int, help='local bind port (default: 8088)')
    parser.add_argument('-t', dest='timeout', default=7200, type=int, help='no read/write timeout (default: 7200)')
    parser.add_argument('-T', dest='proxy_type', default="http", choices=("http", "socks5"), help='proxy type (default: http)')
    parser.add_argument('-P', dest='proxy_host', default="127.0.0.1:8088", help='proxy host, accept format [proxy_host:proxy_port] (default: 127.0.0.1:8088)')
    parser.add_argument('-f', dest='forward_host', default="127.0.0.1:80", help='remote forward host , accept format [remote_host:remote_port] (default: 127.0.0.1:80)')
    args = parser.parse_args()

    server = sevent.tcp.Server()
    server.enable_reuseaddr()
    server.listen((args.bind, args.port))
    logging.info("listen server at %s:%d", args.bind, args.port)
    try:
        sevent.run(tcp_accept, server, args)
    except KeyboardInterrupt:
        exit(0)