# -*- coding: utf-8 -*-
# 2022/1/22
# create by: snower

import os
import string
import struct
import signal
import socket
import sevent

ascii_digits_letters = string.ascii_letters + string.digits
__SSL_CONTEXT_CACHE__ = {}

def config_signal():
    signal.signal(signal.SIGINT, lambda signum, frame: sevent.current().stop())
    signal.signal(signal.SIGTERM, lambda signum, frame: sevent.current().stop())

def get_address_environ(address, key):
    if address and isinstance(address, tuple):
        if isinstance(address[0], str):
            host_key = "".join([c if c in ascii_digits_letters else "_" for c in address[0]]).upper()
            if len(address) >= 2:
                value = os.environ.get("%s_%s_%s" % (key, host_key, address[1]))
                if value is not None:
                    return value
        if len(address) >= 2:
            value = os.environ.get("%s_%s" % (key, address[1]))
            if value is not None:
                return value
    return os.environ.get(key)

def config_ssl_context(address, context):
    import ssl
    versions = get_address_environ(address, "SEVENT_HELPERS_SSL_VERSIONS")
    if versions:
        versions = set(versions.replace(".", "_").split(","))
        for version in ("SSLv2", "SSLv3", "TLSv1", "TLSv1_1", "TLSv1_2", "TLSv1_3"):
            if hasattr(ssl, "OP_NO_%s" % version) and version not in versions:
                context.options |= getattr(ssl, "OP_NO_%s" % version)
    maximum_version = get_address_environ(address, "SEVENT_HELPERS_SSL_MAXIMUM_VERSION")
    if maximum_version and hasattr(ssl.TLSVersion, maximum_version.replace(".", "_")):
        context.maximum_version = getattr(ssl.TLSVersion, maximum_version.replace(".", "_"))
    minimum_version = get_address_environ(address, "SEVENT_HELPERS_SSL_MINIMUM_VERSION")
    if minimum_version and hasattr(ssl.TLSVersion, maximum_version.replace(".", "_")):
        context.minimum_version = getattr(ssl.TLSVersion, maximum_version.replace(".", "_"))
    alpn_protocols = get_address_environ(address, "SEVENT_HELPERS_SSL_ALPN_PROTOCOLS")
    if alpn_protocols:
        context.set_alpn_protocols([alpn_protocol for alpn_protocol in alpn_protocols.split(",")])
    npn_protocols = get_address_environ(address, "SEVENT_HELPERS_SSL_NPN_PROTOCOLS")
    if npn_protocols:
        context.set_npn_protocols([npn_protocol for npn_protocol in npn_protocols.split(",")])
    ciphers = get_address_environ(address, "SEVENT_HELPERS_SSL_CIPHERS")
    if ciphers:
        context.set_ciphers(ciphers)
    ecdh_curve = get_address_environ(address, "SEVENT_HELPERS_SSL_ECDH_CURVE")
    if ecdh_curve:
        context.set_ecdh_curve(ecdh_curve)
    disable_ticket = get_address_environ(address, "SEVENT_HELPERS_SSL_DISABLE_TICKET")
    if disable_ticket:
        context.options |= ssl.OP_NO_TICKET
    else:
        context.options &= ~ssl.OP_NO_TICKET

def create_server(address, *args, **kwargs):
    if not isinstance(address, (tuple, str)):
        address = tuple(address)
    if "pipe" in address:
        server = sevent.pipe.PipeServer()
    else:
        if address in __SSL_CONTEXT_CACHE__:
            context = __SSL_CONTEXT_CACHE__[address]
            server = sevent.ssl.SSLServer(context)
        else:
            ssl_certificate_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CERTIFICATE_FILE")
            ssl_certificate_key_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CERTIFICATE_KEY_FILE")
            if ssl_certificate_file and ssl_certificate_key_file:
                import ssl
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                context.load_cert_chain(certfile=ssl_certificate_file, keyfile=ssl_certificate_key_file)
                ssl_ca_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CA_FILE")
                if ssl_ca_file:
                    if ssl_ca_file == "-":
                        context.load_default_certs(ssl.Purpose.CLIENT_AUTH)
                    else:
                        context.load_verify_locations(cafile=ssl_ca_file)
                if get_address_environ(address, "SEVENT_HELPERS_SSL_VERIFY_OPTIONAL"):
                    context.verify_mode = ssl.CERT_OPTIONAL
                elif get_address_environ(address, "SEVENT_HELPERS_SSL_VERIFY_REQUIRED"):
                    context.verify_mode = ssl.CERT_REQUIRED
                config_ssl_context(address, context)
                __SSL_CONTEXT_CACHE__[address] = context
                server = sevent.ssl.SSLServer(context)
            else:
                server = sevent.tcp.Server()
    server.enable_reuseaddr()
    server.listen(address, *args, **kwargs)
    setattr(server, "address", address)
    return server

def create_socket_ssl_context(address, ssl_ca_file):
    import ssl
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ssl_ca_file if ssl_ca_file != "-" else None)
    ssl_certificate_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CERTIFICATE_FILE")
    ssl_certificate_key_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CERTIFICATE_KEY_FILE")
    if ssl_certificate_file and ssl_certificate_key_file:
        context.load_cert_chain(certfile=ssl_certificate_file, keyfile=ssl_certificate_key_file)
    if get_address_environ(address, "SEVENT_HELPERS_SSL_INSECURE"):
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        if get_address_environ(address, "SEVENT_HELPERS_SSL_VERIFY_NONE"):
            context.verify_mode = ssl.CERT_NONE
        elif get_address_environ(address, "SEVENT_HELPERS_SSL_VERIFY_OPTIONAL"):
            context.verify_mode = ssl.CERT_OPTIONAL
        if get_address_environ(address, "SEVENT_HELPERS_SSL_VERIFY_HOSTNAME_NONE"):
            context.check_hostname = False
    config_ssl_context(address, context)
    return context

def create_socket(address):
    if not isinstance(address, (tuple, str)):
        address = tuple(address)
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
            if address in __SSL_CONTEXT_CACHE__:
                context = __SSL_CONTEXT_CACHE__[address]
                conn = sevent.ssl.SSLSocket(context=context, server_hostname=address[0] if address and isinstance(address, tuple) else None)
            else:
                use_ssl_socket = get_address_environ(address, "SEVENT_HELPERS_SSL_SOCKET")
                ssl_ca_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CA_FILE")
                if use_ssl_socket or ssl_ca_file:
                    context = create_socket_ssl_context(address, ssl_ca_file)
                    __SSL_CONTEXT_CACHE__[address] = context
                    conn = sevent.ssl.SSLSocket(context=context, server_hostname=address[0] if address and isinstance(address, tuple) else None)
                else:
                    conn = sevent.tcp.Socket()
    else:
        if address in __SSL_CONTEXT_CACHE__:
            context = __SSL_CONTEXT_CACHE__[address]
            conn = sevent.ssl.SSLSocket(context=context, server_hostname=address[0] if address and isinstance(address, tuple) else None)
        else:
            use_ssl_socket = get_address_environ(address, "SEVENT_HELPERS_SSL_SOCKET")
            ssl_ca_file = get_address_environ(address, "SEVENT_HELPERS_SSL_CA_FILE")
            if use_ssl_socket or ssl_ca_file:
                context = create_socket_ssl_context(address, ssl_ca_file)
                __SSL_CONTEXT_CACHE__[address] = context
                conn = sevent.ssl.SSLSocket(context=context, server_hostname=address[0] if address and isinstance(address, tuple) else None)
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