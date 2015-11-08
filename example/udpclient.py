# -*- coding: utf-8 -*-
# 15/1/4
# create by: snower

import sevent

loop = sevent.instance()

def on_data(s, address, data):
    print address, data

def start():
    socket  = sevent.udp.Socket()
    socket.write(("127.0.0.1", 20000), "dsfsdfsfs")
    socket.on("data", on_data)

loop.async(start)
loop.start()