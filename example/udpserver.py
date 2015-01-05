# -*- coding: utf-8 -*-
# 15/1/4
# create by: snower

import sevent

loop = sevent.instance()

def on_data(s, address, data):
    print address, data
    s.write(address, "dsfsdfsfs")

server = sevent.udp.Server()
server.bind(("0.0.0.0", 20000))
server.on("data", on_data)
loop.start()