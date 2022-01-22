# -*- coding: utf-8 -*-
# 2021/2/1
# create by: snower

import sys
import multiprocessing
import logging
import sevent
from . import tcp_forward
from . import simple_proxy
from . import tcp2proxy
from . import proxy2proxy
from . import redirect2proxy
from . import tcp_reverse
from . import arproxy

HEPERS = {
    "tcp_forward": tcp_forward,
    "simple_proxy": simple_proxy,
    "tcp2proxy": tcp2proxy,
    "proxy2proxy": proxy2proxy,
    "redirect2proxy": redirect2proxy,
    "tcp_reverse": tcp_reverse,
    "arproxy": arproxy,
}

def show_help_message():
    print('usage: -m [HELPER_NAME] [ARGS]\r\n')
    print('simple sevent helpers \r\n')
    print("can use helpers:\r\n\r\n" + '\r\n'.join(["sevent.helpers." + name for name in HEPERS]))
    print('\r\n\r\n' + '*' * 64 + '\r\n')

    for name, helper in HEPERS.items():
        sys.argv[0] = "-m sevent.helpers." + name
        p = multiprocessing.Process(target=helper.main, args=(["-h"],))
        p.start()
        p.join()
        print('\r\n\r\n' + '*' * 64 + '\r\n')

if __name__ == "__main__":
    if "-h" in sys.argv:
        show_help_message()
        exit(0)

    args_helpers = []
    for arg in sys.argv[1:]:
        if arg and arg[0] == "@" and arg[1:] in HEPERS:
            args_helpers.append((arg[1:], HEPERS[arg[1:]], []))
        elif not args_helpers:
            continue
        else:
            args_helpers[-1][2].append(arg)

    if not args_helpers:
        show_help_message()
        exit(0)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)1.1s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
    for name, helper, argv in args_helpers:
        logging.info("start helper %s by %s", name, argv)
        helper.main(argv)
    try:
        sevent.instance().start()
    except KeyboardInterrupt:
        exit(0)