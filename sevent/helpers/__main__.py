# -*- coding: utf-8 -*-
# 2021/2/1
# create by: snower

import sys
import os
import multiprocessing


def show_help_message(filename):
    sys.argv[0] = filename
    with open(filename) as fp:
        exec(fp.read())
    print()


if __name__ == "__main__":
    if "-h" not in sys.argv:
        exit(0)

    filepath = os.path.dirname(__file__)
    filenames = []
    for filename in os.listdir(filepath):
        if filename[:2] == "__" or filename[-3:] != ".py":
            continue
        filenames.append(filename)

    print('usage: -m [HELPER_NAME] [ARGS]\r\n')
    print('simple sevent helpers \r\n')
    print("can use helpers:\r\n\r\n" + '\r\n'.join(["sevent.helpers." + filename[:-3] for filename in filenames]))
    print('\r\n\r\n' + '*' * 64 + '\r\n')

    for filename in filenames:
        p = multiprocessing.Process(target=show_help_message, args=(filepath + os.path.sep + filename,))
        p.start()
        p.join()
        print('\r\n\r\n' + '*' * 64 + '\r\n')
