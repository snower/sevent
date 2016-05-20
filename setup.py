#!/usr/bin/env python

import os
from setuptools import setup

if os.path.exists("README.md"):
    with open("README.md") as fp:
        long_description = fp.read()
else:
    long_description = ''

setup(
    name = 'sevent',
    version = '0.0.2',
    packages = ['sevent', 'sevent.impl'],
    package_data = {
        'sevent': ['README.md'],
    },
    install_requires = [],
    author = 'snower',
    author_email = 'sujian199@gmail.com',
    url = 'https://github.com/snower/sevent',
    license = 'MIT',
    description = 'lightweight event loop',
    long_description = long_description
)
