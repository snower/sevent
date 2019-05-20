#!/usr/bin/env python

import os
import platform
from setuptools import setup, Extension

if platform.system() != 'Windows' and platform.python_implementation() == "CPython":
    ext_modules = [Extension('sevent/cbuffer', sources=['sevent/cbuffer.c'])]
else:
    ext_modules = []

if os.path.exists("README.md"):
    with open("README.md") as fp:
        long_description = fp.read()
else:
    long_description = ''

setup(
    name = 'sevent',
    version = '0.0.6',
    packages = ['sevent', 'sevent.impl'],
    ext_modules = ext_modules,
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
