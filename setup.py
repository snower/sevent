#!/usr/bin/env python

import os
import sys
import platform
from setuptools import setup, Extension

if platform.system() != 'Windows' and platform.python_implementation() == "CPython":
    ext_modules = [Extension('sevent/cbuffer', sources=['sevent/cbuffer.c'])]
else:
    ext_modules = []

if os.path.exists("README.md"):
    if sys.version_info[0] >= 3:
        with open("README.md", encoding="utf-8") as fp:
            long_description = fp.read()
    else:
        with open("README.md") as fp:
            long_description = fp.read()
else:
    long_description = ''

setup(
    name='sevent',
    version='0.2.6',
    packages=['sevent', 'sevent.impl', 'sevent.coroutines', 'sevent.helpers'],
    ext_modules=ext_modules,
    package_data={
        '': ['README.md'],
    },
    install_requires=[
        'dnslib>=0.9.7',
        'greenlet>=0.4.2',
    ],
    author='snower',
    author_email='sujian199@gmail.com',
    url='https://github.com/snower/sevent',
    license='MIT',
    description='lightweight event loop',
    long_description=long_description,
    long_description_content_type="text/markdown",
)
