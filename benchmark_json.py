#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; mode: python -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: t -*-
# vi: set ft=python sts=4 ts=4 sw=4 noet 
#
# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011-2012 Novacut Inc
#
# This file is part of `microfiber`.
#
# `microfiber` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `microfiber` is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `microfiber`.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#

import json
import platform
import time

count = 20000
keys = 100

obj = dict(
    ('a' * i, 'b' * i) for i in range(1, keys + 1)
)

print('*** Benchmarking json ***')
print('Python: {}, {}, {}'.format(
    platform.python_version(), platform.machine(), platform.system())
)
print('Iterations: {}'.format(count))
print('JSON encoded object size: {} bytes'.format(
    len(json.dumps(obj).encode('utf-8'))
))
print('')

print("json.dumps() + encode('utf-8')")
start = time.time()
for i in range(count):
    json.dumps(obj).encode('utf-8')
elapsed = time.time() - start
print('  Objects per second: {:.0f}'.format(count / elapsed))
print('')

print('json.dumps()')
start = time.time()
for i in range(count):
    json.dumps(obj)
elapsed = time.time() - start
print('  Objects per second: {:.0f}'.format(count / elapsed))
print('')

print('json.dumps(sort_keys=True)')
start = time.time()
for i in range(count):
    json.dumps(obj, sort_keys=True)
elapsed = time.time() - start
print('  Objects per second: {:.0f}'.format(count / elapsed))
print('')

print('json.dumps(ensure_ascii=False)')
start = time.time()
for i in range(count):
    json.dumps(obj, ensure_ascii=False)
elapsed = time.time() - start
print('  Objects per second: {:.0f}'.format(count / elapsed))
print('')

print("decode('utf-8') + json.loads()")
b = json.dumps(obj, sort_keys=True).encode('utf-8')
start = time.time()
for i in range(count):
    json.loads(b.decode('utf-8'))
elapsed = time.time() - start
print('  Objects per second: {:.0f}'.format(count / elapsed))
print('')

print('json.loads()')
s = json.dumps(obj, sort_keys=True)
start = time.time()
for i in range(count):
    json.loads(s)
elapsed = time.time() - start
print('  Objects per second: {:.0f}'.format(count / elapsed))
print('')

