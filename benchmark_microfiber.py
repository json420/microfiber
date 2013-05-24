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

import time
import platform
import json
from copy import deepcopy
import os

from usercouch.misc import TempCouch
import microfiber

name = 'dmedia-1'
count = 2000


tmpcouch = TempCouch()
db = microfiber.Database(name, tmpcouch.bootstrap())
db.put(None)
time.sleep(3)  # Let CouchDB settle a moment
os.sync()  # Flush any pending IO so test is more consistent


master = {
    "atime": 1355388946,
    "bytes": 25272864,
    "origin": "user",
    "stored": {
        "4MO6W5LTAFVH46EF35TEYPSF": {
            "copies": 1,
            "mtime": 1365931098,
            "verified": 1366680068
        },
        "6E3VG6SKPTEQ7I8UKOYRAFHG": {
            "copies": 1,
            "mtime": 1366653493,
            "verified": 1366943766
        },
        "MA6R4DFC6L7V5RC44JA4R4Q4": {
            "copies": 1,
            "mtime": 1365893318,
            "verified": 1366945534
        },
        "T8XGJCRX8ST6SDLBPAKQ46IR": {
            "copies": 1,
            "mtime": 1366943766,
            "verified": 1367029021
        }
    },
    "time": 1355254766.513135,
    "type": "dmedia/file"
}
ids = tuple(microfiber.random_id(30) for i in range(count))
docs = []
for _id in ids:
    doc = deepcopy(master)
    doc['_id'] = _id
    docs.append(doc)
total = 0

print('*** Benchmarking microfiber ***')
print('Python: {}, {}, {}'.format(
    platform.python_version(), platform.machine(), platform.system())
)

print('  Saving {} documents in db {!r}...'.format(count, name))
start = time.perf_counter()
for doc in docs:
    db.save(doc)
elapsed = time.perf_counter() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Saves per second: {:.1f}'.format(count / elapsed))

print('  Getting {} documents from db {!r}...'.format(count, name))
start = time.perf_counter()
for _id in ids:
    db.get(_id)
elapsed = time.perf_counter() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Gets per second: {:.1f}'.format(count / elapsed))

print('  Deleting {} documents from db {!r}...'.format(count, name))
start = time.perf_counter()
for doc in docs:
    db.delete(doc['_id'], rev=doc['_rev'])
elapsed = time.perf_counter() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Deletes per second: {:.1f}'.format(count / elapsed))

print('Total seconds: {:.2f}'.format(total))
print('Total ops per second: {:.1f}'.format((count * 3) / total))
print('')
