#!/usr/bin/python

import sys
import os
from base64 import b32encode
import time
import platform
import json
from subprocess import check_call

import couchdb

name = 'test_benchmark_pycouchdb'
count = 2000
keys = 50

s = couchdb.Server()
try:
    s.delete(name)
except couchdb.ResourceNotFound:
    pass
db = s.create(name)

time.sleep(3)  # Let CouchDB settle a moment
check_call(['/bin/sync'])  # Flush any pending IO so test is more consistent


def random_id():
    return b32encode(os.urandom(15))


master = dict(
    ('a' * i, 'b' * i) for i in xrange(1, keys)
)
ids = tuple(random_id() for i in range(count))
docs = []
total = 0

print('*** Benchmarking python-couchdb ***')
print('Python: {}, {}, {}'.format(
    platform.python_version(), platform.machine(), platform.system())
)

print('  Saving {} documents in db {!r}...'.format(count, name))
start = time.time()
for _id in ids:
    doc = dict(master)
    doc['_id'] = _id
    db.save(doc)
    docs.append(doc)
elapsed = time.time() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Saves per second: {:.1f}'.format(count / elapsed))

print('  Getting {} documents from db {!r}...'.format(count, name))
start = time.time()
for _id in ids:
    db.get(_id)
elapsed = time.time() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Gets per second: {:.1f}'.format(count / elapsed))

print('  Deleting {} documents from db {!r}...'.format(count, name))
start = time.time()
for doc in docs:
    db.delete(doc)
elapsed = time.time() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Deletes per second: {:.1f}'.format(count / elapsed))

print('Total seconds: {:.2f}'.format(total))
print('Total ops per second: {:.1f}'.format((count * 3) / total))
print('')
