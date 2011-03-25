#!/usr/bin/python3

import os
from base64 import b32encode
import time
import platform

import microfiber

name = 'test_benchmark_microfiber'
count = 1000

s = microfiber.Server()
try:
    s.delete(name)
except microfiber.NotFound:
    pass
db = s.database(name)


def random_id():
    return b32encode(os.urandom(15)).decode('ascii')


master = dict(
    ('a' * i, 'b' * i) for i in range(1, 50)
)
ids = tuple(random_id() for i in range(count))
docs = []

print('*** Benchmarking microfiber ***')
print('Python: {}, {}, {}'.format(
    platform.python_version(), platform.machine(), platform.system())
)

print('Saving {} documents in db {!r}'.format(count, name))
start = time.time()
for _id in ids:
    doc = dict(master)
    doc['_id'] = _id
    db.save(doc)
    docs.append(doc)
elapsed = time.time() - start
print('Seconds: {:.2f}'.format(elapsed))
print('Saves per second: {:.1f}'.format(count / elapsed))
print('')

print('Getting {} documents from db {!r}'.format(count, name))
start = time.time()
for _id in ids:
    db.get(_id)
elapsed = time.time() - start
print('Seconds: {:.2f}'.format(elapsed))
print('Gets per second: {:.1f}'.format(count / elapsed))
print('')

print('Deleting {} documents from db {!r}'.format(count, name))
start = time.time()
for doc in docs:
    db.delete(doc['_id'], rev=doc['_rev'])
elapsed = time.time() - start
print('Seconds: {:.2f}'.format(elapsed))
print('Deletes per second: {:.1f}'.format(count / elapsed))
print('')
