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

print('*** Benchmarking microfiber ***')
print('Python: {}, {}, {}'.format(
    platform.python_version(), platform.machine(), platform.system())
)
print('Saving {} documents in db {!r}'.format(count, name))
start = time.time()
for i in range(count):
    doc = dict(master)
    doc['_id'] = random_id()
    db.save(doc)

elapsed = time.time() - start
print('Seconds: {:.2f}'.format(elapsed))
print('Saves per second: {:.1f}'.format(count / elapsed))
print('')
