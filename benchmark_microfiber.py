#!/usr/bin/python3

import os
from base64 import b32encode
import time
import platform
import json
import optparse

import microfiber

name = 'test_benchmark_microfiber'
count = 2000
keys = 50


parser = optparse.OptionParser()
parser.add_option('--dc3',
    help='benchmark against dc3',
    action='store_true',
    default=False,
)
parser.add_option('--basic',
    help='when used with --dc3, forces basic auth',
    action='store_true',
    default=False,
)
(options, args) = parser.parse_args()
if options.dc3:
    env = microfiber.dc3_env()
    if options.basic:
        env['oauth'] = None
else:
    env = microfiber.SERVER


s = microfiber.Server(env)
try:
    s.delete(name)
except microfiber.NotFound:
    pass
db = s.database(name)
db.ensure()


def random_id():
    return b32encode(os.urandom(15)).decode('ascii')


master = dict(
    ('a' * i, 'b' * i) for i in range(1, keys)
)
ids = tuple(random_id() for i in range(count))
docs = []
total = 0

print('*** Benchmarking microfiber ***')
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
    db.delete(doc['_id'], rev=doc['_rev'])
elapsed = time.time() - start
total += elapsed
print('    Seconds: {:.2f}'.format(elapsed))
print('    Deletes per second: {:.1f}'.format(count / elapsed))

print('Total seconds: {:.2f}'.format(total))
print('Total ops per second: {:.1f}'.format((count * 3) / total))
print('')
