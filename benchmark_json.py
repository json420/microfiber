#!/usr/bin/python3

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

