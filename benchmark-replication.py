#!/usr/bin/python3

import time
import logging

from usercouch.misc import TempCouch
from microfiber import Server, Database, dmedia_env
from microfiber.replicator import load_session, replicate


logging.basicConfig(level=logging.DEBUG)


def couchdb(src, dst):
    src.push('dmedia-1', 'dmedia-1', dst.env, create_target=True)


def couchdb_filtered(src, dst):
    src.push('dmedia-1', 'dmedia-1', dst.env, create_target=True, filter='doc/normal')


def microfiber(src, dst):
    src_id = src.get()['uuid']
    dst_id = dst.get()['uuid']
    src_db = src.database('dmedia-1')
    dst_db = dst.database('dmedia-1')
    session = load_session(src_id, src_db, dst_id, dst_db)
    replicate(session)


def benchmark(func, src):
    tmpcouch = TempCouch()
    dst = Server(tmpcouch.bootstrap())
    time.sleep(2)
    start = time.monotonic()
    func(src, dst)
    delta = time.monotonic() - start
    t = (dst.database('dmedia-1').get_tophash(), delta, func.__name__)
    tmpcouch.kill()
    return t


src = Server(dmedia_env())
results = []
results.append(benchmark(couchdb, src))
results.append(benchmark(couchdb_filtered, src))
results.append(benchmark(microfiber, src))

print('')
for t in results:
    print('{} {:.3f} {}'.format(*t))

