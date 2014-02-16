# microfiber: fabric for a lightweight Couch
# Copyright (C) 2014 Novacut Inc
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

"""
A CouchDB replicator for Python.

WARNING: This replicator does *not* replicate design documents or any other
special documents whose ID starts with "_" (underscore).  In the context of
Novacut and Dmedia, this is a feature and is largely why we wrote our own
replicator.  This way we can replicate, minus design docs, without having to use
a filter function (which is slow and seems to have reliability issues anyway).

The replication session resume used here is based on the excellent design used
by the rcouch replicator:

    https://github.com/refuge/rcouch/wiki/Replication-Algorithm

Note that although the design is similar, they aren't directly compatible as
they don't calculate the same replication ID and so one could not resume 
replication started by the other.  This is "a good thing" (TM) as each offers
slightly different options and it's not prudent to assume one replicator can
correctly resume a replication session started by another.
"""

from hashlib import sha512
import logging
import time
import threading

from dbase32 import log_id, db32enc, isdb32

from . import dumps, NotFound, BadRequest, Server


log = logging.getLogger()


def build_replication_id(src_node, src_db, dst_node, dst_db):
    """
    Build a replication ID.

    For example:

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-FOO')
    'I9HEKKQC6IUCLKY7B7NC6PD4KFCHP8TRKU4V37AUVSLWS4X3'

    Note that the replication ID is directional, ie, that A=>B does not get the
    same replication ID as B=>A:

    >>> build_replication_id('node-B', 'db-FOO', 'node-A', 'db-FOO')
    '5MRNDPQEJ7WF8QJFF4PVAWGMUGQSOVP4SVCHOPC34G9X48GA'

    Also note that the source and destination database names influence the
    replication ID:

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-BAR')
    'JSU5ACIFAVHYLOF593CPXP6IYT9DEPGFVMKK737ETAYAX8WQ'

    And likewise have the same directional property:

    >>> build_replication_id('node-A', 'db-BAR', 'node-B', 'db-FOO')
    '6CBC7U6VVWARESMEG9D4YTQF78RJRVFDCAPHEC8ONSAKEMW9'

    Finally, note that the resulting ID will be the same whether a replicator
    running on the source is pushing to the destination, or a replicator running
    on the destination is pulling from the source.  The only important thing is
    which is the source and which is the destination, regardless where the
    replicator is running.  In fact, the replicator could be running on a third
    machine altogether.
    """
    assert (src_node, src_db) != (dst_node, dst_db)
    info = {
        'replicator': 'microfiber/protocol0',
        'src_node': src_node,
        'src_db': src_db,
        'dst_node': dst_node,
        'dst_db': dst_db,
    }
    data = dumps(info).encode()
    digest = sha512(data).digest()
    return db32enc(digest[:30])


def get_checkpoint(db, replication_id):
    local_id = '_local/' + replication_id
    try:
        return db.get(local_id)
    except NotFound:
        return {'_id': local_id}


def load_session(src_id, src, dst_id, dst):
    replication_id = build_replication_id(src_id, src.name, dst_id, dst.name)
    src_doc = get_checkpoint(src, replication_id)
    dst.ensure()  # Create destination DB if needed
    dst_doc = get_checkpoint(dst, replication_id)
    session_id = src_doc.get('session_id')
    src_update_seq = src_doc.get('update_seq')
    dst_update_seq = dst_doc.get('update_seq')
    session = {
        'replication_id': replication_id,
        'src_doc': src_doc,
        'dst_doc': dst_doc,
        'session_id': log_id(),  # ID for this new session,
        'doc_count': 0,
    }
    if (
            session_id == dst_doc.get('session_id')
        and isinstance(session_id, str) and isdb32(session_id)
        and isinstance(src_update_seq, int) and src_update_seq > 0
        and isinstance(dst_update_seq, int) and dst_update_seq > 0
    ):
        session['update_seq'] = min(src_update_seq, dst_update_seq)
        log.info('resuming replication session: %s', dumps(session, True))
    else:
        log.warning('cannot resume replication: %s', dumps(session, True))
    session['src'] = src
    session['dst'] = dst
    return session


def mark_checkpoint(doc, session_id, update_seq):
    doc['session_id'] = session_id
    doc['update_seq'] = update_seq


def save_session(session):
    src = session['src']
    dst = session['dst']
    dst.post(None, '_ensure_full_commit')
    session_id = session['session_id']
    update_seq = session['update_seq']
    for (db, key) in [(src, 'src_doc'), (dst, 'dst_doc')]:
        session[key] = db.update(
            mark_checkpoint, session[key], session_id, update_seq
        )
    log.debug('checkpoint %s at %d', session['replication_id'], update_seq)


def get_missing_changes(session):
    kw = {
        'limit': 50,
        'style': 'all_docs',
    }
    if 'feed' in session:
        kw['feed'] = 'longpoll'
    if 'update_seq' in session:
        kw['since'] = session['update_seq']
    r = session['src'].get('_changes', **kw)
    session['new_update_seq'] = r['last_seq']
    changes = {}
    for row in r['results']:
        if row['id'][0] != '_':
            changes[row['id']] = [c['rev'] for c in row['changes']]
    if changes:
        return session['dst'].post(changes, '_revs_diff')
    return {}


def sequence_was_updated(session):
    new_update_seq = session.pop('new_update_seq', None)
    if session.get('update_seq') == new_update_seq:
        return False
    session['update_seq'] = new_update_seq
    return True


def replicate_one_batch(session):
    missing = get_missing_changes(session)
    src = session['src']
    docs = []
    for (_id, info) in missing.items():
        kw = {
            'revs': True,
            'attachments': True,
            'atts_since': [],
        }
        if 'possible_ancestors' in info:
            kw['atts_since'].extend(info['possible_ancestors'])
        for _rev in info['missing']:
            docs.append(src.get(_id, rev=_rev, **kw))
            kw['atts_since'].append(_rev)
    if docs:
        session['dst'].post({'docs': docs, 'new_edits': False}, '_bulk_docs')
        session['doc_count'] += len(docs)
    return sequence_was_updated(session)


def replicate(session):
    log.info('%r => %r', session['src'], session['dst'])
    session.pop('feed', None)
    stop_at_seq = session['src'].get()['update_seq']
    start = time.monotonic()
    while replicate_one_batch(session):
        save_session(session)
        if session['update_seq'] >= stop_at_seq:
            log.info('current update_seq %d >= stop_at_seq %d', 
                session['update_seq'], stop_at_seq 
            )
            break
    elapsed = time.monotonic() - start
    log.info('%.3f seconds to replicate %d docs from %r to %r',
        elapsed, session['doc_count'], session['src'], session['dst']
    )


def replicate_continuously(session):
    log.info('%r => %r', session['src'], session['dst'])
    session['feed'] = 'longpoll'
    while True:
        if replicate_one_batch(session):
            save_session(session)


def iter_normal_names(src):
    for name in src.get('_all_dbs'):
        if not name.startswith('_'):
            yield name


class Replicator:
    def __init__(self, src_env, dst_env, names_filter_func=None):
        self.src = Server(src_env)
        self.src_id = self.src.get()['uuid']
        self.dst = Server(dst_env)
        self.dst_id = self.dst.get()['uuid']
        assert names_filter_func is None or callable(names_filter_func)
        self.names_filter_func = names_filter_func
        self.threads = {}

    def get_names(self):
        return sorted(
            filter(self.names_filter_func, iter_normal_names(self.src))
        )

    def run(self):
        names = self.get_names()
        self.bring_up(names)
        log.info('current replications: %r', sorted(self.threads))
        while True:
            self.monitor_once()

    def monitor_once(self):
        start = time.monotonic()
        self.reap_threads()
        delta = time.monotonic() - start
        if delta < 15:
            time.sleep(15 - delta)
        self.dst.get()  # Make sure we can still reach dst server
        names = self.get_names()  # Will do same of src server
        for name in set(names) - set(self.threads):
            self.restart_thread(name)

    def bring_up(self, names):
        """
        Gracefully do initial sync-up.
        """
        assert self.threads == {}
        for name in names:
            assert name not in self.threads
            src = self.src.database(name)
            dst = self.dst.database(name)
            session = load_session(self.src_id, src, self.dst_id, dst)
            replicate(session)
            thread = threading.Thread(
                target=replicate_continuously,
                args=(session,),
                daemon=True,
            )
            thread.start()
            self.threads[name] = thread

    def reap_threads(self, timeout=2):
        reaped = []
        for name in sorted(self.threads):
            thread = self.threads[name]
            thread.join(timeout=timeout)
            if not thread.is_alive():
                reaped.append(name)
        for name in reaped:
            thread = self.threads.pop(name)
            thread.join()  # Little safety check
            log.warning('reaped thread for %r (possible crash)', name)
        return reaped

    def restart_thread(self, name):
        """
        Start continuous replication in a new thread.

        Unlike the initial bring up, when a replication thread crashes, or when
        a new database is added, we go directly to continuous replication.
        """
        assert name not in self.threads
        src = self.src.database(name)
        dst = self.dst.database(name)
        session = load_session(self.src_id, src, self.dst_id, dst)
        thread = threading.Thread(
            target=replicate_continuously,
            args=(session,),
            daemon=True,
        )
        thread.start()
        self.threads[name] = thread


def _run_replicator(src_env, dst_env, names_filter_func):
    replicator = Replicator(src_env, dst_env, names_filter_func)
    replicator.run()


def start_replicator(src_env, dst_env, names_filter_func=None):
    import multiprocessing
    process = multiprocessing.Process(
        target=_run_replicator,
        args=(src_env, dst_env, names_filter_func),
        daemon=True,
    )
    process.start()
    return process
