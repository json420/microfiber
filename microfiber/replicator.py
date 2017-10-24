# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011-2016 Novacut Inc
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


Replication loop
----------------

The low-level replication is done by calling `replicate_one_batch()` over and
over, which will:

    1. GET /src/_changes to get up to 50 changes, optionally from *since*

    2. Transform above changes into format needed by POST /dst/_revs_diff,
       filtering docs whose ID starts with "_" (underscore)

    3. POST /dst/_revs_diff with transformed changes from above, to get a list
       of missing revisions

    4. GET /src/doc for each missing revision

    5. POST /dst/_bulk_docs with docs collected above

    6. Checkpoint session at /src/_local/rep_id and /dst/_local/rep_id


To get started, we need two `TempCouch` instances and a `Database` instance for
each:

>>> from usercouch.misc import TempCouch
>>> from microfiber import Database, dumps
>>> couch1 = TempCouch()
>>> db1 = Database('mydb', couch1.bootstrap())
>>> db1.put(None)  # Create DB
{'ok': True}
>>> couch2 = TempCouch()
>>> db2 = Database('mydb', couch2.bootstrap())
>>> db2.put(None)  # Create DB
{'ok': True}

Now we'll save a few documents in db1:

>>> docs = [
...     {'_id': 'foo'},
...     {'_id': 'bar'},
...     {'_id': 'baz'},
... ]
...
>>> result = db1.save_many(docs)
>>> print(dumps(result, pretty=True))
[
    {
        "id": "foo",
        "ok": true,
        "rev": "1-967a00dff5e02add41819138abb3284d"
    },
    {
        "id": "bar",
        "ok": true,
        "rev": "1-967a00dff5e02add41819138abb3284d"
    },
    {
        "id": "baz",
        "ok": true,
        "rev": "1-967a00dff5e02add41819138abb3284d"
    }
]

>>> changes = db1.get('_changes', style='all_docs', limit=50)
>>> print(dumps(changes, pretty=True))
{
    "last_seq": 3,
    "results": [
        {
            "changes": [
                {
                    "rev": "1-967a00dff5e02add41819138abb3284d"
                }
            ],
            "id": "bar",
            "seq": 1
        },
        {
            "changes": [
                {
                    "rev": "1-967a00dff5e02add41819138abb3284d"
                }
            ],
            "id": "baz",
            "seq": 2
        },
        {
            "changes": [
                {
                    "rev": "1-967a00dff5e02add41819138abb3284d"
                }
            ],
            "id": "foo",
            "seq": 3
        }
    ]
}

The changes feed must be transformed into the format needed by
"POST /db/_revs_diff", which is done using the `changes_for_revs_diff()`
function, for example:

>>> for_revs_diff = changes_for_revs_diff(changes)
>>> print(dumps(for_revs_diff, pretty=True))
{
    "bar": [
        "1-967a00dff5e02add41819138abb3284d"
    ],
    "baz": [
        "1-967a00dff5e02add41819138abb3284d"
    ],
    "foo": [
        "1-967a00dff5e02add41819138abb3284d"
    ]
}

>>> missing = db2.post(for_revs_diff, '_revs_diff')
>>> print(dumps(missing, pretty=True))
{
    "bar": {
        "missing": [
            "1-967a00dff5e02add41819138abb3284d"
        ]
    },
    "baz": {
        "missing": [
            "1-967a00dff5e02add41819138abb3284d"
        ]
    },
    "foo": {
        "missing": [
            "1-967a00dff5e02add41819138abb3284d"
        ]
    }
}

"""

from hashlib import sha512
import logging
import time
import threading

from dbase32 import time_id, db32enc, isdb32

from . import dumps, NotFound, Server


log = logging.getLogger()


BATCH_SIZE = 50
CHECKPOINT_SIZE = BATCH_SIZE * 4


def build_replication_id(src_node, src_db, dst_node, dst_db, mode='push'):
    """
    Build a replication ID.

    For example:

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-FOO')
    'SLAIFEESGWH9C4DASBK4PMGI58F89IQWMI3FKCI6E3P7PSLU'

    Note that the replication ID is directional, ie, that A=>B does not get the
    same replication ID as B=>A:

    >>> build_replication_id('node-B', 'db-FOO', 'node-A', 'db-FOO')
    'D8AD3O8WBA679Q35XVL58CEX3UKEFPD8BNUU6HTX7G3RUVWP'

    Also note that the source and destination database names influence the
    replication ID:

    >>> build_replication_id('node-A', 'db-FOO', 'node-A', 'db-BAR')
    'PSX6IQ49BVSM3O6Q88VJJ4AJCT654IFUBDVYQHR9VE7K3GD6'

    And likewise have the same directional property:

    >>> build_replication_id('node-A', 'db-BAR', 'node-A', 'db-FOO')
    '7WXI38MYJSCLGCIVPX4VY3GFMJY4FYXQCX64V9X655CXXXTV'

    Finally, the ID is different depending on whether your intent is "push" mode
    or "pull" mode:

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-FOO', mode='push')
    'SLAIFEESGWH9C4DASBK4PMGI58F89IQWMI3FKCI6E3P7PSLU'

    Compared to:

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-FOO', mode='pull')
    '9VUNRR98XJVHG7BBU4VUVQW7MDB9HFX8FU7NHPN8UO53AI5L'

    In a nutshell, the hashed JSON Object includes a "replication_node"
    attribute for the ID of the machine the replicator is running on, which
    could actually be a 3rd machine altogether.  However, for now we don't need
    that, so the API just exposes the "push" or "pull" mode keyword argument to
    select either the *src_node* or the *dst_node* as the replicator_node,
    respectively.

    It's tempting to use the same replication ID in each the push and pull
    direction, so that, say, a push replication on the *src_node* could later be
    resumed as pull replication running on the *dst_node*.  However, it's
    prudent for one replicator instance not to trust the work done by another.
    It could be different versions of the software, etc.  In Dmedia in
    particular, we want to use pull replication as an independent mechanism for
    verifying that the push replication is working, and as a fall-back mechanism
    if the push replication fails for any reason.

    Be aware that when the *src_node* and *dst_node* are the same, you can only
    use push mode:

    >>> build_replication_id('node-A', 'db-FOO', 'node-A', 'db-BAR', mode='push')
    'PSX6IQ49BVSM3O6Q88VJJ4AJCT654IFUBDVYQHR9VE7K3GD6'

    Compared to:

    >>> build_replication_id('node-A', 'db-FOO', 'node-A', 'db-BAR', mode='pull')
    Traceback (most recent call last):
      ...
    ValueError: when src_node and dst_node are the same, mode must be 'push'

    When both the *src_node* and *dst_node* are the same, there is no semantic
    difference between push mode and pull mode.  In fact, both would get the
    same replication ID because all attributes, including the "replication_node"
    attribute, will be the same.

    Rather than silently letting you use the API in an ambiguous way, the
    Microfiber replicator raises a ``ValueError``.
    """
    if (src_node, src_db) == (dst_node, dst_db):
        raise ValueError(
            'cannot replicate to self: {!r}'.format((src_node, src_db))
        )
    if mode == 'push':
        replicator_node = src_node
    elif mode == 'pull':
        replicator_node = dst_node
    else:
        raise ValueError("mode must be 'push' or 'pull'; got {!r}".format(mode))
    if src_node == dst_node and mode != 'push':
        raise ValueError(
            "when src_node and dst_node are the same, mode must be 'push'"
        )
    info = {
        'replicator': 'microfiber/protocol0',
        'replicator_node': replicator_node,
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


def load_session(src_id, src, dst_id, dst, mode='push'):
    _id = build_replication_id(src_id, src.name, dst_id, dst.name, mode)
    src_doc = get_checkpoint(src, _id)
    dst.ensure()  # Create destination DB if needed
    dst_doc = get_checkpoint(dst, _id)
    session_id = src_doc.get('session_id')
    src_update_seq = src_doc.get('update_seq')
    dst_update_seq = dst_doc.get('update_seq')
    assert mode in ('push', 'pull')
    if mode == 'push':
        label = '{} => {}{}'.format(src.name, dst.url, dst.name)
    else:
        label = '{} <= {}{}'.format(dst.name, src.url, src.name)
    # Some session state is just to make logging/debugging easier (especially
    # the 'label'):
    session = {
        'src_doc': src_doc,
        'dst_doc': dst_doc,
        'label': label,
    }
    if (
            session_id == dst_doc.get('session_id')
        and isinstance(session_id, str) and isdb32(session_id)
        and len(session_id) == 24
        and isinstance(src_update_seq, int) and src_update_seq > 0
        and isinstance(dst_update_seq, int) and dst_update_seq > 0
    ):
        session['update_seq'] = min(src_update_seq, dst_update_seq)
        log.info('resuming at %d %s', session['update_seq'], session['label'])
    else:
        log.warning('cannot resume replication: %s', dumps(session, True))
        session['update_seq'] = 0
    # Other session state we don't want to log above:
    session['saved_update_seq'] = session['update_seq']
    session['src'] = src
    session['dst'] = dst
    session['session_id'] = time_id()  # ID for this new session
    session['doc_count'] = 0
    return session


def mark_checkpoint(doc, session_id, update_seq):
    assert isdb32(session_id) and len(session_id) == 24
    assert update_seq > 0
    doc['session_id'] = session_id
    doc['update_seq'] = update_seq


def save_session(session, force=False):
    saved_update_seq = session['saved_update_seq']
    update_seq = session['update_seq']
    assert 0 <= saved_update_seq <= update_seq
    if saved_update_seq == update_seq:
        return
    if not (force is True or update_seq - saved_update_seq >= CHECKPOINT_SIZE):
        return

    session_id = session['session_id']
    src = session['src']
    dst = session['dst']
    src_doc = session['src_doc']
    dst_doc = session['dst_doc']

    dst.post(None, '_ensure_full_commit')
    src.update(mark_checkpoint, src_doc, session_id, update_seq)
    dst.update(mark_checkpoint, dst_doc, session_id, update_seq)

    log.info('Checkpoint at %s %s', update_seq, session['label'])
    session['saved_update_seq'] = update_seq


def changes_for_revs_diff(result):
    """
    Transform a _changes feed into form needed for posting to _revs_diff.

    WARNING: This will filter out design documents and any other special docs
    whose ID starts with "_" (underscore).
    """
    changes = {}
    for row in result['results']:
        if row['id'][0] != '_':
            changes[row['id']] = [c['rev'] for c in row['changes']]
    return changes 


def get_missing_changes(session):
    kw = {
        'limit': BATCH_SIZE,
        'style': 'all_docs',
        'since': session['update_seq'],
    }
    feed = session.get('feed')
    if feed:
        assert feed == 'longpoll'
        kw['feed'] = feed
    result = session['src'].get('_changes', **kw)
    session['new_update_seq'] = result['last_seq']
    changes = changes_for_revs_diff(result)
    if changes:
        return session['dst'].post(changes, '_revs_diff')
    return {}


def get_sequence_delta(session):
    update_seq = session['update_seq']
    new_update_seq = session.pop('new_update_seq')
    assert 0 <= update_seq <= new_update_seq
    session['update_seq'] = new_update_seq
    return new_update_seq - update_seq


def replicate_one_batch(session):
    missing = get_missing_changes(session)
    src = session['src']
    docs = []
    for (_id, info) in missing.items():
        kw = {
            'revs': True,
            'attachments': True,
        }
        if 'possible_ancestors' in info:
            # NOTE: info['possible_ancestors'] can seemingly be thousands of
            # items long, and doesn't seem to be constrained by _revs_limit.
            # So for now, we're only included at most the final 10 revisions.
            # This issues has thus far only been encountered with CouchDB 1.6.0,
            # so it might be a regression from 1.5.0.
            kw['atts_since'] = info['possible_ancestors'][-10:]
        for _rev in info['missing']:
            docs.append(src.get(_id, rev=_rev, **kw))
    if docs:
        session['dst'].post({'docs': docs, 'new_edits': False}, '_bulk_docs')
        count = len(docs)
        session['doc_count'] += count
        if count == 1:
            log.info('1 doc %s', session['label'])
        else:
            log.info('%s docs %s', count, session['label'])
    return get_sequence_delta(session)


def replicate(session, timeout=None):
    session.pop('feed', None)
    stop_at_seq = session['src'].get()['update_seq']
    start_time = time.monotonic()
    while replicate_one_batch(session):
        save_session(session)
        if timeout and time.monotonic() - start_time > timeout:
            log.warning('%s second timeout reached %s', timeout, session['label'])
            break
        if session['update_seq'] >= stop_at_seq:
            break
    save_session(session, force=True)
    if session['doc_count'] > 0:
        elapsed = time.monotonic() - start_time
        log.info('%.3fs to replicate %d docs %s',
            elapsed, session['doc_count'], session['label']
        )


def replicate_continuously(session):
    log.info('starting continuous %s', session['label'])
    session['feed'] = 'longpoll'
    while True:
        if replicate_one_batch(session):
            save_session(session)


def replicate_then_replicate_continuously(session):
    replicate(session)
    replicate_continuously(session)


class Replicator:
    def __init__(self, src_env, dst_env, names_filter_func=None, mode='push'):
        self.src = Server(src_env)
        self.src_id = self.src.get()['uuid']  # Is src reachable?
        self.dst = Server(dst_env)
        self.dst_id = self.dst.get()['uuid']  # Is dst reachable?
        assert names_filter_func is None or callable(names_filter_func)
        self.names_filter_func = names_filter_func
        assert mode in ('push', 'pull')
        self.mode = mode
        self.threads = {}

    def get_names(self):
        return sorted(
            filter(self.names_filter_func, self.src.normal_db_names())
        )

    def run(self):
        names = self.get_names()
        self.bring_up(names)
        while True:
            self.monitor_once()

    def monitor_once(self):
        start = time.monotonic()
        self.reap_threads()
        delta = time.monotonic() - start
        if delta < 10:
            time.sleep(10 - delta)
        self.dst.get()  # Is dst reachable?
        names = self.get_names()  # Is src reachable?
        for name in set(names) - set(self.threads):
            self.start_thread(name)

    def bring_up(self, names):
        assert self.threads == {}
        for name in names:
            self.start_thread(name)

    def start_thread(self, name):
        assert name not in self.threads
        src = self.src.database(name)
        dst = self.dst.database(name)
        session = load_session(self.src_id, src, self.dst_id, dst, self.mode)
        thread = threading.Thread(
            target=replicate_then_replicate_continuously,
            args=(session,),
            daemon=True,
        )
        thread.start()
        self.threads[name] = thread

    def reap_threads(self, timeout=1):
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


def _run_replicator(src_env, dst_env, names_filter_func, mode):
    replicator = Replicator(src_env, dst_env, names_filter_func, mode)
    replicator.run()


def start_replicator(src_env, dst_env, names_filter_func=None, mode='push'):
    import multiprocessing
    process = multiprocessing.Process(
        target=_run_replicator,
        args=(src_env, dst_env, names_filter_func, mode),
        daemon=True,
    )
    process.start()
    return process


class TempReplicator:
    def __init__(self, src_env, dst_env, names_filter_func=None, mode='push'):
        self.process = start_replicator(src_env, dst_env, names_filter_func, mode)

    def __del__(self):
        self.terminate()

    def terminate(self):
        if getattr(self, 'process', None) is not None:
            self.process.terminate()
            self.process.join()
            self.process = None

