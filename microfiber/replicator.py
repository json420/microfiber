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

from dbase32 import log_id, db32enc, isdb32

from . import dumps, NotFound, BadRequest, Server


log = logging.getLogger()


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

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-BAR')
    'RDJXJIY6R8JNDRMVBI3VYDUN8IF76VNOT66CVICJDE3Y6XQG'

    And likewise have the same directional property:

    >>> build_replication_id('node-A', 'db-BAR', 'node-B', 'db-FOO')
    '4FYW5LTBNFWJKBDJ8TGIBG9ERVG7RJ7936SM696FSO6RY5J6'

    Finally, the ID is different depending on whether your intent is "push" mode
    or "pull" mode:

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-FOO', mode='push')
    'SLAIFEESGWH9C4DASBK4PMGI58F89IQWMI3FKCI6E3P7PSLU'

    >>> build_replication_id('node-A', 'db-FOO', 'node-B', 'db-FOO', mode='pull')
    '9VUNRR98XJVHG7BBU4VUVQW7MDB9HFX8FU7NHPN8UO53AI5L'

    In a nutshell, the hashed JSON Object includes a "replication_node"
    attribute for the ID of the machine the replicator is running on, which
    could actually be a 3rd machine altogether.  However, for now we don't need
    that, so the API just exposes the 'push' or 'pull' mode flag to select
    either the *src_node* or the *dst_node* as the replicator_node,
    respectively.

    It's tempting to use the same replication ID in each the push and pull
    direction, so that, say, a push replication on the *src_node* could later be
    resumed as pull replication running on the *dst_node*.  However, it's
    prudent for one replicator not to trust the work done by another.  It could
    be different versions of the software, etc.  In Dmedia in particular, we
    want to use pull replication as independent mechanism for verifying that the
    push replication is working, and as a fall-back mechanism if the push
    replication fails for any reason.
    """
    assert (src_node, src_db) != (dst_node, dst_db)
    if mode == 'push':
        replicator_node = src_node
    elif mode == 'pull':
        replicator_node = dst_node
    else:
        raise ValueError("mode must be 'push' or 'pull'; got {!r}".format(mode))
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

    # Some session state is just to make logging/debugging easier:
    session = {
        'src_fqn': '{}.{}'.format(src_id, src.name),
        'src_doc': src_doc,
        'dst_doc': dst_doc,
        'dst_fqn': '{}.{}'.format(dst_id, dst.name),
    }
    if (
            session_id == dst_doc.get('session_id')
        and isinstance(session_id, str) and isdb32(session_id)
        and isinstance(src_update_seq, int) and src_update_seq > 0
        and isinstance(dst_update_seq, int) and dst_update_seq > 0
    ):
        session['update_seq'] = min(src_update_seq, dst_update_seq)
    else:
        log.warning('cannot resume replication: %s', dumps(session, True))
    # Other session state we don't want to log above:
    session['src'] = src
    session['dst'] = dst
    session['session_id'] = log_id()  # ID for this new session
    session['doc_count'] = 0
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
        'limit': 50,
        'style': 'all_docs',
    }
    if 'feed' in session:
        kw['feed'] = 'longpoll'
    if 'update_seq' in session:
        kw['since'] = session['update_seq']
    result = session['src'].get('_changes', **kw)
    session['new_update_seq'] = result['last_seq']
    changes = changes_for_revs_diff(result)
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
    log.info('replicate: %s =>  %s', session['src'], session['dst'])
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
    log.info('%.3fs to replicate %d docs from %r to %r',
        elapsed, session['doc_count'], session['src'], session['dst']
    )


def replicate_continuously(session):
    log.info('replicate_continuously: %s =>  %s',
        session['src_fqn'],
        session['dst_fqn'],
    )
    session['feed'] = 'longpoll'
    last = time.monotonic()
    while True:
        if replicate_one_batch(session):
            now = time.monotonic()
            if now - last > 30:
                last = now
                save_session(session)
                log.info('checkpoint at %d: %s =>  %s',
                    session['update_seq'],
                    session['src_fqn'],
                    session['dst_fqn'],
                )


def replicate_then_replicate_continuously(session):
    replicate(session)
    replicate_continuously(session)


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
        log.info('initial replications: %r', names)
        self.bring_up(names)
        while True:
            self.monitor_once()

    def monitor_once(self):
        start = time.monotonic()
        self.reap_threads()
        delta = time.monotonic() - start
        if delta < 5:
            time.sleep(5 - delta)
        self.dst.get()  # Make sure we can still reach dst server
        names = self.get_names()  # Will do same for src server
        for name in set(names) - set(self.threads):
            self.restart_thread(name)

    def bring_up(self, names):
        """
        Gracefully do initial sync-up.

        With how Dmedia historically used the CouchDB replicator, there was a
        crashing tidal wave of parallel connections and requests when a peer
        first came online.  There were a number of problems with this:

            1. Bad user experience - the CPU fans tend to kick up on a laptop,
               there's a lot of IO, and as CouchDB is very fsync happy, other
               applications can get very unresponsive while waiting for writes
               to complete during this initial sync-up

            2. Poor connection reuse - now that we've enabled perfect-forward-
               secrecy (and with ``ssl.OP_SINGLE_ECDH_USE`` at  that), creating
               connections is quite expensive; however, once the connection is
               created, there's surprisingly little overhead when making, say,
               hundreds of HTTP requests through that same SSL connection

            3. No way to prioritize what gets synced first - above all else, we
               want to get "dmedia-1" in sync as quickly as possible, and we
               don't want the syncing of other DBs to slow this down
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

    def restart_thread(self, name):
        """
        Start continuous replication in a new thread.
        """
        assert name not in self.threads
        src = self.src.database(name)
        dst = self.dst.database(name)
        session = load_session(self.src_id, src, self.dst_id, dst)
        thread = threading.Thread(
            target=replicate_then_replicate_continuously,
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


class TempReplicator:
    def __init__(self, src_env, dst_env, names_filter_func=None):
        self.process = start_replicator(src_env, dst_env, names_filter_func)

    def __del__(self):
        self.terminate()

    def terminate(self):
        if getattr(self, 'process', None) is not None:
            self.process.terminate()
            self.process.join()
            self.process = None

