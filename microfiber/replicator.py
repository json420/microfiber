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
special documents whose ID starts with "-".  In the context of Novacut and
Dmedia, this is a feature and is largely why we wrote our own replicator.  This
way we can replicate, minus design docs, without having to use a filter function
(which is slow and seems to have reliability issues anyway).

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

from dbase32 import log_id, db32enc, isdb32

from . import dumps, NotFound, BadRequest


log = logging.getLogger()


def build_replication_id(src_id, dst_id):
    """
    Build a replication ID for replicating changes from *src_id* to *dst_id*.

    For example:

    >>> build_replication_id('node-A', 'node-B')
    'T5TDN9EJ5OT4WMUFMRGHW5NAGO54I3M7A94TAMUHMJCSNGJG'

    Note that the replication ID is directional, ie, that A=>B does not get the
    same replication ID as B=>A:

    >>> build_replication_id('node-B', 'node-A')
    'OW76TN3J8X3JCWXCWOGUUSQAWN5KXVIUSVDNNQA9KM8UNBTJ'

    Also note that the resulting ID will be the same whether a replicator
    running on the source is pushing to the destination, or a replicator running
    on the destination is pulling from the source.  The only important thing is
    which is the source and which is the destination, regardless where the
    replicator is running.  In fact, the replicator could be running on a third
    machine altogether.
    """
    assert src_id != dst_id
    info = {
        'src_id': src_id,
        'dst_id': dst_id,
        'replicator': 'microfiber/protocol0',
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


def load_replication_session(src, src_id, dst, dst_id):
    replication_id = build_replication_id(src_id, dst_id)
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
        'session_id': log_id(),  # ID for this new session
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
    return session


def mark_checkpoint(doc, session_id, update_seq):
    doc['session_id'] = session_id
    doc['update_seq'] = update_seq


def save_replication_session(src, dst, session):
    dst.post(None, '_ensure_full_commit')
    session_id = session['session_id']
    update_seq = session['update_seq']
    for (db, key) in [(dst, 'dst_doc'), (src, 'src_doc')]:
        session[key] = db.update(
            mark_checkpoint, session[key], session_id, update_seq
        )
    log.info('checkpoint %s at %d', session['replication_id'], update_seq)


def get_missing_changes(src, dst, kw):
    try:
        r = src.get('_changes', **kw)
    except (OSError, BadRequest):
        log.exception('Exception while waiting for changes feed:')
        return {}
    kw['since'] = r['last_seq']
    changes = {}
    for row in r['results']:
        if row['id'][0] != '_':
            changes[row['id']] = [c['rev'] for c in row['changes']]
    if changes:
        return dst.post(changes, '_revs_diff')
    return {}


def replicate_one_batch(src, dst, kw, session):
    missing = get_missing_changes(src, dst, kw)
    if not missing:
        return 0
    docs = []
    count = 0
    for (_id, info) in missing.items():
        for _rev in info['missing']:
            docs.append(src.get(_id, rev=_rev, attachments=True, revs=True))
            if len(docs) >= 25:
                dst.post({'docs': docs, 'new_edits': False}, '_bulk_docs')
                count += len(docs)
                docs = []
    if docs:
        dst.post({'docs': docs, 'new_edits': False}, '_bulk_docs')
        count += len(docs)
    session['update_seq'] = kw['since']
    save_replication_session(src, dst, session)
    return len(docs)


def replicate_db(src, src_id, dst, dst_id):
    kw = {
        'limit': 500,
        'style': 'all_docs',
        #'feed': 'longpoll',
    }
    session = load_replication_session(src, src_id, dst, dst_id)
    if 'update_seq' in session:
        kw['since'] = session['update_seq']
    last_seq = kw.get('since')
    while True:
        replicate_one_batch(src, dst, kw, session)
        if last_seq == kw['since']:
            break
        last_seq = kw['since']


