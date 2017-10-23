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
Unit tests for the `microfiber.replicator` module.
"""

from unittest import TestCase
import os
from random import SystemRandom
import time
from hashlib import sha512

from dbase32 import random_id, db32enc, isdb32
from usercouch.misc import TempCouch

from microfiber import (
    Server,
    Database,
    NotFound,
    Attachment,
    encode_attachment,
    dumps,
)
from microfiber import replicator


random = SystemRandom()


def random_dbname():
    return 'db-' + random_id().lower()


def random_attachment():
    size = random.randint(1, 34969)
    data = os.urandom(size)
    return Attachment('application/octet-stream', data)


def add_random_attachment(doc):
    att = random_attachment()
    if '_attachments' not in doc:
        doc['_attachments'] = {}
    doc['_attachments'][random_id()] = encode_attachment(att)


def replace_attachment_data(doc, name):
    att = random_attachment()
    doc['_attachments'][name] = encode_attachment(att) 


def random_doc(i):
    """
    1/3rd of docs will have an attachment.
    """
    doc = {
        '_id': random_id(30),
        '_attachments': {},
        'i': i,
    }
    if i % 3 == 0:
        add_random_attachment(doc)
    return doc


def wait_for_sync(db1, db2):
    assert db1.url != db2.url
    assert db1.name == db2.name
    for i in range(30):
        time.sleep(0.5)
        tophash1 = db1.get_tophash()
        tophash2 = db2.get_tophash()
        if tophash1 == tophash2:
            return tophash1
    raise Exception(
        'could not achive sync: {} != {}'.format(tophash1, tophash2)
    )


def wait_for_create(db):
    for i in range(30):
        time.sleep(1)
        try:
            return db.get()
        except NotFound:
            pass
    raise Exception(
        'db not created: {}'.format(db)
    )


class TestConstants(TestCase):
    def test_BATCH_SIZE(self):
        BATCH_SIZE = replicator.BATCH_SIZE
        self.assertIs(type(BATCH_SIZE), int)
        self.assertGreaterEqual(BATCH_SIZE, 10)
        self.assertEqual(BATCH_SIZE, 50)

    def test_CHECKPOINT_SIZE(self):
        CHECKPOINT_SIZE = replicator.CHECKPOINT_SIZE
        BATCH_SIZE = replicator.BATCH_SIZE
        self.assertIs(type(CHECKPOINT_SIZE), int)
        self.assertGreaterEqual(CHECKPOINT_SIZE, BATCH_SIZE)
        self.assertEqual(CHECKPOINT_SIZE % BATCH_SIZE, 0)
        self.assertEqual(CHECKPOINT_SIZE, 200)


class TestFunctions(TestCase):
    def test_build_replication_id(self):
        build_replication_id = replicator.build_replication_id
        same_id = id1 = random_id()
        same_name = name1 = random_id()
        id2 = random_id()
        name2 = random_id()

        # src and dst cannot be the same:
        args = (same_id, same_name, same_id, same_name)
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args)
        self.assertEqual(str(cm.exception),
            'cannot replicate to self: {!r}'.format((id1, name1))
        )
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args, mode='push')
        self.assertEqual(str(cm.exception),
            'cannot replicate to self: {!r}'.format((id1, name1))
        )
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args, mode='pull')
        self.assertEqual(str(cm.exception),
            'cannot replicate to self: {!r}'.format((id1, name1))
        )

        # mode must be 'push' or 'pull':
        args = (id1, name1, id2, name2)  # Also used in next section
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args, mode='posh')
        self.assertEqual(str(cm.exception),
            "mode must be 'push' or 'pull'; got 'posh'"
        )
        mode = random_id()
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args, mode=mode)
        self.assertEqual(str(cm.exception),
            "mode must be 'push' or 'pull'; got {!r}".format(mode)
        )

        # Different src and dst nodes, different src and dst DB names:
        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': id1,
            'src_node': id1,
            'src_db': name1,
            'dst_node': id2,
            'dst_db': name2,
        }).encode()
        push_A = db32enc(sha512(data).digest()[:30])

        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': id2,
            'src_node': id1,
            'src_db': name1,
            'dst_node': id2,
            'dst_db': name2,
        }).encode()
        pull_A = db32enc(sha512(data).digest()[:30])

        self.assertNotEqual(push_A, pull_A)
        accum = {push_A, pull_A}

        self.assertEqual(build_replication_id(*args), push_A)
        self.assertEqual(build_replication_id(*args, mode='push'), push_A)
        self.assertEqual(build_replication_id(*args, mode='pull'), pull_A)

        # Different src and dst nodes, same src and dst DB name:
        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': id1,
            'src_node': id1,
            'src_db': same_name,
            'dst_node': id2,
            'dst_db': same_name,
        }).encode()
        push_B = db32enc(sha512(data).digest()[:30])

        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': id2,
            'src_node': id1,
            'src_db': same_name,
            'dst_node': id2,
            'dst_db': same_name,
        }).encode()
        pull_B = db32enc(sha512(data).digest()[:30])

        self.assertNotEqual(push_B, pull_B)
        self.assertNotIn(push_B, accum)
        self.assertNotIn(pull_B, accum)
        accum.update({push_B, pull_B})

        args = (id1, same_name, id2, same_name)
        self.assertEqual(build_replication_id(*args), push_B)
        self.assertEqual(build_replication_id(*args, mode='push'), push_B)
        self.assertEqual(build_replication_id(*args, mode='pull'), pull_B)

        # As above, but flip src and dst nodes:
        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': id2,
            'src_node': id2,
            'src_db': same_name,
            'dst_node': id1,
            'dst_db': same_name,
        }).encode()
        push_C = db32enc(sha512(data).digest()[:30])

        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': id1,
            'src_node': id2,
            'src_db': same_name,
            'dst_node': id1,
            'dst_db': same_name,
        }).encode()
        pull_C = db32enc(sha512(data).digest()[:30])

        self.assertNotEqual(push_C, pull_C)
        self.assertNotIn(push_C, accum)
        self.assertNotIn(pull_C, accum)
        accum.update({push_C, pull_C})

        args = (id2, same_name, id1, same_name)
        self.assertEqual(build_replication_id(*args), push_C)
        self.assertEqual(build_replication_id(*args, mode='push'), push_C)
        self.assertEqual(build_replication_id(*args, mode='pull'), pull_C)

        # Same src and dst node, different src and dst DB names:
        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': same_id,
            'src_node': same_id,
            'src_db': name1,
            'dst_node': same_id,
            'dst_db': name2,
        }).encode()
        D = db32enc(sha512(data).digest()[:30])

        self.assertNotIn(D, accum)
        accum.add(D)

        args = (same_id, name1, same_id, name2)
        self.assertEqual(build_replication_id(*args), D)
        self.assertEqual(build_replication_id(*args, mode='push'), D)
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args, mode='pull')
        self.assertEqual(str(cm.exception),
            "when src_node and dst_node are the same, mode must be 'push'"
        )

        # As above, but flip src and dst DB names:
        data = dumps({
            'replicator': 'microfiber/protocol0',
            'replicator_node': same_id,
            'src_node': same_id,
            'src_db': name2,
            'dst_node': same_id,
            'dst_db': name1,
        }).encode()
        E = db32enc(sha512(data).digest()[:30])

        self.assertNotIn(E, accum)
        accum.add(E)

        args = (same_id, name2, same_id, name1)
        self.assertEqual(build_replication_id(*args), E)
        self.assertEqual(build_replication_id(*args, mode='push'), E)
        with self.assertRaises(ValueError) as cm:
            build_replication_id(*args, mode='pull')
        self.assertEqual(str(cm.exception),
            "when src_node and dst_node are the same, mode must be 'push'"
        )

        # Final sanity check:
        args = (same_id, id1, id2, same_name, name1, name2)
        self.assertEqual(len(set(args)), 4)
        self.assertEqual(set(args), {id1, id2, name1, name2})
        self.assertEqual(same_id, id1)
        self.assertEqual(same_name, name1)

        rep_ids = (pull_A, push_A, pull_B, push_B, pull_C, push_C, D, E)
        self.assertEqual(len(set(rep_ids)), len(rep_ids))
        self.assertEqual(set(rep_ids), accum)
        self.assertEqual(len(rep_ids), 8)

    def test_get_checkpoint(self):
        get_checkpoint = replicator.get_checkpoint

        rep_id = random_id(30)
        local_id = '_local/' + rep_id
        couch = TempCouch()
        db = Database(random_dbname(), couch.bootstrap())

        # Database does not exists:
        self.assertEqual(get_checkpoint(db, rep_id), {'_id': local_id})
        with self.assertRaises(NotFound):
            db.get(local_id)

        # Doc with local_id does not exists:
        self.assertTrue(db.ensure())
        self.assertEqual(get_checkpoint(db, rep_id), {'_id': local_id})
        with self.assertRaises(NotFound):
            db.get(local_id)

        # Doc with local_id exists:
        marker = random_id()
        rev = db.post({'_id': local_id, 'm': marker})['rev']
        self.assertEqual(get_checkpoint(db, rep_id),
            {'_id': local_id, '_rev': rev, 'm': marker}
        )
        self.assertEqual(db.get(local_id),
            {'_id': local_id, '_rev': rev, 'm': marker}
        )

    def test_load_session(self):
        load_session = replicator.load_session

        src_id = random_id()
        src_name = random_dbname()
        dst_id = random_id()
        dst_name = random_dbname()
        push_id = replicator.build_replication_id(src_id, src_name, dst_id, dst_name)
        local_push_id = '_local/' + push_id

        src_couch = TempCouch()
        src = Database(src_name, src_couch.bootstrap())
        dst_couch = TempCouch()
        dst = Database(dst_name, dst_couch.bootstrap())
        push_label = '{} => {}{}'.format(src_name, dst.url, dst_name)

        # Create src Database, but don't create dst Database (so we can make
        # sure load_session() creates dst Database):
        self.assertTrue(src.ensure())
        with self.assertRaises(NotFound):
            dst.get()

        # src checkpoint doc missing, dst database doesn't exist:
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': {'_id': local_push_id},
                'dst_doc': {'_id': local_push_id},
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(dst.get()['db_name'], dst_name)
        with self.assertRaises(NotFound):
            src.get(local_push_id)
        with self.assertRaises(NotFound):
            dst.get(local_push_id)

        # Both src and dst checkpoint docs exist and have same update_seq, but
        # previous session_id doesn't match:
        old_id1 = random_id()
        old_id2 = random_id()
        src_doc = {'_id': local_push_id, 'session_id': old_id1, 'update_seq': 69}
        dst_doc = {'_id': local_push_id, 'session_id': old_id2, 'update_seq': 69}
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Both src and dst checkpoint docs have same session_id, but previous
        # update_seq doesn't match (so the lowest should be choosen):
        old_id = random_id()
        src_doc['session_id'] = old_id
        dst_doc['session_id'] = old_id
        dst_doc['update_seq'] = 42
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 42,
                'saved_update_seq': 42,
            }
        )
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Same as above, but this time dst_doc has the larger update_seq:
        dst_doc['update_seq'] = 3469
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 69,
                'saved_update_seq': 69,
            }
        )
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Both checkpoint docs have same update_seq but are missing session_id:
        del src_doc['session_id']
        del dst_doc['session_id']
        src_doc['update_seq'] = 1776
        dst_doc['update_seq'] = 1776
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Both checkpoint docs have same session_id but are missing update_seq:
        del src_doc['update_seq']
        del dst_doc['update_seq']
        old_id = random_id()
        src_doc['session_id'] = old_id
        dst_doc['session_id'] = old_id
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # The moons again align, both checkpoint docs have matching session_id
        # and update_seq:
        src_doc['update_seq'] = 1776
        dst_doc['update_seq'] = 1776
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst)
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': push_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 1776,
                'saved_update_seq': 1776,
            }
        )
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Sanity check: dst_doc should be one revision ahead but otherwise
        # exactly equal:
        self.assertEqual(src_doc['_rev'], '0-5')
        self.assertEqual(dst_doc['_rev'], '0-6')
        src.save(src_doc)
        self.assertEqual(src.get(local_push_id), src_doc)
        self.assertEqual(dst.get(local_push_id), dst_doc)
        self.assertEqual(src_doc, dst_doc)

        ########################################################################
        # Oh my, we need to do it all again, this time for pull replication:
        self.assertEqual(src.delete(), {'ok': True})
        self.assertEqual(dst.delete(), {'ok': True})

        pull_id = replicator.build_replication_id(
            src_id, src_name, dst_id, dst_name, mode='pull'
        )
        local_pull_id = '_local/' + pull_id
        pull_label = '{} <= {}{}'.format(dst_name, src.url, src_name)

        # Create src Database, but don't create dst Database (so we can make
        # sure load_session() creates dst Database):
        self.assertTrue(src.ensure())
        with self.assertRaises(NotFound):
            dst.get()

        # src checkpoint doc missing, dst database doesn't exist:
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': {'_id': local_pull_id},
                'dst_doc': {'_id': local_pull_id},
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(dst.get()['db_name'], dst_name)
        with self.assertRaises(NotFound):
            src.get(local_pull_id)
        with self.assertRaises(NotFound):
            dst.get(local_pull_id)

        # Both src and dst checkpoint docs exist and have same update_seq, but
        # previous session_id doesn't match:
        old_id1 = random_id()
        old_id2 = random_id()
        src_doc = {'_id': local_pull_id, 'session_id': old_id1, 'update_seq': 69}
        dst_doc = {'_id': local_pull_id, 'session_id': old_id2, 'update_seq': 69}
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Both src and dst checkpoint docs have same session_id, but previous
        # update_seq doesn't match (so the lowest should be choosen):
        old_id = random_id()
        src_doc['session_id'] = old_id
        dst_doc['session_id'] = old_id
        dst_doc['update_seq'] = 42
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 42,
                'saved_update_seq': 42,
            }
        )
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Same as above, but this time dst_doc has the larger update_seq:
        dst_doc['update_seq'] = 3469
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 69,
                'saved_update_seq': 69,
            }
        )
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Both checkpoint docs have same update_seq but are missing session_id:
        del src_doc['session_id']
        del dst_doc['session_id']
        src_doc['update_seq'] = 1776
        dst_doc['update_seq'] = 1776
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Both checkpoint docs have same session_id but are missing update_seq:
        del src_doc['update_seq']
        del dst_doc['update_seq']
        old_id = random_id()
        src_doc['session_id'] = old_id
        dst_doc['session_id'] = old_id
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # The moons again align, both checkpoint docs have matching session_id
        # and update_seq:
        src_doc['update_seq'] = 1776
        dst_doc['update_seq'] = 1776
        src.save(src_doc)
        dst.save(dst_doc)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        self.assertIsInstance(session, dict)
        session_id = session['session_id']
        self.assertIsInstance(session_id, str)
        self.assertTrue(isdb32(session_id))
        self.assertEqual(len(session_id), 24)
        self.assertEqual(session,
            {
                'src_doc': src_doc,
                'dst_doc': dst_doc,
                'label': pull_label,
                'src': src,
                'dst': dst,
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 1776,
                'saved_update_seq': 1776,
            }
        )
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertNotEqual(src_doc, dst_doc)

        # Sanity check: dst_doc should be one revision ahead but otherwise
        # exactly equal:
        self.assertEqual(src_doc['_rev'], '0-5')
        self.assertEqual(dst_doc['_rev'], '0-6')
        src.save(src_doc)
        self.assertEqual(src.get(local_pull_id), src_doc)
        self.assertEqual(dst.get(local_pull_id), dst_doc)
        self.assertEqual(src_doc, dst_doc)

    def test_mark_checkpoint(self):
        _id = random_id()
        doc = {}
        self.assertIsNone(replicator.mark_checkpoint(doc, _id, 1))
        self.assertEqual(doc, {'session_id': _id, 'update_seq': 1})

        doc = {}
        self.assertIsNone(replicator.mark_checkpoint(doc, _id, 2))
        self.assertEqual(doc, {'session_id': _id, 'update_seq': 2})

        seq = random.randrange(0, 10000)
        doc = {}
        self.assertIsNone(replicator.mark_checkpoint(doc, _id, seq))
        self.assertEqual(doc, {'session_id': _id, 'update_seq': seq})

    def test_save_session(self):
        save_session = replicator.save_session

        # Nothing should be done with saved_update_seq == update_seq:
        session = {'saved_update_seq': 0, 'update_seq': 0}
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {'saved_update_seq': 0, 'update_seq': 0})
        self.assertIsNone(save_session(session, force=True))
        self.assertEqual(session, {'saved_update_seq': 0, 'update_seq': 0})

        session = {'saved_update_seq': 1, 'update_seq': 1}
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {'saved_update_seq': 1, 'update_seq': 1})
        self.assertIsNone(save_session(session, force=True))
        self.assertEqual(session, {'saved_update_seq': 1, 'update_seq': 1})

        seq = random.randrange(0, 10000)
        session = {'saved_update_seq': seq, 'update_seq': seq}
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {'saved_update_seq': seq, 'update_seq': seq})
        self.assertIsNone(save_session(session, force=True))
        self.assertEqual(session, {'saved_update_seq': seq, 'update_seq': seq})

        # Setup for remaining tests:
        src_couch = TempCouch()
        dst_couch = TempCouch()
        src = Database(random_dbname(), src_couch.bootstrap())
        dst = Database(random_dbname(), dst_couch.bootstrap())
        self.assertTrue(src.ensure())
        self.assertTrue(dst.ensure())

        session_id = random_id()
        local_id = '_local/' + random_id(30)
        session = {
            'saved_update_seq': 0,
            'update_seq': 0,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {'_id': local_id},
            'dst_doc': {'_id': local_id},
            'label': 'mylabel',
        }

        # Should still do nothing when saved_update_seq == update_seq:
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {
            'saved_update_seq': 0,
            'update_seq': 0,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {'_id': local_id},
            'dst_doc': {'_id': local_id},
            'label': 'mylabel',
        })
        with self.assertRaises(NotFound):
            src.get(local_id)
        with self.assertRaises(NotFound):
            dst.get(local_id)

        # Even with force=True, should not checkpoint:
        self.assertIsNone(save_session(session, force=True))
        self.assertEqual(session, {
            'saved_update_seq': 0,
            'update_seq': 0,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {'_id': local_id},
            'dst_doc': {'_id': local_id},
            'label': 'mylabel',
        })
        with self.assertRaises(NotFound):
            src.get(local_id)
        with self.assertRaises(NotFound):
            dst.get(local_id)

        # Increment update_seq by 1, should not checkpoint without force=True:
        session['update_seq'] += 1
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {
            'saved_update_seq': 0,
            'update_seq': 1,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {'_id': local_id},
            'dst_doc': {'_id': local_id},
            'label': 'mylabel',
        })
        with self.assertRaises(NotFound):
            src.get(local_id)
        with self.assertRaises(NotFound):
            dst.get(local_id)

        # Now should checkpoint when we use force=True:
        self.assertIsNone(save_session(session, force=True))
        self.assertEqual(session, {
            'saved_update_seq': 1,
            'update_seq': 1,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {
                '_id': local_id,
                '_rev': '0-1',
                'session_id': session_id,
                'update_seq': 1,
            },
            'dst_doc': {
                '_id': local_id,
                '_rev': '0-1',
                'session_id': session_id,
                'update_seq': 1,
            },
            'label': 'mylabel',
        })
        self.assertEqual(src.get(local_id), session['src_doc'])
        self.assertEqual(dst.get(local_id), session['dst_doc'])
        self.assertEqual(session['src_doc'], session['dst_doc'])

        # Should not checkpoint as update_seq hasn't changed:
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {
            'saved_update_seq': 1,
            'update_seq': 1,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {
                '_id': local_id,
                '_rev': '0-1',
                'session_id': session_id,
                'update_seq': 1,
            },
            'dst_doc': {
                '_id': local_id,
                '_rev': '0-1',
                'session_id': session_id,
                'update_seq': 1,
            },
            'label': 'mylabel',
        })
        self.assertEqual(src.get(local_id), session['src_doc'])
        self.assertEqual(dst.get(local_id), session['dst_doc'])
        self.assertEqual(session['src_doc'], session['dst_doc'])

        # Increment update_seq by (CHECKPOINT_SIZE - 1), should not checkpoint
        # without force=True:
        session['update_seq'] += (replicator.CHECKPOINT_SIZE - 1)
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {
            'saved_update_seq': 1,
            'update_seq': 200,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {
                '_id': local_id,
                '_rev': '0-1',
                'session_id': session_id,
                'update_seq': 1,
            },
            'dst_doc': {
                '_id': local_id,
                '_rev': '0-1',
                'session_id': session_id,
                'update_seq': 1,
            },
            'label': 'mylabel',
        })
        self.assertEqual(src.get(local_id), session['src_doc'])
        self.assertEqual(dst.get(local_id), session['dst_doc'])
        self.assertEqual(session['src_doc'], session['dst_doc'])

        # Increment update_seq by 1, should now checkpoint even without
        # force=True:
        session['update_seq'] += 1
        self.assertIsNone(save_session(session))
        self.assertEqual(session, {
            'saved_update_seq': 201,
            'update_seq': 201,
            'session_id': session_id,
            'src': src,
            'dst': dst,
            'src_doc': {
                '_id': local_id,
                '_rev': '0-2',
                'session_id': session_id,
                'update_seq': 201,
            },
            'dst_doc': {
                '_id': local_id,
                '_rev': '0-2',
                'session_id': session_id,
                'update_seq': 201,
            },
            'label': 'mylabel',
        })
        self.assertEqual(src.get(local_id), session['src_doc'])
        self.assertEqual(dst.get(local_id), session['dst_doc'])
        self.assertEqual(session['src_doc'], session['dst_doc'])

    def test_get_missing_changes(self):
        # Create two CouchDB instances, a Database for each:
        couch1 = TempCouch()
        db1 = Database('mydb', couch1.bootstrap())
        db1.put(None)  # Create DB1
        couch2 = TempCouch()
        db2 = Database('mydb', couch2.bootstrap())
        db2.put(None)  # Create DB2

        # We'll need a minimal session dict:
        session = {
            'src': db1,
            'dst': db2,
            'update_seq': 0,
        }

        # Create and save some random test docs:
        ids = tuple(random_id() for i in range(69))
        docs = [{'_id': _id} for _id in ids]
        db1.save_many(docs)

        # Note that the seq will be assiged by btree order within each batch
        # that CouchDB saves, so we need to sort by ID here:
        docs.sort(key=lambda d: d['_id'])

        # Verify assumptions about _changes feed with no limit:
        self.assertEqual(db1.get('_changes', style='all_docs'),
            {
                'last_seq': 69,
                'results': [
                    {
                        'seq': i + 1,
                        'id': doc['_id'],
                        'changes': [{'rev': doc['_rev']}],
                    }
                    for (i, doc) in enumerate(docs)
                ]
            }
        )

        #################################
        # Simulate 1st replication batch:

        # Verify assumptions about changes feed as called for 1st batch:
        changes = db1.get('_changes', style='all_docs', limit=50)
        self.assertEqual(changes,
            {
                'last_seq': 50,
                'results': [
                    {
                        'seq': i + 1,
                        'id': doc['_id'],
                        'changes': [{'rev': doc['_rev']}],
                    }
                    for (i, doc) in enumerate(docs[:50])
                ]
            }
        )

        # Verify assumptions about changes_for_revs_diff():
        for_revs_diff = replicator.changes_for_revs_diff(changes)
        self.assertEqual(for_revs_diff,
            dict(
                (doc['_id'], [doc['_rev']])
                for doc in docs[:50]
            )
        )

        # Verify assupmtions about what "POST /db/_revs_diff" will return:
        missing = db2.post(for_revs_diff, '_revs_diff')
        self.assertEqual(missing,
            dict(
                (
                    doc['_id'],
                    {'missing': [doc['_rev']]}
                )
                for doc in docs[:50]
            )
        )

        # make sure get_missing_changes() returns the same missing:
        self.assertEqual(replicator.get_missing_changes(session), missing)
        self.assertEqual(session.pop('new_update_seq'), 50)
        self.assertEqual(session, {'src': db1, 'dst': db2, 'update_seq': 0})

        # Save first 17 docs into db2, test when not all are missing:
        db2.post({'docs': docs[:17], 'new_edits': False}, '_bulk_docs')
        missing = db2.post(for_revs_diff, '_revs_diff')
        self.assertEqual(missing,
            dict(
                (
                    doc['_id'],
                    {'missing': [doc['_rev']]}
                )
                for doc in docs[17:50]
            )
        )
        self.assertEqual(replicator.get_missing_changes(session), missing)
        self.assertEqual(session.pop('new_update_seq'), 50)
        self.assertEqual(session, {'src': db1, 'dst': db2, 'update_seq': 0})

        #################################
        # Simulate 2nd replication batch:

        # Note that since=50 should work the same, regardless whether all of the
        # docs from above were saved, at least in this scenario because we have
        # 69 docs with 1 revision each.  So we above deliberately don't save all
        # the docs from the first batch into db2

        # Verify assumptions about changes feed as called for 2nd batch:
        changes = db1.get('_changes', style='all_docs', limit=50, since=50)
        self.assertEqual(changes,
            {
                'last_seq': 69,
                'results': [
                    {
                        'seq': i + 51,
                        'id': doc['_id'],
                        'changes': [{'rev': doc['_rev']}],
                    }
                    for (i, doc) in enumerate(docs[50:])
                ]
            }
        )

        # Verify assumptions about changes_for_revs_diff():
        for_revs_diff = replicator.changes_for_revs_diff(changes)
        self.assertEqual(for_revs_diff,
            dict(
                (doc['_id'], [doc['_rev']])
                for doc in docs[50:]
            )
        )

        # Verify assupmtions about what "POST /db/_revs_diff" will return:
        missing = db2.post(for_revs_diff, '_revs_diff')
        self.assertEqual(missing,
            dict(
                (
                    doc['_id'],
                    {'missing': [doc['_rev']]}
                )
                for doc in docs[50:]
            )
        )

        # make sure get_missing_changes() returns the same missing:
        session['update_seq'] = 50
        self.assertEqual(replicator.get_missing_changes(session), missing)
        self.assertEqual(session.pop('new_update_seq'), 69)
        self.assertEqual(session, {'src': db1, 'dst': db2, 'update_seq': 50})

        # Save last 3 docs into db2, test when not all are missing:
        db2.post({'docs': docs[-3:], 'new_edits': False}, '_bulk_docs')
        missing = db2.post(for_revs_diff, '_revs_diff')
        self.assertEqual(missing,
            dict(
                (
                    doc['_id'],
                    {'missing': [doc['_rev']]}
                )
                for doc in docs[50:66]
            )
        )
        self.assertEqual(replicator.get_missing_changes(session), missing)
        self.assertEqual(session.pop('new_update_seq'), 69)
        self.assertEqual(session, {'src': db1, 'dst': db2, 'update_seq': 50})

        ###############################################
        # Finally, test with nothing is missing in db2:
        db2.post({'docs': docs, 'new_edits': False}, '_bulk_docs')
        session['update_seq'] = 0
        self.assertEqual(replicator.get_missing_changes(session), {})
        self.assertEqual(session,
            {'src': db1, 'dst': db2, 'update_seq': 0, 'new_update_seq': 50}
        )
        session['update_seq'] = session.pop('new_update_seq')
        self.assertEqual(replicator.get_missing_changes(session), {})
        self.assertEqual(session,
            {'src': db1, 'dst': db2, 'update_seq': 50, 'new_update_seq': 69}
        )

        # Directly make sure docs actually match:
        self.assertEqual(db1.get_many(ids), db2.get_many(ids))

    def test_get_sequence_delta(self):
        get_sequence_delta = replicator.get_sequence_delta

        # update_seq=0:
        session = {'update_seq': 0, 'new_update_seq': 0}
        self.assertEqual(get_sequence_delta(session), 0)
        self.assertEqual(session, {'update_seq': 0})

        session = {'update_seq': 0, 'new_update_seq': 1}
        self.assertEqual(get_sequence_delta(session), 1)
        self.assertEqual(session, {'update_seq': 1})

        session = {'update_seq': 0, 'new_update_seq': 2}
        self.assertEqual(get_sequence_delta(session), 2)
        self.assertEqual(session, {'update_seq': 2})

        # update_seq=1:
        session = {'update_seq': 1, 'new_update_seq': 1}
        self.assertEqual(get_sequence_delta(session), 0)
        self.assertEqual(session, {'update_seq': 1})

        session = {'update_seq': 1, 'new_update_seq': 2}
        self.assertEqual(get_sequence_delta(session), 1)
        self.assertEqual(session, {'update_seq': 2})

        session = {'update_seq': 1, 'new_update_seq': 3}
        self.assertEqual(get_sequence_delta(session), 2)
        self.assertEqual(session, {'update_seq': 3})

        # update_seq=2:
        session = {'update_seq': 2, 'new_update_seq': 2}
        self.assertEqual(get_sequence_delta(session), 0)
        self.assertEqual(session, {'update_seq': 2})

        session = {'update_seq': 2, 'new_update_seq': 3}
        self.assertEqual(get_sequence_delta(session), 1)
        self.assertEqual(session, {'update_seq': 3})

        session = {'update_seq': 2, 'new_update_seq': 4}
        self.assertEqual(get_sequence_delta(session), 2)
        self.assertEqual(session, {'update_seq': 4})

        # random value test:
        seq = random.randrange(0, 10000)
        new_seq = seq + random.randrange(0, 10000)
        session = {'update_seq': seq, 'new_update_seq': new_seq}
        self.assertEqual(get_sequence_delta(session), new_seq - seq)
        self.assertEqual(session, {'update_seq': new_seq})

    def test_replicate(self):
        # Create two CouchDB instances, a Database for each:
        couch1 = TempCouch()
        db1 = Database('mydb', couch1.bootstrap())
        db1.put(None)  # Create DB1
        couch2 = TempCouch()
        db2 = Database('mydb', couch2.bootstrap())
        db2.put(None)  # Create DB2

        # We'll need a minimal session dict:
        session_id = random_id()
        local_id = '_local/' + random_id(30)
        session = {
            'src': db1,
            'dst': db2,
            'src_doc': {'_id': local_id},
            'dst_doc': {'_id': local_id},
            'label': 'mylabel',
            'session_id': session_id,
            'doc_count': 0,
            'update_seq': 0,
            'saved_update_seq': 0,
        }

        # First try when src is empty:
        self.assertIsNone(replicator.replicate(session))
        self.assertEqual(session, 
            {
                'src': db1,
                'dst': db2,
                'src_doc': {'_id': local_id},
                'dst_doc': {'_id': local_id},
                'label': 'mylabel',
                'session_id': session_id,
                'doc_count': 0,
                'update_seq': 0,
                'saved_update_seq': 0,
            }
        )

        # Create and save some random test docs, replicate:
        docs = [random_doc(i) for i in range(69)]
        ids = tuple(d['_id'] for d in docs)
        db1.save_many(docs)
        self.assertIsNone(replicator.replicate(session))
        self.assertEqual(session, 
            {
                'src': db1,
                'dst': db2,
                'src_doc': {
                    '_id': local_id,
                    '_rev': '0-1',
                    'session_id': session_id,
                    'update_seq': 69,
                },
                'dst_doc': {
                   '_id': local_id,
                    '_rev': '0-1',
                    'session_id': session_id,
                    'update_seq': 69,
                },
                'label': 'mylabel',
                'session_id': session_id,
                'doc_count': 69,
                'update_seq': 69,
                'saved_update_seq': 69,
            }
        )
        self.assertEqual(db1.get_many(ids), db2.get_many(ids))

        # Should have no change if run again from current session:
        self.assertIsNone(replicator.replicate(session))
        self.assertEqual(session, 
            {
                'src': db1,
                'dst': db2,
                'src_doc': {
                    '_id': local_id,
                    '_rev': '0-1',
                    'session_id': session_id,
                    'update_seq': 69,
                },
                'dst_doc': {
                   '_id': local_id,
                    '_rev': '0-1',
                    'session_id': session_id,
                    'update_seq': 69,
                },
                'label': 'mylabel',
                'session_id': session_id,
                'doc_count': 69,
                'update_seq': 69,
                'saved_update_seq': 69,
            }
        )
        self.assertEqual(db1.get_many(ids), db2.get_many(ids))

        # Add a random attachment on the first 17 docs:
        for doc in docs[:17]:
            add_random_attachment(doc)
            db1.save(doc)

        # Make a no-change update on all 69 docs:
        db1.save_many(docs)

        # Add a random attachment on the last 18 docs:
        for doc in docs[-18:]:
            add_random_attachment(doc)
            db1.save(doc)

        # Run again
        self.assertIsNone(replicator.replicate(session))
        self.assertEqual(session, 
            {
                'src': db1,
                'dst': db2,
                'src_doc': {
                    '_id': local_id,
                    '_rev': '0-2',
                    'session_id': session_id,
                    'update_seq': 173,
                },
                'dst_doc': {
                   '_id': local_id,
                    '_rev': '0-2',
                    'session_id': session_id,
                    'update_seq': 173,
                },
                'label': 'mylabel',
                'session_id': session_id,
                'doc_count': 138,
                'update_seq': 173,
                'saved_update_seq': 173,
            }
        )
        self.assertEqual(db1.get_many(ids), db2.get_many(ids))

        # Lets try with conflicts:
        docs1 = db1.get_many(ids)
        for doc in docs1:
            doc['marker'] = 'foo'
        db1.save_many(docs1)
        for doc in docs1:
            doc['marker'] = 'bar'
        db1.save_many(docs1)

        docs2 = db2.get_many(ids)
        for doc in docs2:
            doc['marker'] = 'baz'
        db2.save_many(docs2)

        self.assertNotEqual(db1.get_tophash(), db2.get_tophash())
        self.assertIsNone(replicator.replicate(session))
        self.assertEqual(session['doc_count'], 207)
        self.assertEqual(session['update_seq'], 311)
        self.assertEqual(session, 
            {
                'src': db1,
                'dst': db2,
                'src_doc': {
                    '_id': local_id,
                    '_rev': '0-3',
                    'session_id': session_id,
                    'update_seq': 311,
                },
                'dst_doc': {
                   '_id': local_id,
                    '_rev': '0-3',
                    'session_id': session_id,
                    'update_seq': 311,
                },
                'label': 'mylabel',
                'session_id': session_id,
                'doc_count': 207,
                'update_seq': 311,
                'saved_update_seq': 311,
            }
        )
        self.assertEqual(db1.get_many(ids), db2.get_many(ids))
        self.assertEqual(db1.get_tophash(), db2.get_tophash())


class TestReplicator(TestCase):
    def test_live(self):
        # Create two CouchDB instances, a Database for each:
        couch1 = TempCouch()
        couch2 = TempCouch()
        s1 = Server(couch1.bootstrap())
        s2 = Server(couch2.bootstrap())

        name_a = random_dbname()
        name_b = random_dbname()
        name_c = random_dbname()

        ###########################################################
        # Well start with one DB that already existed on both ends:
        # Create 117 docs in db1:
        db1a = s1.database(name_a)
        db2a = s2.database(name_a)
        db1a.put(None)
        db2a.put(None)

        docs1a = [random_doc(i) for i in range(117)]
        db1a.save_many(docs1a)

        # Create 119 docs in db2:
        docs2a = [random_doc(i) for i in range(119)]
        db2a.save_many(docs2a)

        # Get starting tophash of each
        tophash_1a = db1a.get_tophash()
        tophash_2a = db2a.get_tophash()
        self.assertNotEqual(tophash_1a, tophash_2a)

        s1_to_s2 = replicator.TempReplicator(s1.env, s2.env)
        s2_to_s1 = replicator.TempReplicator(s2.env, s1.env)

        tophash_a = wait_for_sync(db1a, db2a)
        self.assertNotIn(tophash_a, {tophash_1a, tophash_2a})
        self.assertEqual(db1a.get_tophash(), tophash_a)
        self.assertEqual(db2a.get_tophash(), tophash_a)

        #########################################################
        # Now create a db on s1, make sure it gets created on s2:
        db1b = s1.database(name_b)
        db2b = s2.database(name_b)
        db1b.put(None)
        db1b.save_many([random_doc(i) for i in range(47)])
        wait_for_create(db2b)
        db2b.save_many([random_doc(i) for i in range(75)])
        db1b.save_many([random_doc(i) for i in range(17)])
        tophash_b = wait_for_sync(db1b, db2b)

        #########################################################
        # Now create a db on s2, make sure it gets created on s1:
        db1c = s1.database(name_c)
        db2c = s2.database(name_c)
        db2c.put(None)
        db2c.save_many([random_doc(i) for i in range(101)])
        wait_for_create(db1c)
        db1c.save_many([random_doc(i) for i in range(75)])
        db2c.save_many([random_doc(i) for i in range(17)])
        tophash_c = wait_for_sync(db1c, db2c)

        ###################################################
        # Shutdown Replicators, create a bunch O conflicts:
        s1_to_s2.terminate()
        s2_to_s1.terminate()

        # a will have no changes

        # b will have chances just on s1 end:
        for doc in db1b.iter_all_docs():
            add_random_attachment(doc)
            db1b.save(doc)
        db1b.save_many([random_doc(i) for i in range(69)])
        tophash_1b = db1b.get_tophash()
        self.assertNotEqual(tophash_1b, tophash_b)

        # c will get changes on both ends:
        docs = list(db1c.iter_all_docs())
        ids = tuple(d['_id'] for d in docs)
        for doc in docs:
            add_random_attachment(doc)
        db1c.save_many(docs)
        for _id in ids:
            doc = db2c.get(_id, attachments=True)
            if '_attachments' not in doc:
                doc['_attachments'] = {}
            for name in doc['_attachments']:
                replace_attachment_data(doc, name)
            add_random_attachment(doc)
            db2c.save(doc)
        tophash_1c = db1c.get_tophash()
        tophash_2c = db2c.get_tophash()
        self.assertNotEqual(tophash_1c, tophash_2c)
        self.assertNotIn(tophash_c, {tophash_1c, tophash_2c})

        # Start replicator back up:
        s1_to_s2 = replicator.TempReplicator(s1.env, s2.env)
        s2_to_s1 = replicator.TempReplicator(s2.env, s1.env)

        # A not have changed:
        self.assertEqual(db1a.get_tophash(), tophash_a)
        self.assertEqual(db2a.get_tophash(), tophash_a)

        # B should have be in the state that db1b was in:
        tophash_b = wait_for_sync(db1b, db2b)
        self.assertEqual(tophash_b, tophash_1b)

        # C should be in new a state neither were in:
        tophash_c = wait_for_sync(db1c, db2c)
        self.assertNotIn(tophash_c, {tophash_1c, tophash_2c})

        # Check database names on each one for kicks:
        expected = sorted([name_a, name_b, name_c])
        for s in [s1, s2]:
            self.assertEqual(list(replicator.iter_normal_names(s)), expected)

