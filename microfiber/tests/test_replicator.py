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
Unit tests for the `microfiber.replicator` module.
"""

from unittest import TestCase
import os
from random import SystemRandom
import time

from dbase32 import random_id
from usercouch.misc import TempCouch

from microfiber import Server, Database, NotFound, Attachment, encode_attachment
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
        tophash1 = db1.get_tophash()
        tophash2 = db2.get_tophash()
        if tophash1 == tophash2:
            return tophash1
        time.sleep(0.5)
    raise Exception(
        'could not achive sync: {} != {}'.format(tophash1, tophash2)
    )


def wait_for_create(db):
    for i in range(30):
        time.sleep(0.5)
        try:
            return db.get()
        except NotFound:
            pass
    raise Exception(
        'db not created: {}'.format(db)
    )


class TestFunctions(TestCase):
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
        self.assertEqual(session, {'src': db1, 'dst': db2})

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
        self.assertEqual(session, {'src': db1, 'dst': db2})

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
        session.pop('update_seq')
        self.assertEqual(replicator.get_missing_changes(session), {})
        self.assertEqual(session,
            {'src': db1, 'dst': db2, 'new_update_seq': 50}
        )
        session['update_seq'] = session.pop('new_update_seq')
        self.assertEqual(replicator.get_missing_changes(session), {})
        self.assertEqual(session,
            {'src': db1, 'dst': db2, 'update_seq': 50, 'new_update_seq': 69}
        )

        # Directly make sure docs actually match:
        self.assertEqual(db1.get_many(ids), db2.get_many(ids))

    def test_replicate(self):
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
            'src_doc': {'_id': '_local/myrep'},
            'dst_doc': {'_id': '_local/myrep'},
            'session_id': 'mysession',
            'doc_count': 0,
        }

        # First try when src is empty:
        self.assertIsNone(replicator.replicate(session))
        self.assertEqual(session, 
            {
                'src': db1,
                'dst': db2,
                'src_doc': {
                    '_id': '_local/myrep',
                    '_rev': '0-1',
                    'session_id': 'mysession',
                    'update_seq': 0,
                },
                'dst_doc': {
                   '_id': '_local/myrep',
                    '_rev': '0-1',
                    'session_id': 'mysession',
                    'update_seq': 0,
                },
                'session_id': 'mysession',
                'doc_count': 0,
                'update_seq': 0,
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
                    '_id': '_local/myrep',
                    '_rev': '0-3',
                    'session_id': 'mysession',
                    'update_seq': 69,
                },
                'dst_doc': {
                   '_id': '_local/myrep',
                    '_rev': '0-3',
                    'session_id': 'mysession',
                    'update_seq': 69,
                },
                'session_id': 'mysession',
                'doc_count': 69,
                'update_seq': 69,
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
                    '_id': '_local/myrep',
                    '_rev': '0-3',
                    'session_id': 'mysession',
                    'update_seq': 69,
                },
                'dst_doc': {
                   '_id': '_local/myrep',
                    '_rev': '0-3',
                    'session_id': 'mysession',
                    'update_seq': 69,
                },
                'session_id': 'mysession',
                'doc_count': 69,
                'update_seq': 69,
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
                    '_id': '_local/myrep',
                    '_rev': '0-5',
                    'session_id': 'mysession',
                    'update_seq': 173,
                },
                'dst_doc': {
                   '_id': '_local/myrep',
                    '_rev': '0-5',
                    'session_id': 'mysession',
                    'update_seq': 173,
                },
                'session_id': 'mysession',
                'doc_count': 138,
                'update_seq': 173,
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
                    '_id': '_local/myrep',
                    '_rev': '0-7',
                    'session_id': 'mysession',
                    'update_seq': 311,
                },
                'dst_doc': {
                   '_id': '_local/myrep',
                    '_rev': '0-7',
                    'session_id': 'mysession',
                    'update_seq': 311,
                },
                'session_id': 'mysession',
                'doc_count': 207,
                'update_seq': 311,
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

        # Check database names on each in for kicks:
        expected = sorted(['_users', '_replicator', name_a, name_b, name_c])
        self.assertEqual(s1.get('_all_dbs'), expected)
        self.assertEqual(s2.get('_all_dbs'), expected)

