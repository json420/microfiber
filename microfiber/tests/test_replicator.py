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

from dbase32 import random_id
from usercouch.misc import TempCouch

from microfiber import Database, Attachment, encode_attachment
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
    doc['_attachments'][random_id()] = encode_attachment(att) 


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
