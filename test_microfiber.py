# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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

"""
Unit tests for `microfiber` module.
"""

import sys
from unittest import TestCase
import os
from base64 import b64encode, b64decode, b32encode, b32decode
from copy import deepcopy
import json
import time
import io
if sys.version_info >= (3, 0):
    from urllib.parse import urlparse, urlencode
    from http.client import HTTPConnection, HTTPSConnection
else:
    from urlparse import urlparse
    from httplib import HTTPConnection, HTTPSConnection

import microfiber
from microfiber import NotFound, MethodNotAllowed, Conflict, PreconditionFailed


def random_id():
    return b32encode(os.urandom(10)).decode('ascii')


class FakeResponse(object):
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason


class TestFunctions(TestCase):
    def test_random_id(self):
        _id = microfiber.random_id()
        if sys.version_info >= (3, 0):
            self.assertIsInstance(_id, str)
        else:
            self.assertIsInstance(_id, unicode)
        self.assertEqual(len(_id), 24)
        b = b32decode(_id.encode('ascii'))
        self.assertIsInstance(b, bytes)
        self.assertEqual(len(b) * 8, 120)

    def test_random_id2(self):
        _id = microfiber.random_id2()
        if sys.version_info >= (3, 0):
            self.assertIsInstance(_id, str)
        else:
            self.assertIsInstance(_id, unicode)
        self.assertEqual(len(_id), 27)
        (t, r) = _id.split('.')
        self.assertEqual(len(t), 10)
        self.assertTrue(int(t) > 1234567890)
        self.assertEqual(len(r), 16)
        b = b32decode(r.encode('ascii'))
        self.assertIsInstance(b, bytes)
        self.assertEqual(len(b) * 8, 80)

    def test_dumps(self):
        obj = [
            {
                '_id': random_id(),
                '_rev': random_id(),
                'foo': True,
                'bar': None,
                'hello': 17,
                'world': 18.5,
                'index': i,
            }
            for i in range(100)
        ]
        s = json.dumps(obj, sort_keys=True, separators=(',', ':'))
        self.assertEqual(microfiber.dumps(obj), s.encode('UTF-8'))

    def test_loads(self):
        obj = [
            {
                '_id': random_id(),
                '_rev': random_id(),
                'foo': True,
                'bar': None,
                'hello': 17,
                'world': 18.5,
                'index': i,
            }
            for i in range(100)
        ]
        b = json.dumps(obj).encode('utf-8')
        self.assertEqual(microfiber.loads(b), obj)

    def test_queryiter(self):
        f = microfiber.queryiter
        self.assertEqual(
            list(f(foo=True, bar=False, baz=None, aye=10, zee=17.5, key='app')),
            [
                ('aye', '10'),
                ('bar', 'false'),
                ('baz', 'null'),
                ('foo', 'true'),
                ('key', '"app"'),
                ('zee', '17.5'),
            ]
        )
        options = dict(
            rev='2-dedd68efea922add7ae9b22ed5694a73',
            key='foo',
            startkey='bar',
            endkey='baz',
            endkey_docid='V5XXVMUJHR3WKHLLJ4W2UMTL',
            startkey_docid='6BLRBJKV2J3COTUPJCU57UNA',
            group=True,
            group_level=6,
            include_docs=True,
            inclusive_end=False,
            limit=666,
            reduce=False,
            skip=69,
            stale='ok',
            update_seq=True,
        )
        self.assertEqual(
            list(f(**options)),
            [
                ('endkey', '"baz"'),
                ('endkey_docid', 'V5XXVMUJHR3WKHLLJ4W2UMTL'),
                ('group', 'true'),
                ('group_level', '6'),
                ('include_docs', 'true'),
                ('inclusive_end', 'false'),
                ('key', '"foo"'),
                ('limit', '666'),
                ('reduce', 'false'),
                ('rev', '2-dedd68efea922add7ae9b22ed5694a73'),
                ('skip', '69'),
                ('stale', 'ok'),
                ('startkey', '"bar"'),
                ('startkey_docid', '6BLRBJKV2J3COTUPJCU57UNA'),
                ('update_seq', 'true'),
            ]
        )

    def test_query(self):
        f = microfiber.query
        self.assertEqual(
            f(foo=True, bar=False, baz=None, aye=10, zee=17.5, key='app'),
            'aye=10&bar=false&baz=null&foo=true&key=%22app%22&zee=17.5'
        )
        self.assertEqual(
            f(need='some space', bad='and+how', nauhty='you&you&you'),
            'bad=and%2Bhow&nauhty=you%26you%26you&need=some+space'
        )


class TestErrors(TestCase):
    def test_errors(self):
        self.assertEqual(
            microfiber.errors,
            {
                400: microfiber.BadRequest,
                401: microfiber.Unauthorized,
                403: microfiber.Forbidden,
                404: microfiber.NotFound,
                405: microfiber.MethodNotAllowed,
                406: microfiber.NotAcceptable,
                409: microfiber.Conflict,
                412: microfiber.PreconditionFailed,
                415: microfiber.BadContentType,
                416: microfiber.BadRangeRequest,
                417: microfiber.ExpectationFailed,
            }
        )
        method = 'MOST'
        url = '/restful?and=awesome'
        for (status, klass) in microfiber.errors.items():
            reason = b32encode(os.urandom(10))
            data = os.urandom(20)
            r = FakeResponse(status, reason)
            inst = klass(r, data, method, url)
            self.assertIs(inst.response, r)
            self.assertEqual(inst.method, method)
            self.assertEqual(inst.url, url)
            self.assertEqual(inst.data, data)
            self.assertEqual(
                str(inst),
                '{} {}: {} {}'.format(status, reason, method, url)
            )


class TestCouchBase(TestCase):
    klass = microfiber.CouchBase

    def test_init(self):
        bad = 'sftp://localhost:5984/'
        with self.assertRaises(ValueError) as cm:
            inst = self.klass(url=bad)
        self.assertEqual(
            str(cm.exception),
            'url scheme must be http or https: {!r}'.format(bad)
        )

        bad = 'http:localhost:5984/foo/bar'
        with self.assertRaises(ValueError) as cm:
            inst = self.klass(url=bad)
        self.assertEqual(
            str(cm.exception),
            'bad url: {!r}'.format(bad)
        )

        inst = self.klass(url='https://localhost:5984/couch?foo=bar/')
        self.assertEqual(inst.url, 'https://localhost:5984/couch/')
        self.assertEqual(inst.basepath, '/couch/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIsNone(inst.session)

        inst = self.klass(url='http://localhost:5984?/')
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIsNone(inst.session)

        inst = self.klass(url='http://localhost:5001/')
        self.assertEqual(inst.url, 'http://localhost:5001/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIsNone(inst.session)

        inst = self.klass(url='http://localhost:5002')
        self.assertEqual(inst.url, 'http://localhost:5002/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIsNone(inst.session)

        inst = self.klass(url='https://localhost:5003/')
        self.assertEqual(inst.url, 'https://localhost:5003/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIsNone(inst.session)

        inst = self.klass(url='https://localhost:5004')
        self.assertEqual(inst.url, 'https://localhost:5004/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIsNone(inst.session)

        inst = self.klass(session='foo')
        self.assertEqual(inst.session, 'foo')

    def test_path(self):
        options = dict(
            rev='1-3e812567',
            foo=True,
            bar=None,
        )
        inst = self.klass(url='http://localhost:5001/')

        self.assertEqual(inst.path(), '/')
        self.assertEqual(
            inst.path(**options),
            '/?bar=null&foo=true&rev=1-3e812567'
        )

        self.assertEqual(inst.path('db', 'doc', 'att'), '/db/doc/att')
        self.assertEqual(
            inst.path('db', 'doc', 'att', **options),
            '/db/doc/att?bar=null&foo=true&rev=1-3e812567'
        )

        self.assertEqual(inst.path('db/doc/att'), '/db/doc/att')
        self.assertEqual(
            inst.path('db/doc/att', **options),
            '/db/doc/att?bar=null&foo=true&rev=1-3e812567'
        )

    def test_json(self):
        class Subclass(self.klass):
            def request(self, *args):
                self._args = args
                return 'fake response'

        inst = Subclass()

        headers = {'Content-Type': 'application/json'}
        doc = {
            '_id': 'foo',
            'bar': 'baz',
            'hello': 'world',
        }
        json_str = json.dumps(doc, sort_keys=True, separators=(',',':'))
        json_bytes = json_str.encode('utf-8')

        # Test with obj=None
        self.assertEqual(
            inst.json('PUT', None, 'foo', 'bar', okay=True),
            'fake response'
        )
        self.assertEqual(
            inst._args,
            ('PUT', '/foo/bar?okay=true', None, headers)
        )

        # Test when obj is a dict:
        self.assertEqual(
            inst.json('PUT', doc, 'foo', 'bar', okay=True),
            'fake response'
        )
        self.assertEqual(
            inst._args,
            ('PUT', '/foo/bar?okay=true', json_bytes, headers)
        )

        # Test when obj is a pre-encoded str
        self.assertEqual(
            inst.json('PUT', json_str, 'foo', 'bar', okay=True),
            'fake response'
        )
        self.assertEqual(
            inst._args,
            ('PUT', '/foo/bar?okay=true', json_bytes, headers)
        )

        # Test when obj is pre-encoded bytes
        self.assertEqual(
            inst.json('PUT', json_bytes, 'foo', 'bar', okay=True),
            'fake response'
        )
        self.assertEqual(
            inst._args,
            ('PUT', '/foo/bar?okay=true', json_bytes, headers)
        )

        # Test when obj is an io.BytesIO
        obj = io.BytesIO(json_bytes)
        self.assertEqual(
            inst.json('PUT', obj, 'foo', 'bar', okay=True),
            'fake response'
        )
        self.assertEqual(
            inst._args,
            ('PUT', '/foo/bar?okay=true', obj, headers)
        )



class TestServer(TestCase):
    klass = microfiber.Server

    def test_init(self):
        inst = self.klass()
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertNotIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('https://localhost:6000')
        self.assertEqual(inst.url, 'https://localhost:6000/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('http://example.com/foo')
        self.assertEqual(inst.url, 'http://example.com/foo/')
        self.assertEqual(inst.basepath, '/foo/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertNotIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('https://example.com/bar')
        self.assertEqual(inst.url, 'https://example.com/bar/')
        self.assertEqual(inst.basepath, '/bar/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass(session='bar')
        self.assertEqual(inst.session, 'bar')

    def test_repr(self):
        inst = self.klass('http://localhost:5001/')
        self.assertEqual(repr(inst), "Server('http://localhost:5001/')")

        inst = self.klass('http://localhost:5002')
        self.assertEqual(repr(inst), "Server('http://localhost:5002/')")

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(repr(inst), "Server('https://localhost:5003/')")

        inst = self.klass('https://localhost:5004')
        self.assertEqual(repr(inst), "Server('https://localhost:5004/')")

    def test_database(self):
        s = microfiber.Server()
        db = s.database('foo')
        self.assertIsInstance(db, microfiber.Database)
        self.assertIsNone(db.session)

        s = microfiber.Server(session='bar')
        self.assertEqual(s.session, 'bar')
        db = s.database('foo')
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.session, 'bar')


class TestDatabase(TestCase):
    klass = microfiber.Database

    def test_init(self):
        inst = self.klass('foo')
        self.assertEqual(inst.name, 'foo')
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/foo/')

        inst = self.klass('baz', 'https://example.com/bar')
        self.assertEqual(inst.name, 'baz')
        self.assertEqual(inst.url, 'https://example.com/bar/')
        self.assertEqual(inst.basepath, '/bar/baz/')

    def test_repr(self):
        inst = self.klass('dmedia')
        self.assertEqual(
            repr(inst),
            "Database('dmedia', 'http://localhost:5984/')"
        )

        inst = self.klass('novacut', 'https://localhost:5004/')
        self.assertEqual(
            repr(inst),
            "Database('novacut', 'https://localhost:5004/')"
        )

    def test_server(self):
        db = microfiber.Database('foo')
        self.assertIsNone(db.session)
        s = db.server()
        self.assertIsInstance(s, microfiber.Server)
        self.assertIsNone(s.session)

        db = microfiber.Database('foo', session='bar')
        self.assertEqual(db.session, 'bar')
        s = db.server()
        self.assertIsInstance(s, microfiber.Server)
        self.assertEqual(s.session, 'bar')


class LiveTestCase(TestCase):

    def getvar(self, key):
        try:
            return os.environ[key]
        except KeyError:
            self.skipTest('{} not set'.format(key))

    def setUp(self):
        self.url = self.getvar('MICROFIBER_TEST_URL')
        self.db = self.getvar('MICROFIBER_TEST_DB')
        self.dburl = self.url + self.db
        t = urlparse(self.dburl)
        conn = HTTPConnection(t.netloc)
        headers = {'Accept': 'application/json'}
        conn.request('DELETE', t.path, None, headers)
        r = conn.getresponse()
        r.read()


class TestCouchBaseLive(LiveTestCase):
    klass = microfiber.CouchBase

    def test_put_att(self):
        inst = self.klass(url=self.url)

        # Create database
        self.assertEqual(inst.put(None, self.db), {'ok': True})

        mime = 'image/jpeg'
        data = os.urandom(2001)

        # Try to GET attachment that doesn't exist:
        self.assertRaises(NotFound, inst.get_att, self.db, 'doc1', 'att')

        # PUT an attachment
        r = inst.put_att(mime, data, self.db, 'doc1', 'att')
        self.assertEqual(set(r), set(['id', 'rev', 'ok']))
        self.assertEqual(r['id'], 'doc1')
        self.assertEqual(r['ok'], True)

        # GET the doc with attachments=True
        doc = inst.get(self.db, 'doc1', attachments=True)
        self.assertEqual(set(doc), set(['_id', '_rev', '_attachments']))
        self.assertEqual(doc['_id'], 'doc1')
        self.assertEqual(doc['_rev'], r['rev'])
        self.assertEqual(
            doc['_attachments'],
            {
                'att': {
                    'content_type': mime,
                    'data': b64encode(data).decode('ascii'),
                    'revpos': 1,
                },
            }
        )

        # GET the attachment
        self.assertEqual(
            inst.get_att(self.db, 'doc1', 'att'),
            (mime, data)
        )

        # Create new doc with inline attachment:
        new = {
            '_id': 'doc2',
            '_attachments': {
                'att': {
                    'content_type': mime,
                    'data': b64encode(data).decode('ascii'),
                },
            },
        }
        r = inst.post(new, self.db)

        self.assertEqual(set(r), set(['id', 'rev', 'ok']))
        self.assertEqual(r['id'], 'doc2')
        self.assertEqual(r['ok'], True)

        # GET the doc with attachments=true
        doc = inst.get(self.db, 'doc2', attachments=True)
        self.assertEqual(set(doc), set(['_id', '_rev', '_attachments']))
        self.assertEqual(doc['_id'], 'doc2')
        self.assertEqual(doc['_rev'], r['rev'])
        self.assertEqual(
            doc['_attachments'],
            {
                'att': {
                    'content_type': mime,
                    'data': b64encode(data).decode('ascii'),
                    'revpos': 1,
                },
            }
        )

        # GET the attachment:
        self.assertEqual(
            inst.get_att(self.db, 'doc2', 'att'),
            (mime, data)
        )

    def test_put_post(self):
        inst = self.klass(url=self.url)

        ####################
        # Test requests to /
        self.assertRaises(MethodNotAllowed, inst.post, None)
        self.assertRaises(MethodNotAllowed, inst.put, None)
        self.assertRaises(MethodNotAllowed, inst.delete)
        self.assertEqual(
            inst.get(),
            {'couchdb': 'Welcome', 'version': '1.0.1'}
        )


        #####################
        # Create the database

        # Try to get DB when it doesn't exist:
        self.assertRaises(NotFound, inst.get, self.db)

        # Create DB:
        d = inst.put(None, self.db)
        self.assertEqual(d, {'ok': True})

        # Try to create DB when it already exists:
        self.assertRaises(PreconditionFailed, inst.put, None, self.db)

        # Get DB info:
        d = inst.get(self.db)
        self.assertEqual(d['db_name'], self.db)
        self.assertEqual(d['doc_count'], 0)


        ##############################
        # Test document PUT/GET/DELETE
        _id = random_id()

        # Try getting doc that doesn't exist:
        self.assertRaises(NotFound, inst.get, self.db, _id)

        # Create doc with a put:
        doc = {'foo': 'bar'}
        d = inst.put(doc, self.db, _id)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['ok'], True)
        self.assertEqual(d['id'], _id)
        doc['_rev'] = d['rev']
        doc['_id'] = _id

        # get the doc:
        self.assertEqual(inst.get(self.db, _id), doc)

        # Try creating doc that already exists with put:
        self.assertRaises(Conflict, inst.put, {'no': 'way'}, self.db, _id)

        # Try deleting the doc with *no* revision supplied:
        self.assertRaises(Conflict, inst.delete, self.db, _id)

        # Try deleting the doc with the wrong revision supplied:
        old = doc['_rev']
        doc['stuff'] = 'junk'
        d = inst.put(doc, self.db, _id)
        self.assertRaises(Conflict, inst.delete, self.db, _id, rev=old)

        # Delete the doc
        cur = d['rev']
        d = inst.delete(self.db, _id, rev=cur)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['id'], _id)
        self.assertIs(d['ok'], True)
        self.assertGreater(d['rev'], cur)

        # Try deleting doc that has already been deleted
        self.assertRaises(NotFound, inst.delete, self.db, _id, rev=d['rev'])
        self.assertRaises(NotFound, inst.get, self.db, _id)


        ###############################
        # Test document POST/GET/DELETE
        _id = random_id()
        doc = {
            '_id': _id,
            'naughty': 'nurse',
        }

        # Try getting doc that doesn't exist:
        self.assertRaises(NotFound, inst.get, self.db, _id)

        # Create doc with a post:
        d = inst.post(doc, self.db)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['ok'], True)
        self.assertEqual(d['id'], _id)
        doc['_rev'] = d['rev']

        # get the doc:
        self.assertEqual(inst.get(self.db, _id), doc)

        # Try creating doc that already exists with a post:
        nope = {'_id': _id, 'no': 'way'}
        self.assertRaises(Conflict, inst.post, nope, self.db)

        # Try deleting the doc with *no* revision supplied:
        self.assertRaises(Conflict, inst.delete, self.db, _id)

        # Update the doc:
        old = doc['_rev']
        doc['touch'] = 'bad'
        d = inst.post(doc, self.db)

        # Try updating with wrong revision:
        self.assertRaises(Conflict, inst.post, doc, self.db)

        # Try deleting the doc with the wrong revision supplied:
        self.assertRaises(Conflict, inst.delete, self.db, _id, rev=old)

        # Delete the doc
        cur = d['rev']
        d = inst.delete(self.db, _id, rev=cur)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['id'], _id)
        self.assertIs(d['ok'], True)
        self.assertGreater(d['rev'], cur)

        # Try deleting doc that has already been deleted
        self.assertRaises(NotFound, inst.delete, self.db, _id, rev=cur)
        self.assertRaises(NotFound, inst.get, self.db, _id)


        #####################
        # Delete the database
        self.assertEqual(inst.delete(self.db), {'ok': True})
        self.assertRaises(NotFound, inst.delete, self.db)
        self.assertRaises(NotFound, inst.get, self.db)


class TestDatabaseLive(LiveTestCase):
    klass = microfiber.Database

    def test_server(self):
        inst = self.klass('foo')
        s = inst.server()
        self.assertIsInstance(s, microfiber.Server)
        self.assertEqual(s.url, 'http://localhost:5984/')
        self.assertEqual(s.basepath, '/')

        inst = self.klass('baz', 'https://example.com/bar')
        s = inst.server()
        self.assertIsInstance(s, microfiber.Server)
        self.assertEqual(s.url, 'https://example.com/bar/')
        self.assertEqual(s.basepath, '/bar/')

    def test_ensure(self):
        inst = self.klass(self.db, self.url)
        self.assertRaises(NotFound, inst.get)
        self.assertIsNone(inst.ensure())
        self.assertEqual(inst.get()['db_name'], self.db)
        self.assertIsNone(inst.ensure())
        self.assertEqual(inst.delete(), {'ok': True})
        self.assertRaises(NotFound, inst.get)

    def test_save(self):
        inst = self.klass(self.db, self.url)

        self.assertRaises(NotFound, inst.get)
        self.assertEqual(inst.put(None), {'ok': True})
        self.assertEqual(inst.get()['db_name'], self.db)
        self.assertRaises(PreconditionFailed, inst.put, None)

        docs = [{'_id': random_id(), 'foo': i} for i in range(100)]
        for d in docs:
            c = deepcopy(d)
            r = inst.save(d)
            self.assertNotIn('_rev', c)
            self.assertEqual(d['_rev'], r['rev'])

        copy = deepcopy(docs)
        for d in docs:
            d['bar'] = random_id()
            c = deepcopy(d)
            r = inst.save(d)
            self.assertLess(c['_rev'], d['_rev'])
            self.assertEqual(d['_rev'], r['rev'])

        for c in copy:
            self.assertRaises(Conflict, inst.save, c)

        # Test that _id is generated if missing:
        docs = [{'n': i} for i in range(100)]
        for (i, d) in enumerate(docs):
            r = inst.save(d)
            self.assertEqual(set(d), set(['_id', '_rev', 'n']))
            self.assertEqual(d['_id'], r['id'])
            self.assertEqual(len(d['_id']), 24)
            self.assertEqual(d['_rev'], r['rev'])
            self.assertTrue(d['_rev'].startswith('1-'))
            self.assertEqual(d['n'], i)

    def test_bulksave(self):
        inst = self.klass(self.db, self.url)

        self.assertRaises(NotFound, inst.get)
        self.assertEqual(inst.put(None), {'ok': True})
        self.assertEqual(inst.get()['db_name'], self.db)
        self.assertRaises(PreconditionFailed, inst.put, None)

        docs = [{'_id': random_id(), 'foo': i} for i in range(1000)]
        copy = deepcopy(docs)
        rows = inst.bulksave(copy)
        self.assertIsInstance(rows, list)
        for (d, c) in zip(docs, copy):
            self.assertEqual(d['_id'], c['_id'])
            self.assertEqual(d['foo'], c['foo'])
        for (r, c) in zip(rows, copy):
            self.assertEqual(r['id'], c['_id'])
            self.assertEqual(r['rev'], c['_rev'])
            self.assertTrue(c['_rev'].startswith('1-'))

        old = docs
        docs = copy
        for d in docs:
            d['bar'] = random_id()
        copy = deepcopy(docs)
        rows = inst.bulksave(copy)
        for (d, c) in zip(docs, copy):
            self.assertEqual(d['_id'], c['_id'])
            self.assertLess(d['_rev'], c['_rev'])
            self.assertEqual(d['foo'], c['foo'])
            self.assertEqual(d['bar'], c['bar'])
        for (r, c) in zip(rows, copy):
            self.assertEqual(r['id'], c['_id'])
            self.assertEqual(r['rev'], c['_rev'])
            self.assertTrue(c['_rev'].startswith('2-'))

        # FIXME: Is CouchDB 1.0.1 broken in this regard... shouldn't this raise
        # ExpectationFailed?
        inst.bulksave(old)

        # Test compacting the db
        oldsize = inst.get()['disk_size']
        self.assertEqual(inst.post(None, '_compact'), {'ok': True})
        while True:
            time.sleep(1)
            if inst.get()['compact_running'] is False:
                break
        newsize = inst.get()['disk_size']
        self.assertLess(newsize, oldsize)

        # Test that _id is generated if missing:
        docs = [{'n': i} for i in range(1000)]
        rows = inst.bulksave(docs)
        self.assertEqual(len(docs), len(rows))
        i = 0
        for (d, r) in zip(docs, rows):
            self.assertEqual(set(d), set(['_id', '_rev', 'n']))
            self.assertEqual(d['_id'], r['id'])
            self.assertEqual(len(d['_id']), 24)
            self.assertEqual(d['_rev'], r['rev'])
            self.assertTrue(d['_rev'].startswith('1-'))
            self.assertEqual(d['n'], i)
            i += 1
