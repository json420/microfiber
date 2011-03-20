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

from unittest import TestCase
from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse
import os
from base64 import b32encode
from copy import deepcopy

import microfiber
from microfiber import NotFound, MethodNotAllowed, Conflict, PreconditionFailed


def random_id():
    return b32encode(os.urandom(10)).decode('ascii')


class FakeResponse(object):
    def __init__(self, status, reason, data):
        self.status = status
        self.reason = reason
        self.__data = data

    def read(self):
        return self.__data


class TestFunctions(TestCase):
    def test_queryiter(self):
        f = microfiber.queryiter
        self.assertEqual(
            list(f(foo=True, bar=False, baz=None, aye=10, zee=17.5, key='app')),
            [
                ('aye', 10),
                ('bar', 'false'),
                ('baz', 'null'),
                ('foo', 'true'),
                ('key', 'app'),
                ('zee', 17.5),
            ]
        )

    def test_query(self):
        f = microfiber.query
        self.assertEqual(
            f(foo=True, bar=False, baz=None, aye=10, zee=17.5, key='app'),
            'aye=10&bar=false&baz=null&foo=true&key=app&zee=17.5'
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
        for (status, klass) in microfiber.errors.items():
            self.assertEqual(klass.status, status)
            reason = b32encode(os.urandom(10))
            data = os.urandom(20)
            r = FakeResponse(status, reason, data)
            inst = klass(r, 'MOST', '/restful?and=awesome')
            self.assertIs(inst.response, r)
            self.assertEqual(inst.method, 'MOST')
            self.assertEqual(inst.url, '/restful?and=awesome')
            self.assertEqual(inst.data, data)


class TestCouchCore(TestCase):
    klass = microfiber.CouchCore

    def test_init(self):
        bad = 'sftp://localhost:5984/'
        with self.assertRaises(ValueError) as cm:
            inst = self.klass(bad)
        self.assertEqual(
            str(cm.exception),
            'url scheme must be http or https: {!r}'.format(bad)
        )

        bad = 'http:localhost:5984/foo/bar'
        with self.assertRaises(ValueError) as cm:
            inst = self.klass(bad)
        self.assertEqual(
            str(cm.exception),
            'bad url: {!r}'.format(bad)
        )

        inst = self.klass('https://localhost:5984/db?foo=bar/')
        self.assertEqual(inst.url, 'https://localhost:5984/db/')
        self.assertEqual(inst.basepath, '/db/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('http://localhost:5984?/')
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('http://localhost:5001/')
        self.assertEqual(inst.url, 'http://localhost:5001/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('http://localhost:5002')
        self.assertEqual(inst.url, 'http://localhost:5002/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(inst.url, 'https://localhost:5003/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('https://localhost:5004')
        self.assertEqual(inst.url, 'https://localhost:5004/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

    def test_repr(self):
        inst = self.klass('http://localhost:5001/')
        self.assertEqual(repr(inst), "CouchCore('http://localhost:5001/')")

        inst = self.klass('http://localhost:5002')
        self.assertEqual(repr(inst), "CouchCore('http://localhost:5002/')")

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(repr(inst), "CouchCore('https://localhost:5003/')")

        inst = self.klass('https://localhost:5004')
        self.assertEqual(repr(inst), "CouchCore('https://localhost:5004/')")

    def test_path(self):
        options = dict(
            rev='1-3e812567',
            foo=True,
            bar=None,
        )
        inst = self.klass('http://localhost:5001/')

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



class TestServer(TestCase):
    klass = microfiber.Server

    def test_init(self):
        inst = self.klass()
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('https://localhost:6000')
        self.assertEqual(inst.url, 'https://localhost:6000/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

    def test_repr(self):
        inst = self.klass('http://localhost:5001/')
        self.assertEqual(repr(inst), "Server('http://localhost:5001/')")

        inst = self.klass('http://localhost:5002')
        self.assertEqual(repr(inst), "Server('http://localhost:5002/')")

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(repr(inst), "Server('https://localhost:5003/')")

        inst = self.klass('https://localhost:5004')
        self.assertEqual(repr(inst), "Server('https://localhost:5004/')")


class TestDatabase(TestCase):
    klass = microfiber.Database

    def test_repr(self):
        inst = self.klass('http://localhost:5001/')
        self.assertEqual(repr(inst), "Database('http://localhost:5001/')")

        inst = self.klass('http://localhost:5002')
        self.assertEqual(repr(inst), "Database('http://localhost:5002/')")

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(repr(inst), "Database('https://localhost:5003/')")

        inst = self.klass('https://localhost:5004')
        self.assertEqual(repr(inst), "Database('https://localhost:5004/')")



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



class TestCouchCoreLive(LiveTestCase):

    def test_put_post(self):
        klass = microfiber.CouchCore
        inst = klass(self.url)

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

    def test_save(self):
        inst = self.klass(self.dburl)

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

    def test_bulksave(self):
        inst = self.klass(self.dburl)

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
