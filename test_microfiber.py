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
        inst = self.klass('http://localhost:5001/')
        self.assertEqual(inst.url, 'http://localhost:5001/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('http://localhost:5002')
        self.assertEqual(inst.url, 'http://localhost:5002/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(inst.url, 'https://localhost:5003/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('https://localhost:5004')
        self.assertEqual(inst.url, 'https://localhost:5004/')
        self.assertIsInstance(inst.conn, HTTPConnection)

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

    def test_init(self):
        inst = self.klass()
        self.assertEqual(inst.url, 'http://localhost:5984/_users/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = self.klass('https://localhost:5984/dmedia')
        self.assertEqual(inst.url, 'https://localhost:5984/dmedia/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

    def test_repr(self):
        inst = self.klass('http://localhost:5001/')
        self.assertEqual(repr(inst), "Database('http://localhost:5001/')")

        inst = self.klass('http://localhost:5002')
        self.assertEqual(repr(inst), "Database('http://localhost:5002/')")

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(repr(inst), "Database('https://localhost:5003/')")

        inst = self.klass('https://localhost:5004')
        self.assertEqual(repr(inst), "Database('https://localhost:5004/')")


class TestLive(TestCase):

    def setUp(self):
        self.url = 'http://localhost:5984/'
        self.name = 'test_microfiber'
        t = urlparse(self.url)
        conn = HTTPConnection(t.netloc)
        headers = {'Accept': 'application/json'}
        conn.request('DELETE', self.url + self.name, None, headers)
        r = conn.getresponse()
        r.read()

    def test_CouchCore(self):
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
        self.assertRaises(NotFound, inst.get, self.name)

        # Create DB:
        d = inst.put(None, self.name)
        self.assertEqual(d, {'ok': True})

        # Try to create DB when it already exists:
        self.assertRaises(PreconditionFailed, inst.put, None, self.name)

        # Get DB info:
        d = inst.get(self.name)
        self.assertEqual(d['db_name'], self.name)
        self.assertEqual(d['doc_count'], 0)


        ##############################
        # Test document PUT/GET/DELETE
        _id = random_id()

        # Try getting doc that doesn't exist:
        self.assertRaises(NotFound, inst.get, self.name, _id)

        # Create doc with a put:
        doc = {'foo': 'bar'}
        d = inst.put(doc, self.name, _id)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['ok'], True)
        self.assertEqual(d['id'], _id)
        doc['_rev'] = d['rev']
        doc['_id'] = _id

        # get the doc:
        self.assertEqual(inst.get(self.name, _id), doc)

        # Try creating doc that already exists with put:
        self.assertRaises(Conflict, inst.put, {'no': 'way'}, self.name, _id)

        # Try deleting the doc with *no* revision supplied:
        self.assertRaises(Conflict, inst.delete, self.name, _id)

        # Try deleting the doc with the wrong revision supplied:
        old = doc['_rev']
        doc['stuff'] = 'junk'
        d = inst.put(doc, self.name, _id)
        self.assertRaises(Conflict, inst.delete, self.name, _id, rev=old)

        # Delete the doc
        cur = d['rev']
        d = inst.delete(self.name, _id, rev=cur)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['id'], _id)
        self.assertIs(d['ok'], True)
        self.assertGreater(d['rev'], cur)

        # Try deleting doc that has already been deleted
        self.assertRaises(NotFound, inst.delete, self.name, _id, rev=d['rev'])
        self.assertRaises(NotFound, inst.get, self.name, _id)


        ###############################
        # Test document POST/GET/DELETE
        _id = random_id()
        doc = {
            '_id': _id,
            'naughty': 'nurse',
        }

        # Try getting doc that doesn't exist:
        self.assertRaises(NotFound, inst.get, self.name, _id)

        # Create doc with a post:
        d = inst.post(doc, self.name)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['ok'], True)
        self.assertEqual(d['id'], _id)
        doc['_rev'] = d['rev']

        # get the doc:
        self.assertEqual(inst.get(self.name, _id), doc)

        # Try creating doc that already exists with a post:
        nope = {'_id': _id, 'no': 'way'}
        self.assertRaises(Conflict, inst.post, nope, self.name)

        # Try deleting the doc with *no* revision supplied:
        self.assertRaises(Conflict, inst.delete, self.name, _id)

        # Update the doc:
        old = doc['_rev']
        doc['touch'] = 'bad'
        d = inst.post(doc, self.name)

        # Try updating with wrong revision:
        self.assertRaises(Conflict, inst.post, doc, self.name)

        # Try deleting the doc with the wrong revision supplied:
        self.assertRaises(Conflict, inst.delete, self.name, _id, rev=old)

        # Delete the doc
        cur = d['rev']
        d = inst.delete(self.name, _id, rev=cur)
        self.assertEqual(set(d), set(['id', 'rev', 'ok']))
        self.assertEqual(d['id'], _id)
        self.assertIs(d['ok'], True)
        self.assertGreater(d['rev'], cur)

        # Try deleting doc that has already been deleted
        self.assertRaises(NotFound, inst.delete, self.name, _id, rev=cur)
        self.assertRaises(NotFound, inst.get, self.name, _id)


        #####################
        # Delete the database
        self.assertEqual(inst.delete(self.name), {'ok': True})
        self.assertRaises(NotFound, inst.delete, self.name)
        self.assertRaises(NotFound, inst.get, self.name)
