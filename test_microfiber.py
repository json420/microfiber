"""
Unit tests for `microfiber` module.
"""

from unittest import TestCase
from http.client import HTTPConnection, HTTPSConnection
import os
from base64 import b32encode

import microfiber


class FakeResponse(object):
    def __init__(self, status, reason, data):
        self.status = status
        self.reason = reason
        self.__data = data

    def read(self):
        return self.__data


class TestFunctions(TestCase):
    pass


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
