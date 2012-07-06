# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011 Novacut Inc
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
Unit tests for `microfiber` module.
"""

from unittest import TestCase
import os
from os import path
from base64 import b64encode, b64decode, b32encode, b32decode
from copy import deepcopy
import json
import time
import io
import tempfile
import shutil
from hashlib import md5
from urllib.parse import urlparse, urlencode
from http.client import HTTPConnection, HTTPSConnection
import threading
from random import SystemRandom

try:
    import usercouch.misc
except ImportError:
    usercouch = None

import microfiber
from microfiber import NotFound, MethodNotAllowed, Conflict, PreconditionFailed


random = SystemRandom()

# OAuth test string from http://oauth.net/core/1.0a/#anchor46
BASE_STRING = 'GET&http%3A%2F%2Fphotos.example.net%2Fphotos&file%3Dvacation.jpg%26oauth_consumer_key%3Ddpf43f3p2l4k3l03%26oauth_nonce%3Dkllo9940pd9333jh%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1191242096%26oauth_token%3Dnnch734d00sl2jdk%26oauth_version%3D1.0%26size%3Doriginal'

B32ALPHABET = frozenset('234567ABCDEFGHIJKLMNOPQRSTUVWXYZ')


def is_microfiber_id(_id):
    assert isinstance(_id, str)
    return (
        len(_id) == microfiber.RANDOM_B32LEN
        and set(_id).issubset(B32ALPHABET)
    )


def random_id():
    return b32encode(os.urandom(10)).decode('ascii')


def random_oauth():
    return dict(
        (k, random_id())
        for k in ('consumer_key', 'consumer_secret', 'token', 'token_secret')
    )


def random_basic():
    return dict(
        (k, random_id())
        for k in ('username', 'password')
    )


def test_id():
    """
    So we can tell our random test IDs from the ones microfiber.random_id()
    makes, we use 160-bit IDs instead of 120-bit.
    """
    return b32encode(os.urandom(20)).decode('ascii')


assert is_microfiber_id(microfiber.random_id())
assert not is_microfiber_id(random_id())
assert not is_microfiber_id(test_id())


class FakeResponse(object):
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason


class TestFunctions(TestCase):

    def test_random_id(self):
        _id = microfiber.random_id()
        self.assertIsInstance(_id, str)
        self.assertEqual(len(_id), 24)
        b = b32decode(_id.encode('ascii'))
        self.assertIsInstance(b, bytes)
        self.assertEqual(len(b) * 8, 120)

    def test_random_id2(self):
        _id = microfiber.random_id2()
        self.assertIsInstance(_id, str)
        self.assertEqual(len(_id), 27)
        (t, r) = _id.split('-')
        self.assertEqual(len(t), 10)
        self.assertTrue(int(t) > 1234567890)
        self.assertEqual(len(r), 16)
        b = b32decode(r.encode('ascii'))
        self.assertIsInstance(b, bytes)
        self.assertEqual(len(b) * 8, 80)

    def test_json_body(self):
        doc = {
            '_id': 'foo',
            'bar': 'baz',
            'hello': 'world',
        }
        json_str = json.dumps(doc, sort_keys=True, separators=(',',':'))
        json_str2 = json.dumps(json_str, sort_keys=True, separators=(',',':'))
        json_bytes = json_str.encode('utf-8')

        # Test with obj=None
        self.assertIsNone(microfiber._json_body(None))

        # Test when obj is a dict:
        self.assertEqual(microfiber._json_body(doc), json_bytes)

        # Test when obj is a pre-dumped str
        self.assertEqual(
            microfiber._json_body(json_str),
            json_str2.encode('utf-8')
        )
        
        # Test other stuff that should get JSON encoded:
        self.assertEqual(microfiber._json_body(True), b'true')
        self.assertEqual(microfiber._json_body(False), b'false')
        self.assertEqual(microfiber._json_body('hello'), b'"hello"')
        self.assertEqual(microfiber._json_body(18), b'18')
        self.assertEqual(microfiber._json_body(17.9), b'17.9')
        self.assertEqual(microfiber._json_body({}), b'{}')
        self.assertEqual(
            microfiber._json_body(['one', True, 3]),
            b'["one",true,3]'
        )
        
        # Test when obj in an open file
        d = tempfile.mkdtemp()
        try:
            f = path.join(d, 'foo.json')
            open(f, 'wb').write(b'["one",true,3]')
            fp = open(f, 'rb')
            self.assertIs(microfiber._json_body(fp), fp)
        finally:
            shutil.rmtree(d)

        # Test when obj is pre-encoded bytes
        self.assertEqual(microfiber._json_body(json_bytes), json_bytes)

    def test_queryiter(self):
        f = microfiber._queryiter
        d = dict(foo=True, bar=False, baz=None, aye=10, zee=17.5, key='app')
        self.assertEqual(
            list(f(d)),
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
            list(f(options)),
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

    def test_oauth_base_string(self):
        f = microfiber._oauth_base_string

        method = 'GET'
        url = 'http://photos.example.net/photos'
        params = {
            'oauth_consumer_key': 'dpf43f3p2l4k3l03',
            'oauth_token': 'nnch734d00sl2jdk',
            'oauth_signature_method': 'HMAC-SHA1',
            'oauth_timestamp': '1191242096',
            'oauth_nonce': 'kllo9940pd9333jh',
            'oauth_version': '1.0',
            'file': 'vacation.jpg',
            'size': 'original',
        }
        self.assertEqual(f(method, url, params), BASE_STRING)

    def test_oauth_sign(self):
        f = microfiber._oauth_sign

        oauth = {
            'consumer_secret': 'kd94hf93k423kf44',
            'token_secret': 'pfkkdhi9sl3r4s00',
        }
        self.assertEqual(
            f(oauth, BASE_STRING),
            'tR3+Ty81lMeYAr/Fid0kMTYa/WM='
        )

    def test_oauth_header(self):
        self.maxDiff = None
        f = microfiber._oauth_header

        oauth = {
            'consumer_secret': 'kd94hf93k423kf44',
            'token_secret': 'pfkkdhi9sl3r4s00',
            'consumer_key': 'dpf43f3p2l4k3l03',
            'token': 'nnch734d00sl2jdk',
        }
        method = 'GET'
        baseurl = 'http://photos.example.net/photos'
        query = {'file': 'vacation.jpg', 'size': 'original'}
        testing = ('1191242096', 'kllo9940pd9333jh')

        expected = ', '.join([
            'OAuth realm=""',
            'oauth_consumer_key="dpf43f3p2l4k3l03"',
            'oauth_nonce="kllo9940pd9333jh"',
            'oauth_signature="tR3%2BTy81lMeYAr%2FFid0kMTYa%2FWM%3D"',
            'oauth_signature_method="HMAC-SHA1"',
            'oauth_timestamp="1191242096"',
            'oauth_token="nnch734d00sl2jdk"',
            'oauth_version="1.0"',
        ])
        self.assertEqual(
            f(oauth, method, baseurl, query, testing),
            {'Authorization': expected},
        )

    def test_basic_auth_header(self):
        f = microfiber._basic_auth_header
        basic = {'username': 'Aladdin', 'password': 'open sesame'}
        self.assertEqual(
            f(basic),
            {'Authorization': 'Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=='}
        )

    def test_replication_body(self):
        src = test_id()
        dst = test_id()
        self.assertEqual(
            microfiber.replication_body(src, dst),
            {
                'source': src,
                'target': dst,
            }
        )
        self.assertEqual(
            microfiber.replication_body(src, dst, continuous=True),
            {
                'source': src,
                'target': dst,
                'continuous': True, 
            }
        )
        self.assertEqual(
            microfiber.replication_body(src, dst, cancel=True),
            {
                'source': src,
                'target': dst,
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.replication_body(src, dst, continuous=True, cancel=True),
            {
                'source': src,
                'target': dst,
                'continuous': True,
                'cancel': True,
            }
        )

    def test_replication_peer(self):
        url = 'http://' + random_id().lower() + ':5984/'
        name = 'db-' + random_id().lower()

        # Test with no auth
        self.assertEqual(
            microfiber.replication_peer(name, {'url': url}),
            {'url': url + name}
        )

        # Test with OAuth
        tokens = random_oauth()
        env = {'url': url, 'oauth': deepcopy(tokens)}
        self.assertEqual(
            microfiber.replication_peer(name, env),
            {
                'url': url + name,
                'auth': {'oauth': tokens},
            }
        )

        # Test with basic HTTP auth
        basic = random_basic()
        headers = microfiber._basic_auth_header(basic)
        env = {'url': url, 'basic': basic}
        self.assertEqual(
            microfiber.replication_peer(name, env),
            {
                'url': url + name,
                'headers': headers,
            }
        )

        # Test that OAuth takes precedence over basic auth
        env['oauth'] = deepcopy(tokens)
        self.assertEqual(
            microfiber.replication_peer(name, env),
            {
                'url': url + name,
                'auth': {'oauth': tokens},
            }
        )

    def test_push_replication(self):
        url = 'http://' + random_id().lower() + ':5984/'
        name = 'db-' + random_id().lower()

        # Test with no auth
        env = {'url': url}
        self.assertEqual(
            microfiber.push_replication(name, env),
            {
                'source': name,
                'target': {
                    'url': url + name,
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                },
                'continuous': True,
                'cancel': True,
            }
        )

        # Test with OAuth
        tokens = random_oauth()
        env = {'url': url, 'oauth': deepcopy(tokens)}
        self.assertEqual(
            microfiber.push_replication(name, env),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
                'cancel': True,
            }
        )

        # Test with basic HTTP auth
        basic = random_basic()
        headers = microfiber._basic_auth_header(basic)
        env = {'url': url, 'basic': basic}
        self.assertEqual(
            microfiber.push_replication(name, env),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'headers': headers,
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'headers': headers,
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'headers': headers,
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'headers': headers,
                },
                'continuous': True,
                'cancel': True,
            }
        )

        # Test that OAuth takes precedence over basic auth
        env['oauth'] = deepcopy(tokens)
        self.assertEqual(
            microfiber.push_replication(name, env),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(name, env, continuous=True, cancel=True),
            {
                'source': name,
                'target': {
                    'url': url + name,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
                'cancel': True,
            }
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


class TestBulkConflict(TestCase):
    def test_init(self):
        conflicts = ['foo', 'bar']
        rows = ['raz', 'jaz']
        inst = microfiber.BulkConflict(conflicts, rows)
        self.assertEqual(str(inst), 'conflict on 2 docs')
        self.assertIs(inst.conflicts, conflicts)
        self.assertIs(inst.rows, rows)

        conflicts = ['hello']
        inst = microfiber.BulkConflict(conflicts, rows)
        self.assertEqual(str(inst), 'conflict on 1 doc')
        self.assertIs(inst.conflicts, conflicts)
        self.assertIs(inst.rows, rows)


class TestCouchBase(TestCase):
    klass = microfiber.CouchBase

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

        inst = self.klass('https://localhost:5984/couch?foo=bar/')
        self.assertEqual(inst.url, 'https://localhost:5984/couch/')
        self.assertEqual(inst.basepath, '/couch/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIs(inst.Conn, HTTPSConnection)
        self.assertIsNone(inst._oauth)
        self.assertIsNone(inst._basic)

        inst = self.klass('http://localhost:5984?/')
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIs(inst.Conn, HTTPConnection)
        self.assertIsNone(inst._oauth)
        self.assertIsNone(inst._basic)

        inst = self.klass('http://localhost:5001/')
        self.assertEqual(inst.url, 'http://localhost:5001/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIs(inst.Conn, HTTPConnection)
        self.assertIsNone(inst._oauth)
        self.assertIsNone(inst._basic)

        inst = self.klass('http://localhost:5002')
        self.assertEqual(inst.url, 'http://localhost:5002/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIs(inst.Conn, HTTPConnection)
        self.assertIsNone(inst._oauth)
        self.assertIsNone(inst._basic)

        inst = self.klass('https://localhost:5003/')
        self.assertEqual(inst.url, 'https://localhost:5003/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIs(inst.Conn, HTTPSConnection)
        self.assertIsNone(inst._oauth)
        self.assertIsNone(inst._basic)

        inst = self.klass('https://localhost:5004')
        self.assertEqual(inst.url, 'https://localhost:5004/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIs(inst.Conn, HTTPSConnection)
        self.assertIsNone(inst._oauth)
        self.assertIsNone(inst._basic)

        inst = self.klass({'oauth': 'foo'})
        self.assertEqual(inst._oauth, 'foo')

        inst = self.klass({'basic': 'bar'})
        self.assertEqual(inst._basic, 'bar')

    def test_conn(self):
        inst = microfiber.CouchBase()
        self.assertIsInstance(inst._threadlocal, threading.local)
        value = random_id()
        inst._threadlocal.conn = value
        self.assertEqual(inst.conn, value)
        delattr(inst._threadlocal, 'conn')
        self.assertIsInstance(inst.conn, HTTPConnection)

    def test_full_url(self):
        inst = self.klass('https://localhost:5003/')
        self.assertEqual(
            inst._full_url('/'),
            'https://localhost:5003/'
        )
        self.assertEqual(
            inst._full_url('/db/doc/att?bar=null&foo=true'),
            'https://localhost:5003/db/doc/att?bar=null&foo=true'
        )

        inst = self.klass('http://localhost:5003/mydb/')
        self.assertEqual(
            inst._full_url('/'),
            'http://localhost:5003/'
        )
        self.assertEqual(
            inst._full_url('/db/doc/att?bar=null&foo=true'),
            'http://localhost:5003/db/doc/att?bar=null&foo=true'
        )


class TestServer(TestCase):
    klass = microfiber.Server

    def test_init(self):
        inst = self.klass()
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIs(inst.Conn, HTTPConnection)
        self.assertNotIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('https://localhost:6000')
        self.assertEqual(inst.url, 'https://localhost:6000/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIs(inst.Conn, HTTPSConnection)

        inst = self.klass('http://example.com/foo')
        self.assertEqual(inst.url, 'http://example.com/foo/')
        self.assertEqual(inst.basepath, '/foo/')
        self.assertIsInstance(inst.conn, HTTPConnection)
        self.assertIs(inst.Conn, HTTPConnection)
        self.assertNotIsInstance(inst.conn, HTTPSConnection)

        inst = self.klass('https://example.com/bar')
        self.assertEqual(inst.url, 'https://example.com/bar/')
        self.assertEqual(inst.basepath, '/bar/')
        self.assertIsInstance(inst.conn, HTTPSConnection)
        self.assertIs(inst.Conn, HTTPSConnection)

        inst = self.klass({'oauth': 'bar'})
        self.assertEqual(inst._oauth, 'bar')

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
        db = s.database('mydb')
        self.assertIsInstance(db, microfiber.Database)
        self.assertIsNone(db._basic)
        self.assertIsNone(db._oauth)
        
        s = microfiber.Server({'basic': 'foo', 'oauth': 'bar'})
        self.assertEqual(s._basic, 'foo')
        self.assertEqual(s._oauth, 'bar')
        db = s.database('mydb')
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(s._basic, 'foo')
        self.assertEqual(s._oauth, 'bar')


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
        db = microfiber.Database('mydb')
        self.assertIsNone(db._basic)
        self.assertIsNone(db._oauth)
        s = db.server()
        self.assertIsInstance(s, microfiber.Server)
        self.assertEqual(s.url, 'http://localhost:5984/')
        self.assertEqual(s.basepath, '/')
        self.assertIsNone(s._basic)
        self.assertIsNone(s._oauth)

        db = microfiber.Database('mydb',
            {'url': 'https://example.com/stuff', 'basic': 'foo', 'oauth': 'bar'}
        )
        self.assertEqual(db._basic, 'foo')
        self.assertEqual(db._oauth, 'bar')
        s = db.server()
        self.assertIsInstance(s, microfiber.Server)
        self.assertEqual(s.url, 'https://example.com/stuff/')
        self.assertEqual(s.basepath, '/stuff/')
        self.assertEqual(s._basic, 'foo')
        self.assertEqual(s._oauth, 'bar')


class ReplicationTestCase(TestCase):
    def setUp(self):
        if os.environ.get('MICROFIBER_TEST_NO_LIVE') == 'true':
            self.skipTest('called with --no-live')
        if usercouch is None:
            self.skipTest('`usercouch` not installed')
        self.tmp1 = usercouch.misc.TempCouch()
        self.env1 = self.tmp1.bootstrap()
        self.tmp2 = usercouch.misc.TempCouch()
        self.env2 = self.tmp2.bootstrap()

    def tearDown(self):
        self.tmp1 = None
        self.env1 = None
        self.tmp2 = None
        self.env2 = None


class TestServerReplication(ReplicationTestCase):
    def test_push(self):
        s1 = microfiber.Server(self.env1)
        s2 = microfiber.Server(self.env2)

        # Create databases
        self.assertEqual(s1.put(None, 'foo'), {'ok': True})
        self.assertEqual(s2.put(None, 'foo'), {'ok': True})

        # Start continuous s1 => s2 replication of foo
        result = s1.push('foo', self.env2, continuous=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s1, make sure they show up in s2
        docs1 = [{'_id': test_id()} for i in range(100)]
        for doc in docs1:
            doc['_rev'] = s1.post(doc, 'foo')['rev']
        time.sleep(1)
        for doc in docs1:
            self.assertEqual(s2.get('foo', doc['_id']), doc)

        # Start continuous s2 => s1 replication of foo
        result = s2.push('foo', self.env1, continuous=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s2, make sure they show up in s1
        docs2 = [{'_id': test_id(), 'two': True} for i in range(100)]
        for doc in docs2:
            doc['_rev'] = s2.post(doc, 'foo')['rev']
        time.sleep(1)
        for doc in docs2:
            self.assertEqual(s1.get('foo', doc['_id']), doc)


class LiveTestCase(TestCase):
    db = 'test_microfiber'

    def setUp(self):
        if os.environ.get('MICROFIBER_TEST_NO_LIVE') == 'true':
            self.skipTest('called with --no-live')
        if usercouch is None:
            self.skipTest('`usercouch` not installed')
        self.auth = os.environ.get('MICROFIBER_TEST_AUTH', 'basic')
        self.tmpcouch = usercouch.misc.TempCouch()
        self.env = self.tmpcouch.bootstrap(self.auth)

    def tearDown(self):
        self.tmpcouch = None
        self.env = None


class TestCouchBaseLive(LiveTestCase):
    klass = microfiber.CouchBase

    def test_bad_status_line(self):
        if os.environ.get('MICROFIBER_TEST_SKIP_SLOW') == 'true':
            self.skipTest('called with --skip-slow')

        inst = self.klass(self.env)

        # Create database
        self.assertEqual(inst.put(None, self.db), {'ok': True})

        # Create a doc:
        inst.put({'hello': 'world'}, self.db, 'bar')

        time.sleep(30)  # The connection should close, raising BadStatusLine

        # Get the doc
        doc = inst.get(self.db, 'bar')

    def test_put_att(self):
        inst = self.klass(self.env)

        # Create database
        self.assertEqual(inst.put(None, self.db), {'ok': True})

        mime = 'image/jpeg'
        data = os.urandom(2001)
        digest = b64encode(md5(data).digest()).decode('utf-8')

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
        self.assertEqual(set(doc['_attachments']), set(['att']))
        att = doc['_attachments']['att']
        self.assertEqual(
            set(att),
            set([
                'content_type',
                'data',
                'digest',
                'revpos',   
            ])
        )
        self.assertEqual(att['content_type'], mime)
        self.assertEqual(att['digest'], 'md5-{}'.format(digest))
        self.assertEqual(att['revpos'], 1)
        self.assertEqual(att['data'], b64encode(data).decode('utf-8'))

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
                    'digest': 'md5-{}'.format(digest),
                },
            }
        )

        # GET the attachment:
        self.assertEqual(
            inst.get_att(self.db, 'doc2', 'att'),
            (mime, data)
        )

    def test_put_post(self):
        inst = self.klass(self.env)

        ####################
        # Test requests to /
        self.assertRaises(MethodNotAllowed, inst.post, None)
        self.assertRaises(MethodNotAllowed, inst.put, None)
        self.assertRaises(MethodNotAllowed, inst.delete)
        ret = inst.get()
        self.assertIsInstance(ret, dict)
        self.assertEqual(set(ret), set(['couchdb', 'version']))
        self.assertEqual(ret['couchdb'], 'Welcome')
        self.assertIsInstance(ret['version'], str)

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

    def test_ensure(self):
        inst = self.klass(self.db, self.env)
        self.assertRaises(NotFound, inst.get)
        self.assertTrue(inst.ensure())
        self.assertEqual(inst.get()['db_name'], self.db)
        self.assertFalse(inst.ensure())
        self.assertEqual(inst.delete(), {'ok': True})
        self.assertRaises(NotFound, inst.get)

    def test_save(self):
        inst = self.klass(self.db, self.env)

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

    def test_bulk_non_atomic(self):
        """
        Verify our assumptions about CouchDB "non-atomic" bulk semantics.

        Results: conflicting docs are not updated, and we know which docs were
        conflicting; non-conflicting doc get updated normally.

        Pro tip: use this!
        """
        db = microfiber.Database(self.db, self.env)
        db.ensure()
        db.post({'_id': 'example'})
        me = db.get('example')
        you = db.get('example')
        self.assertEqual(me,
            {
                '_id': 'example',
                '_rev': '1-967a00dff5e02add41819138abb3284d',
            }
        )
        self.assertEqual(me, you)

        # you make a change, creating a conflict for me
        you['x'] = 'foo'
        db.save(you)
        self.assertEqual(db.get('example'),
            {
                '_id': 'example',
                '_rev': '2-047387155f2bb8c7cd80b0a5da505e9a',
                'x': 'foo',
            }
        )

        # me makes a change, what happens?
        me['y'] = 'bar'
        rows = db.post({'docs': [me]}, '_bulk_docs')
        self.assertEqual(
            rows,
            [{'id': 'example', 'error': 'conflict', 'reason': 'Document update conflict.'}]
        )
        self.assertEqual(db.get('example'),
            {
                '_id': 'example',
                '_rev': '2-047387155f2bb8c7cd80b0a5da505e9a',
                'x': 'foo',
            }
        )

    def test_bulk_all_or_nothing(self):
        """
        Verify our assumptions about CouchDB "all-or-nothing" bulk semantics.

        Results: subtle and surprising, unlikely what you want!  Totally
        different behavior when both ends are at the same revision number vs
        when one is ahead in revision number!

        For example, in this case the last change wins:

            1. Sue and Ann both get the "1-" rev of the "foo" doc
            2. Sue saves/bulksaves a change in "foo", now at rev "2-"
            3. Ann bulksaves a change in "foo"
            4. Ann has the winning "2-" rev of "foo"

        But in this case, something totally different happens:

            1. Sue and Ann both get the "1-" rev of the "foo" doc
            2. Sue saves/bulksaves a change in "foo", now at rev "2-"
            3. Sue saves/bulksaves a *2nd* change in "foo", now at rev "3-"
            4. Ann bulksaves a change in "foo"
            5. Ann thinks she has the winning "2-" rev of "foo", but Ann didn't
               make the last change according to rest of the world, and worse,
               Ann thinks her "2-" rev is the lastest, when it's actually "3-"

        Pro tip: these are not the semantics you're looking for!
        """
        db = microfiber.Database(self.db, self.env)
        db.ensure()
        db.post({'_id': 'example'})
        me = db.get('example')
        you = db.get('example')
        self.assertEqual(me,
            {
                '_id': 'example',
                '_rev': '1-967a00dff5e02add41819138abb3284d',
            }
        )
        self.assertEqual(me, you)

        # you make a change, creating a conflict for me
        you['x'] = 'foo'
        db.save(you)
        self.assertEqual(db.get('example'),
            {
                '_id': 'example',
                '_rev': '2-047387155f2bb8c7cd80b0a5da505e9a',
                'x': 'foo',
            }
        )

        # me makes a change, what happens?
        me['y'] = 'bar'
        rows = db.post({'docs': [me], 'all_or_nothing': True}, '_bulk_docs')
        self.assertEqual(
            rows,
            [{'id': 'example', 'rev': '2-34e30c39538299cfed3958f6692f794d'}]
        )
        self.assertEqual(db.get('example'),
            {
                '_id': 'example',
                '_rev': '2-34e30c39538299cfed3958f6692f794d',
                'y': 'bar',
            }
        )

        # Seems like reasonable last-one-wins, right? Not so fast! Let's try
        # another example:
        db.post({'_id': 'example2'})
        me = db.get('example2')
        you = db.get('example2')
        self.assertEqual(me,
            {
                '_id': 'example2',
                '_rev': '1-967a00dff5e02add41819138abb3284d',
            }
        )
        self.assertEqual(me, you)

        # you make *two* changes, creating a conflict for me
        you['x'] = 'foo'
        db.save(you)
        self.assertEqual(db.get('example2'),
            {
                '_id': 'example2',
                '_rev': '2-047387155f2bb8c7cd80b0a5da505e9a',
                'x': 'foo',
            }
        )
        db.save(you)
        self.assertEqual(db.get('example2'),
            {
                '_id': 'example2',
                '_rev': '3-074e07f92324e448702162e585e718fb',
                'x': 'foo',
            }
        )

        # me makes a change, what happens?
        me['y'] = 'bar'
        rows = db.post({'docs': [me], 'all_or_nothing': True}, '_bulk_docs')
        self.assertEqual(
            rows,
            [{'id': 'example2', 'rev': '2-34e30c39538299cfed3958f6692f794d'}]
        )
        self.assertEqual(db.get('example2'),
            {
                '_id': 'example2',
                '_rev': '3-074e07f92324e448702162e585e718fb',
                'x': 'foo',
            }
        )

    def test_bulksave(self):
        db = microfiber.Database(self.db, self.env)
        self.assertTrue(db.ensure())

        # Test that doc['_id'] gets set automatically
        markers = tuple(test_id() for i in range(10))
        docs = [{'marker': m} for m in markers]
        rows = db.bulksave(docs)
        for (marker, doc, row) in zip(markers, docs, rows):
            self.assertEqual(doc['marker'], marker)
            self.assertEqual(doc['_id'], row['id'])
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertTrue(is_microfiber_id(doc['_id']))

        # Test when doc['_id'] is already present
        ids = tuple(test_id() for i in range(10))
        docs = [{'_id': _id} for _id in ids]
        rows = db.bulksave(docs)
        for (_id, doc, row) in zip(ids, docs, rows):
            self.assertEqual(doc['_id'], _id)
            self.assertEqual(row['id'], _id)
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(db.get(_id), doc)

        # Let's update all the docs
        for doc in docs:
            doc['x'] = 'foo'    
        rows = db.bulksave(docs)
        for (_id, doc, row) in zip(ids, docs, rows):
            self.assertEqual(doc['_id'], _id)
            self.assertEqual(row['id'], _id)
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['x'], 'foo')
            self.assertEqual(db.get(_id), doc)

        # Let's update half the docs out-of-band to create conflicts
        for (i, doc) in enumerate(docs):
            if i % 2 == 0:
                d = deepcopy(doc)
                d['x'] = 'gotcha'
                db.post(d)

        # Now let's update all the docs, test for BulkConflict
        good = []
        bad = []
        for (i, doc) in enumerate(docs):
            doc['x'] = 'bar'
            if i % 2 == 0:
                bad.append(doc)
            else:
                good.append(doc)

        with self.assertRaises(microfiber.BulkConflict) as cm:
            rows = db.bulksave(docs)
        self.assertEqual(str(cm.exception), 'conflict on 5 docs')
        self.assertEqual(cm.exception.conflicts, bad)
        self.assertEqual(len(cm.exception.rows), 10)
        for (i, row) in enumerate(cm.exception.rows):
            _id = ids[i]
            doc = docs[i]
            real = db.get(_id)
            self.assertEqual(row['id'], _id)
            self.assertTrue(real['_rev'].startswith('3-'))
            if i % 2 == 0:
                self.assertEqual(real['x'], 'gotcha')
                self.assertEqual(doc['x'], 'bar')
                self.assertNotIn('rev', row)
                self.assertTrue(doc['_rev'].startswith('2-'))
            else:
                self.assertEqual(real['x'], 'bar')
                self.assertEqual(row['rev'], doc['_rev'])
                self.assertEqual(real, doc)

    def test_bulksave2(self):
        db = microfiber.Database(self.db, self.env)
        self.assertTrue(db.ensure())

        # Test that doc['_id'] gets set automatically
        markers = tuple(test_id() for i in range(10))
        docs = [{'marker': m} for m in markers]
        rows = db.bulksave2(docs)
        for (marker, doc, row) in zip(markers, docs, rows):
            self.assertEqual(doc['marker'], marker)
            self.assertEqual(doc['_id'], row['id'])
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertTrue(is_microfiber_id(doc['_id']))

        # Test when doc['_id'] is already present
        ids = tuple(test_id() for i in range(10))
        docs = [{'_id': _id} for _id in ids]
        rows = db.bulksave2(docs)
        for (_id, doc, row) in zip(ids, docs, rows):
            self.assertEqual(doc['_id'], _id)
            self.assertEqual(row['id'], _id)
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(db.get(_id), doc)

        # Let's update all the docs
        for doc in docs:
            doc['x'] = 'foo'    
        rows = db.bulksave2(docs)
        for (_id, doc, row) in zip(ids, docs, rows):
            self.assertEqual(doc['_id'], _id)
            self.assertEqual(row['id'], _id)
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('2-'))
            self.assertEqual(doc['x'], 'foo')
            self.assertEqual(db.get(_id), doc)

        # Let's update half the docs out-of-band to create conflicts
        for (i, doc) in enumerate(docs):
            if i % 2 == 0:
                d = deepcopy(doc)
                d['x'] = 'gotcha'
                db.save(d)

        # Now let's update all the docs, test all-or-nothing behavior
        for doc in docs:
            doc['x'] = 'bar'    
        rows = db.bulksave2(docs)
        for (_id, doc, row) in zip(ids, docs, rows):
            self.assertEqual(doc['_id'], _id)
            self.assertEqual(row['id'], _id)
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('3-'))
            self.assertEqual(doc['x'], 'bar')
            self.assertEqual(db.get(_id), doc)

        # Again update half the docs out-of-band, but this time saving twice
        # so the conflict is ahead in revision number:
        for (i, doc) in enumerate(docs):
            if i % 2 == 0:
                d = deepcopy(doc)
                d['x'] = 'gotcha'
                db.save(d)
                db.save(d)
                self.assertTrue(d['_rev'].startswith('5-'))

        # Now update all the docs again, realize all-or-nothing is a bad idea:
        for doc in docs:
            doc['x'] = 'baz'    
        rows = db.bulksave2(docs)
        for (i, row) in enumerate(rows):
            _id = ids[i]
            doc = docs[i]
            real = db.get(_id)
            self.assertEqual(row['id'], _id)
            if i % 2 == 0:
                self.assertEqual(real['x'], 'gotcha')
                self.assertTrue(real['_rev'].startswith('5-'))
                self.assertTrue(row['rev'].startswith('4-'))
            else:
                self.assertEqual(real['x'], 'baz')
                self.assertTrue(real['_rev'].startswith('4-'))
                self.assertEqual(row['rev'], real['_rev'])
                self.assertEqual(doc, real)

    def test_get_many(self):
        db = microfiber.Database(self.db, self.env)
        self.assertTrue(db.ensure())

        ids = tuple(test_id() for i in range(50))
        docs = [{'_id': _id} for _id in ids]
        db.bulksave(docs)

        # Test an empty doc_ids list
        self.assertEqual(db.get_many([]), [])

        # Test a get_many on all the docs
        self.assertEqual(db.get_many(ids), docs)

        # Test with some random subsets
        rdocs = random.sample(docs, 40)
        self.assertEqual(db.get_many([d['_id'] for d in rdocs]), rdocs)

        rdocs = random.sample(docs, 20)
        self.assertEqual(db.get_many([d['_id'] for d in rdocs]), rdocs)

        rdocs = random.sample(docs, 10)
        self.assertEqual(db.get_many([d['_id'] for d in rdocs]), rdocs)

        # Test with duplicate ids
        self.assertEqual(
            db.get_many([ids[7], ids[7], ids[7]]),
            [docs[7], docs[7], docs[7]]
        )
            
