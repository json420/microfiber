# -*- coding: utf-8; tab-width: 4; mode: python -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: t -*-
# vi: set ft=python sts=4 ts=4 sw=4 noet 
#
# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011-2012 Novacut Inc
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
import gzip
import time
import io
import tempfile
import shutil
from hashlib import md5
from urllib.parse import urlparse, urlencode
from http.client import HTTPConnection, HTTPSConnection
import ssl
import threading
from random import SystemRandom
from usercouch.misc import TempCouch, TempCerts

import microfiber
from microfiber import random_id
from microfiber import NotFound, MethodNotAllowed, Conflict, PreconditionFailed


random = SystemRandom()
B32ALPHABET = frozenset('234567ABCDEFGHIJKLMNOPQRSTUVWXYZ')


# OAuth 1.0A test vector from http://oauth.net/core/1.0a/#anchor46

SAMPLE_OAUTH_TOKENS = (
    ('consumer_secret', 'kd94hf93k423kf44'),
    ('token_secret', 'pfkkdhi9sl3r4s00'),
    ('consumer_key', 'dpf43f3p2l4k3l03'),
    ('token', 'nnch734d00sl2jdk'),
)

SAMPLE_OAUTH_BASE_STRING = 'GET&http%3A%2F%2Fphotos.example.net%2Fphotos&file%3Dvacation.jpg%26oauth_consumer_key%3Ddpf43f3p2l4k3l03%26oauth_nonce%3Dkllo9940pd9333jh%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1191242096%26oauth_token%3Dnnch734d00sl2jdk%26oauth_version%3D1.0%26size%3Doriginal'

SAMPLE_OAUTH_AUTHORIZATION = ', '.join([
    'OAuth realm=""',
    'oauth_consumer_key="dpf43f3p2l4k3l03"',
    'oauth_nonce="kllo9940pd9333jh"',
    'oauth_signature="tR3%2BTy81lMeYAr%2FFid0kMTYa%2FWM%3D"',
    'oauth_signature_method="HMAC-SHA1"',
    'oauth_timestamp="1191242096"',
    'oauth_token="nnch734d00sl2jdk"',
    'oauth_version="1.0"',
])


# A sample view from Dmedia:
doc_type = """
function(doc) {
    emit(doc.type, null);
}
"""
doc_time = """
function(doc) {
    emit(doc.time, null);
}
"""
doc_design = {
    '_id': '_design/doc',
    'views': {
        'type': {'map': doc_type, 'reduce': '_count'},
        'time': {'map': doc_time},
    },
}


def test_id():
    """
    So we can tell our random test IDs from the ones microfiber.random_id()
    makes, we use 160-bit IDs instead of 120-bit.
    """
    return b32encode(os.urandom(20)).decode('ascii')


def is_microfiber_id(_id):
    assert isinstance(_id, str)
    return (
        len(_id) == microfiber.RANDOM_B32LEN
        and set(_id).issubset(B32ALPHABET)
    )

assert is_microfiber_id(microfiber.random_id())
assert not is_microfiber_id(test_id())


def random_dbname():
    return 'db-' + microfiber.random_id().lower()


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

    def test_json(self):
        """
        Test our assumptions about json.dumps().
        """
        tm = '™'
        self.assertEqual(json.dumps(tm), '"\\u2122"')
        self.assertEqual(json.dumps(tm, ensure_ascii=False), '"™"')

    def test_dumps(self):
        doc = {
            'hello': 'мир',
            'welcome': 'все',
        }
        self.assertEqual(
            microfiber.dumps(doc),
            '{"hello":"мир","welcome":"все"}'
        )
        self.assertEqual(
            microfiber.dumps(doc, pretty=True),
            '{\n    "hello": "мир",\n    "welcome": "все"\n}'
        )

    def test_json_body(self):
        doc = {
            '_id': 'foo',
            'bar': 'baz',
            'hello': 'world',
            'name': 'Jon Åslund',
        }
        json_str = json.dumps(doc,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',',':'),
        )
        json_str2 = json.dumps(json_str,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',',':'),
        )
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
        self.assertEqual(
            microfiber._json_body('*safe solvent™'),
            b'"*safe solvent\xe2\x84\xa2"'
        )   
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

        # Test key, startkey, endkey with non-ascii values
        options = dict(
            key='Hanna Sköld',
            startkey='Matias Särs',
            endkey='Paweł Moll',
        )
        self.assertEqual(
            list(microfiber._queryiter(options)),
            [
                ('endkey', '"Paweł Moll"'),
                ('key', '"Hanna Sköld"'),
                ('startkey', '"Matias Särs"'),
            ]
        )

    def test_oauth_base_string(self):
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
        self.assertEqual(
            microfiber._oauth_base_string(method, url, params),
            SAMPLE_OAUTH_BASE_STRING
        )

    def test_oauth_sign(self):
        tokens = dict(SAMPLE_OAUTH_TOKENS)
        self.assertEqual(
            microfiber._oauth_sign(tokens, SAMPLE_OAUTH_BASE_STRING),
            'tR3+Ty81lMeYAr/Fid0kMTYa/WM='
        )

    def test_oauth_header(self):
        tokens = dict(SAMPLE_OAUTH_TOKENS)
        method = 'GET'
        baseurl = 'http://photos.example.net/photos'
        query = {'file': 'vacation.jpg', 'size': 'original'}
        testing = ('1191242096', 'kllo9940pd9333jh')
        self.assertEqual(
            microfiber._oauth_header(tokens, method, baseurl, query, testing),
            {'Authorization': SAMPLE_OAUTH_AUTHORIZATION},
        )

    def test_basic_auth_header(self):
        f = microfiber._basic_auth_header
        basic = {'username': 'Aladdin', 'password': 'open sesame'}
        self.assertEqual(
            f(basic),
            {'Authorization': 'Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=='}
        )

    def test_build_ssl_context(self):
        certs = TempCerts()

        # FIXME: We need to add tests for config['ca_path'], but
        # `usercouch.sslhelpers` doesn't have the needed helpers yet.

        # Empty config, uses openssl default ca_path
        ctx = microfiber.build_ssl_context({})
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertEqual(ctx.protocol, ssl.PROTOCOL_TLSv1)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)

        # Provide ca_file
        config = {
            'ca_file': certs.user.ca_file,
        }
        ctx = microfiber.build_ssl_context(config)
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertEqual(ctx.protocol, ssl.PROTOCOL_TLSv1)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)

        # Provide cert_file and key_file (uses openssl default ca_path)
        config = {
            'cert_file': certs.machine.cert_file,
            'key_file': certs.machine.key_file,
        }
        ctx = microfiber.build_ssl_context(config)
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertEqual(ctx.protocol, ssl.PROTOCOL_TLSv1)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)

        # Provide all three
        config = {
            'ca_file': certs.user.ca_file,
            'cert_file': certs.machine.cert_file,
            'key_file': certs.machine.key_file,
        }
        ctx = microfiber.build_ssl_context(config)
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertEqual(ctx.protocol, ssl.PROTOCOL_TLSv1)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)

        # Provide junk ca_file, make sure ca_file is actually being used
        config = {
            'ca_file': certs.machine.key_file,
        }
        with self.assertRaises(ssl.SSLError) as cm:
            microfiber.build_ssl_context(config)

        # Leave out key_file, make sure cert_file is actually being used
        config = {
            'cert_file': certs.machine.cert_file,
        }
        with self.assertRaises(ssl.SSLError) as cm:
            microfiber.build_ssl_context(config)

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
        local_db = random_dbname()
        remote_db = random_dbname()
        remote_url = 'http://' + random_id().lower() + ':5984/'

        # Test with no auth
        remote_env = {'url': remote_url}
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                },
                'continuous': True,
                'cancel': True,
            }
        )

        # Test with OAuth
        tokens = random_oauth()
        remote_env = {'url': remote_url, 'oauth': deepcopy(tokens)}
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
                'cancel': True,
            }
        )

        # Test with basic HTTP auth
        basic = random_basic()
        headers = microfiber._basic_auth_header(basic)
        remote_env = {'url': remote_url, 'basic': basic}
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'continuous': True,
                'cancel': True,
            }
        )

        # Test that OAuth takes precedence over basic auth
        remote_env['oauth'] = deepcopy(tokens)
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.push_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': local_db,
                'target': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'continuous': True,
                'cancel': True,
            }
        )

    def test_pull_replication(self):
        local_db = 'db-' + random_id().lower()
        remote_db = 'db-' + random_id().lower()
        remote_url = 'http://' + random_id().lower() + ':5984/'

        # Test with no auth
        remote_env = {'url': remote_url}
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env),
            {
                'source': {
                    'url': remote_url + remote_db,
                },
                'target': local_db,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                },
                'target': local_db,
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                },
                'target': local_db,
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                },
                'target': local_db,
                'continuous': True,
                'cancel': True,
            }
        )

        # Test with OAuth
        tokens = random_oauth()
        remote_env = {'url': remote_url, 'oauth': deepcopy(tokens)}
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
                'continuous': True,
                'cancel': True,
            }
        )

        # Test with basic HTTP auth
        basic = random_basic()
        headers = microfiber._basic_auth_header(basic)
        remote_env = {'url': remote_url, 'basic': basic}
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'target': local_db,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'target': local_db,
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'target': local_db,
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'headers': headers,
                },
                'target': local_db,
                'continuous': True,
                'cancel': True,
            }
        )

        # Test that OAuth takes precedence over basic auth
        remote_env['oauth'] = deepcopy(tokens)
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
                'cancel': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
                'continuous': True,
            }
        )
        self.assertEqual(
            microfiber.pull_replication(local_db, remote_db, remote_env,
                    continuous=True, cancel=True
            ),
            {
                'source': {
                    'url': remote_url + remote_db,
                    'auth': {'oauth': tokens},
                },
                'target': local_db,
                'continuous': True,
                'cancel': True,
            }
        )

    def test_id_slice_iter(self):
        ids = [random_id() for i in range(74)]
        rows = [{'id': _id} for _id in ids]
        chunks = list(microfiber.id_slice_iter(rows))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 25)
        self.assertEqual(len(chunks[1]), 25)
        self.assertEqual(len(chunks[2]), 24)
        accum = []
        for chunk in chunks:
            accum.extend(chunk)
        self.assertEqual(accum, ids)

        ids = [random_id() for i in range(75)]
        rows = [{'id': _id} for _id in ids]
        chunks = list(microfiber.id_slice_iter(rows))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 25)
        self.assertEqual(len(chunks[1]), 25)
        self.assertEqual(len(chunks[2]), 25)
        accum = []
        for chunk in chunks:
            accum.extend(chunk)
        self.assertEqual(accum, ids)

        ids = [random_id() for i in range(76)]
        rows = [{'id': _id} for _id in ids]
        chunks = list(microfiber.id_slice_iter(rows))
        self.assertEqual(len(chunks), 4)
        self.assertEqual(len(chunks[0]), 25)
        self.assertEqual(len(chunks[1]), 25)
        self.assertEqual(len(chunks[2]), 25)
        self.assertEqual(len(chunks[3]), 1)
        accum = []
        for chunk in chunks:
            accum.extend(chunk)
        self.assertEqual(accum, ids)


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


class TestContext(TestCase):
    def test_init(self):
        # Test with bad env type:
        bad = [microfiber.SERVER]
        with self.assertRaises(TypeError) as cm:
            microfiber.Context(bad)
        self.assertEqual(
            str(cm.exception),
            'env must be a `dict` or `str`; got {!r}'.format(bad)
        )

        # Test with bad URL scheme:
        bad = 'sftp://localhost:5984/'
        with self.assertRaises(ValueError) as cm:
            microfiber.Context(bad)
        self.assertEqual(
            str(cm.exception),
            'url scheme must be http or https; got {!r}'.format(bad)
        )

        # Test with bad URL:
        bad = 'http:localhost:5984/foo/bar'
        with self.assertRaises(ValueError) as cm:
            microfiber.Context(bad)
        self.assertEqual(
            str(cm.exception),
            'bad url: {!r}'.format(bad)
        )

        # Test with default env:
        ctx = microfiber.Context()
        self.assertEqual(ctx.env, {'url': microfiber.DEFAULT_URL})
        self.assertEqual(ctx.basepath, '/')
        self.assertEqual(ctx.t, urlparse(microfiber.DEFAULT_URL))
        self.assertEqual(ctx.url, microfiber.DEFAULT_URL)
        self.assertIsInstance(ctx.threadlocal, threading.local)
        self.assertFalse(hasattr(ctx, 'ssl_ctx'))
        self.assertFalse(hasattr(ctx, 'check_hostname'))

        # Test with an empty env dict:
        ctx = microfiber.Context({})
        self.assertEqual(ctx.env, {})
        self.assertEqual(ctx.basepath, '/')
        self.assertEqual(ctx.t, urlparse(microfiber.DEFAULT_URL))
        self.assertEqual(ctx.url, microfiber.DEFAULT_URL)
        self.assertIsInstance(ctx.threadlocal, threading.local)
        self.assertFalse(hasattr(ctx, 'ssl_ctx'))
        self.assertFalse(hasattr(ctx, 'check_hostname'))

        # Test with HTTP IPv4 URLs:
        url = 'http://localhost:5984/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://localhost:5984/'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('http', 'localhost:5984', '/', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://localhost:5984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))
        url = 'http://localhost:5984'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://localhost:5984'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('http', 'localhost:5984', '', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://localhost:5984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))
        url = 'http://localhost:5984/foo/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://localhost:5984/foo/'})
            self.assertEqual(ctx.basepath, '/foo/')
            self.assertEqual(ctx.t,
                ('http', 'localhost:5984', '/foo/', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://localhost:5984/foo/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))
        url = 'http://localhost:5984/foo'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://localhost:5984/foo'})
            self.assertEqual(ctx.basepath, '/foo/')
            self.assertEqual(ctx.t,
                ('http', 'localhost:5984', '/foo', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://localhost:5984/foo/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))

        # Test with HTTP IPv6 URLs:
        url = 'http://[::1]:5984/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://[::1]:5984/'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('http', '[::1]:5984', '/', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://[::1]:5984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))
        url = 'http://[::1]:5984'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://[::1]:5984'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('http', '[::1]:5984', '', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://[::1]:5984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))
        url = 'http://[::1]:5984/foo/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://[::1]:5984/foo/'})
            self.assertEqual(ctx.basepath, '/foo/')
            self.assertEqual(ctx.t,
                ('http', '[::1]:5984', '/foo/', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://[::1]:5984/foo/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))
        url = 'http://[::1]:5984/foo'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'http://[::1]:5984/foo'})
            self.assertEqual(ctx.basepath, '/foo/')
            self.assertEqual(ctx.t,
                ('http', '[::1]:5984', '/foo', '', '', '')
            )
            self.assertEqual(ctx.url, 'http://[::1]:5984/foo/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertFalse(hasattr(ctx, 'ssl_ctx'))
            self.assertFalse(hasattr(ctx, 'check_hostname'))

        # Test with HTTPS IPv4 URLs:
        url = 'https://localhost:6984/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://localhost:6984/'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('https', 'localhost:6984', '/', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://localhost:6984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)
        url = 'https://localhost:6984'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://localhost:6984'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('https', 'localhost:6984', '', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://localhost:6984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)
        url = 'https://localhost:6984/bar/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://localhost:6984/bar/'})
            self.assertEqual(ctx.basepath, '/bar/')
            self.assertEqual(ctx.t,
                ('https', 'localhost:6984', '/bar/', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://localhost:6984/bar/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)
        url = 'https://localhost:6984/bar'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://localhost:6984/bar'})
            self.assertEqual(ctx.basepath, '/bar/')
            self.assertEqual(ctx.t,
                ('https', 'localhost:6984', '/bar', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://localhost:6984/bar/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)

        # Test with HTTPS IPv6 URLs:
        url = 'https://[::1]:6984/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://[::1]:6984/'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('https', '[::1]:6984', '/', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://[::1]:6984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)
        url = 'https://[::1]:6984'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://[::1]:6984'})
            self.assertEqual(ctx.basepath, '/')
            self.assertEqual(ctx.t,
                ('https', '[::1]:6984', '', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://[::1]:6984/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)
        url = 'https://[::1]:6984/bar/'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://[::1]:6984/bar/'})
            self.assertEqual(ctx.basepath, '/bar/')
            self.assertEqual(ctx.t,
                ('https', '[::1]:6984', '/bar/', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://[::1]:6984/bar/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)
        url = 'https://[::1]:6984/bar'
        for env in (url, {'url': url}):
            ctx = microfiber.Context(env)
            self.assertEqual(ctx.env, {'url': 'https://[::1]:6984/bar'})
            self.assertEqual(ctx.basepath, '/bar/')
            self.assertEqual(ctx.t,
                ('https', '[::1]:6984', '/bar', '', '', '')
            )
            self.assertEqual(ctx.url, 'https://[::1]:6984/bar/')
            self.assertIsInstance(ctx.threadlocal, threading.local)
            self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
            self.assertIsNone(ctx.check_hostname)

        # Test with check_hostname=False
        env = {
            'url': 'https://127.0.0.1:6984/',
            'ssl': {'check_hostname': False},
        }
        ctx = microfiber.Context(env)
        self.assertEqual(ctx.env,
            {
                'url': 'https://127.0.0.1:6984/',
                'ssl': {'check_hostname': False},
            }
        )
        self.assertEqual(ctx.basepath, '/')
        self.assertEqual(ctx.t,
            ('https', '127.0.0.1:6984', '/', '', '', '')
        )
        self.assertEqual(ctx.url, 'https://127.0.0.1:6984/')
        self.assertIsInstance(ctx.threadlocal, threading.local)
        self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
        self.assertIs(ctx.check_hostname, False)
        env = {
            'url': 'https://[::1]:6984/',
            'ssl': {'check_hostname': False},
        }
        ctx = microfiber.Context(env)
        self.assertEqual(ctx.env,
            {
                'url': 'https://[::1]:6984/',
                'ssl': {'check_hostname': False},
            }
        )
        self.assertEqual(ctx.basepath, '/')
        self.assertEqual(ctx.t,
            ('https', '[::1]:6984', '/', '', '', '')
        )
        self.assertEqual(ctx.url, 'https://[::1]:6984/')
        self.assertIsInstance(ctx.threadlocal, threading.local)
        self.assertIsInstance(ctx.ssl_ctx, ssl.SSLContext)
        self.assertIs(ctx.check_hostname, False)

    def test_full_url(self):
        ctx = microfiber.Context('https://localhost:5003/')
        self.assertEqual(
            ctx.full_url('/'),
            'https://localhost:5003/'
        )
        self.assertEqual(
            ctx.full_url('/db/doc/att?bar=null&foo=true'),
            'https://localhost:5003/db/doc/att?bar=null&foo=true'
        )

        ctx = microfiber.Context('https://localhost:5003/mydb/')
        self.assertEqual(
            ctx.full_url('/'),
            'https://localhost:5003/'
        )
        self.assertEqual(
            ctx.full_url('/db/doc/att?bar=null&foo=true'),
            'https://localhost:5003/db/doc/att?bar=null&foo=true'
        )

        for url in microfiber.URL_CONSTANTS:
            ctx = microfiber.Context(url)
            self.assertEqual(ctx.full_url('/'), url)

    def test_get_connection(self):
        ctx = microfiber.Context(microfiber.HTTP_IPv4_URL)
        conn = ctx.get_connection()
        self.assertIsInstance(conn, HTTPConnection)
        self.assertNotIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '127.0.0.1')
        self.assertEqual(conn.port, 5984)

        ctx = microfiber.Context(microfiber.HTTP_IPv6_URL)
        conn = ctx.get_connection()
        self.assertIsInstance(conn, HTTPConnection)
        self.assertNotIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '::1')
        self.assertEqual(conn.port, 5984)

        ctx = microfiber.Context(microfiber.HTTPS_IPv4_URL)
        conn = ctx.get_connection()
        self.assertIsInstance(conn, HTTPConnection)
        self.assertIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '127.0.0.1')
        self.assertEqual(conn.port, 6984)
        self.assertIs(conn._context, ctx.ssl_ctx)
        self.assertIs(conn._check_hostname, True)

        ctx = microfiber.Context(microfiber.HTTPS_IPv6_URL)
        conn = ctx.get_connection()
        self.assertIsInstance(conn, HTTPConnection)
        self.assertIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '::1')
        self.assertEqual(conn.port, 6984)
        self.assertIs(conn._context, ctx.ssl_ctx)
        self.assertIs(conn._check_hostname, True)

        env = {
            'url': microfiber.HTTPS_IPv4_URL,
            'ssl': {'check_hostname': False},
        }
        ctx = microfiber.Context(env)
        conn = ctx.get_connection()
        self.assertIsInstance(conn, HTTPConnection)
        self.assertIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '127.0.0.1')
        self.assertEqual(conn.port, 6984)
        self.assertIs(conn._context, ctx.ssl_ctx)
        self.assertIs(conn._check_hostname, False)

        env = {
            'url': microfiber.HTTPS_IPv6_URL,
            'ssl': {'check_hostname': False},
        }
        ctx = microfiber.Context(env)
        conn = ctx.get_connection()
        self.assertIsInstance(conn, HTTPConnection)
        self.assertIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '::1')
        self.assertEqual(conn.port, 6984)
        self.assertIs(conn._context, ctx.ssl_ctx)
        self.assertIs(conn._check_hostname, False)

    def test_get_threadlocal_connection(self):
        id1 = test_id()
        id2 = test_id()

        class ContextSubclass(microfiber.Context):
            def __init__(self):
                self.threadlocal = threading.local()
                self._calls = 0

            def get_connection(self):
                self._calls += 1
                return id1

        # Test when connection does *not* exist in current thread
        ctx = ContextSubclass()
        self.assertEqual(ctx.get_threadlocal_connection(), id1)
        self.assertEqual(ctx.threadlocal.connection, id1)
        self.assertEqual(ctx._calls, 1)
        self.assertEqual(ctx.get_threadlocal_connection(), id1)
        self.assertEqual(ctx.threadlocal.connection, id1)
        self.assertEqual(ctx._calls, 1)
        del ctx.threadlocal.connection
        self.assertEqual(ctx.get_threadlocal_connection(), id1)
        self.assertEqual(ctx.threadlocal.connection, id1)
        self.assertEqual(ctx._calls, 2)
        self.assertEqual(ctx.get_threadlocal_connection(), id1)
        self.assertEqual(ctx.threadlocal.connection, id1)
        self.assertEqual(ctx._calls, 2)

        # Test when connection does exist in current thread
        ctx = ContextSubclass()
        ctx.threadlocal.connection = id2
        self.assertEqual(ctx.get_threadlocal_connection(), id2)
        self.assertEqual(ctx.threadlocal.connection, id2)
        self.assertEqual(ctx._calls, 0)
        self.assertEqual(ctx.get_threadlocal_connection(), id2)
        self.assertEqual(ctx.threadlocal.connection, id2)
        self.assertEqual(ctx._calls, 0)
        del ctx.threadlocal.connection
        self.assertEqual(ctx.get_threadlocal_connection(), id1)
        self.assertEqual(ctx.threadlocal.connection, id1)
        self.assertEqual(ctx._calls, 1)
        self.assertEqual(ctx.get_threadlocal_connection(), id1)
        self.assertEqual(ctx.threadlocal.connection, id1)
        self.assertEqual(ctx._calls, 1)

        # Sanity check with the original class:
        ctx = microfiber.Context(microfiber.HTTPS_IPv6_URL)
        conn = ctx.get_threadlocal_connection()
        self.assertIs(conn, ctx.threadlocal.connection)
        self.assertIsInstance(conn, HTTPConnection)
        self.assertIsInstance(conn, HTTPSConnection)
        self.assertEqual(conn.host, '::1')
        self.assertEqual(conn.port, 6984)
        self.assertIs(conn._context, ctx.ssl_ctx)
        self.assertIs(conn._check_hostname, True)
        self.assertIs(ctx.get_threadlocal_connection(), conn)
        self.assertIs(conn, ctx.threadlocal.connection)

    def test_get_auth_headers(self):
        method = 'GET'
        path = '/photos'
        query = (('file', 'vacation.jpg'), ('size', 'original'))
        testing = ('1191242096', 'kllo9940pd9333jh')

        # Test with no-auth (open):
        env = {
            'url': 'http://photos.example.net/',
        }
        ctx = microfiber.Context(env)
        self.assertEqual(
            ctx.get_auth_headers(method, path, query, testing),
            {}
        )

        # Test with basic auth:
        env = {
            'url': 'http://photos.example.net/',
            'basic': {'username': 'Aladdin', 'password': 'open sesame'},
        }
        ctx = microfiber.Context(env)
        self.assertEqual(
            ctx.get_auth_headers(method, path, query, testing),
            {'Authorization': 'Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=='}
        )

        # Test with oauth:
        env = {
            'url': 'http://photos.example.net/',
            'oauth': dict(SAMPLE_OAUTH_TOKENS),
        }
        ctx = microfiber.Context(env)
        self.assertEqual(
            ctx.get_auth_headers(method, path, query, testing),
            {'Authorization': SAMPLE_OAUTH_AUTHORIZATION},
        )

        # Make sure oauth overrides basic
        env = {
            'url': 'http://photos.example.net/',
            'oauth': dict(SAMPLE_OAUTH_TOKENS),
            'basic': {'username': 'Aladdin', 'password': 'open sesame'},
        }
        ctx = microfiber.Context(env)
        self.assertEqual(
            ctx.get_auth_headers(method, path, query, testing),
            {'Authorization': SAMPLE_OAUTH_AUTHORIZATION},
        )


class TestCouchBase(TestCase):
    def test_init(self):
        # Supply neither *env* nor *ctx*:
        inst = microfiber.CouchBase()
        self.assertIsInstance(inst.ctx, microfiber.Context)
        self.assertEqual(inst.env, {'url': microfiber.HTTP_IPv4_URL})
        self.assertIs(inst.env, inst.ctx.env)
        self.assertEqual(inst.basepath, '/')
        self.assertIs(inst.basepath, inst.ctx.basepath)
        self.assertEqual(inst.url, microfiber.HTTP_IPv4_URL)
        self.assertIs(inst.url, inst.ctx.url)

        # Supply *env*:
        env = {'url': microfiber.HTTPS_IPv6_URL}
        inst = microfiber.CouchBase(env=env)
        self.assertIsInstance(inst.ctx, microfiber.Context)
        self.assertEqual(inst.env, {'url': microfiber.HTTPS_IPv6_URL})
        self.assertIs(inst.env, inst.ctx.env)
        self.assertIs(inst.env, env)
        self.assertEqual(inst.basepath, '/')
        self.assertIs(inst.basepath, inst.ctx.basepath)
        self.assertEqual(inst.url, microfiber.HTTPS_IPv6_URL)
        self.assertIs(inst.url, inst.ctx.url)

        # Supply *ctx*:
        url = 'http://example.com/foo/'
        ctx = microfiber.Context(url)
        inst = microfiber.CouchBase(ctx=ctx)
        self.assertIsInstance(inst.ctx, microfiber.Context)
        self.assertIs(inst.ctx, ctx)
        self.assertEqual(inst.env, {'url': url})
        self.assertIs(inst.env, inst.ctx.env)
        self.assertEqual(inst.basepath, '/foo/')
        self.assertIs(inst.basepath, inst.ctx.basepath)
        self.assertEqual(inst.url, url)
        self.assertIs(inst.url, inst.ctx.url)

    def test_full_url(self):
        inst = microfiber.CouchBase('https://localhost:5003/')
        self.assertEqual(
            inst._full_url('/'),
            'https://localhost:5003/'
        )
        self.assertEqual(
            inst._full_url('/db/doc/att?bar=null&foo=true'),
            'https://localhost:5003/db/doc/att?bar=null&foo=true'
        )

        inst = microfiber.CouchBase('http://localhost:5003/mydb/')
        self.assertEqual(
            inst._full_url('/'),
            'http://localhost:5003/'
        )
        self.assertEqual(
            inst._full_url('/db/doc/att?bar=null&foo=true'),
            'http://localhost:5003/db/doc/att?bar=null&foo=true'
        )


class TestServer(TestCase):
    def test_init(self):
        # Supply neither *env* nor *ctx*:
        inst = microfiber.Server()
        self.assertIsInstance(inst.ctx, microfiber.Context)
        self.assertEqual(inst.env, {'url': microfiber.HTTP_IPv4_URL})
        self.assertIs(inst.env, inst.ctx.env)
        self.assertEqual(inst.basepath, '/')
        self.assertIs(inst.basepath, inst.ctx.basepath)
        self.assertEqual(inst.url, microfiber.HTTP_IPv4_URL)
        self.assertIs(inst.url, inst.ctx.url)

        # Supply *env*:
        env = {'url': microfiber.HTTPS_IPv6_URL}
        inst = microfiber.Server(env=env)
        self.assertIsInstance(inst.ctx, microfiber.Context)
        self.assertEqual(inst.env, {'url': microfiber.HTTPS_IPv6_URL})
        self.assertIs(inst.env, inst.ctx.env)
        self.assertIs(inst.env, env)
        self.assertEqual(inst.basepath, '/')
        self.assertIs(inst.basepath, inst.ctx.basepath)
        self.assertEqual(inst.url, microfiber.HTTPS_IPv6_URL)
        self.assertIs(inst.url, inst.ctx.url)

        # Supply *ctx*:
        url = 'http://example.com/foo/'
        ctx = microfiber.Context(url)
        inst = microfiber.Server(ctx=ctx)
        self.assertIsInstance(inst.ctx, microfiber.Context)
        self.assertIs(inst.ctx, ctx)
        self.assertEqual(inst.env, {'url': url})
        self.assertIs(inst.env, inst.ctx.env)
        self.assertEqual(inst.basepath, '/foo/')
        self.assertIs(inst.basepath, inst.ctx.basepath)
        self.assertEqual(inst.url, url)
        self.assertIs(inst.url, inst.ctx.url)

    def test_repr(self):
        # Use a subclass to make sure only Server.url factors into __repr__():
        class ServerSubclass(microfiber.Server):
            def __init__(self):
                pass

        inst = ServerSubclass()
        inst.url = microfiber.HTTP_IPv4_URL
        self.assertEqual(repr(inst),
            "ServerSubclass('http://127.0.0.1:5984/')"
        )
        inst.url = microfiber.HTTPS_IPv4_URL
        self.assertEqual(repr(inst),
            "ServerSubclass('https://127.0.0.1:6984/')"
        )
        inst.url = microfiber.HTTP_IPv6_URL
        self.assertEqual(repr(inst),
            "ServerSubclass('http://[::1]:5984/')"
        )
        inst.url = microfiber.HTTPS_IPv6_URL
        self.assertEqual(repr(inst),
            "ServerSubclass('https://[::1]:6984/')"
        )

        # Sanity check with original class
        inst = microfiber.Server()
        self.assertEqual(repr(inst),
            "Server('http://127.0.0.1:5984/')"
        )
        inst = microfiber.Server(microfiber.HTTPS_IPv6_URL)
        self.assertEqual(repr(inst),
            "Server('https://[::1]:6984/')"
        )

    def test_database(self):
        server = microfiber.Server()
        db = server.database('mydb')
        self.assertIsInstance(db, microfiber.Database)
        self.assertIs(db.ctx, server.ctx)
        self.assertEqual(db.name, 'mydb')
        self.assertEqual(db.basepath, '/mydb/')

        server = microfiber.Server('http://example.com/foo/')
        db = server.database('mydb')
        self.assertIsInstance(db, microfiber.Database)
        self.assertIs(db.ctx, server.ctx)
        self.assertEqual(db.name, 'mydb')
        self.assertEqual(db.basepath, '/foo/mydb/')
        self.assertEqual(db.url, 'http://example.com/foo/')


class TestDatabase(TestCase):
    klass = microfiber.Database

    def test_init(self):
        inst = self.klass('foo')
        self.assertEqual(inst.name, 'foo')
        self.assertEqual(inst.url, 'http://127.0.0.1:5984/')
        self.assertEqual(inst.basepath, '/foo/')

        inst = self.klass('baz', 'https://example.com/bar')
        self.assertEqual(inst.name, 'baz')
        self.assertEqual(inst.url, 'https://example.com/bar/')
        self.assertEqual(inst.basepath, '/bar/baz/')

    def test_repr(self):
        inst = self.klass('dmedia')
        self.assertEqual(
            repr(inst),
            "Database('dmedia', 'http://127.0.0.1:5984/')"
        )

        inst = self.klass('novacut', 'https://localhost:5004/')
        self.assertEqual(
            repr(inst),
            "Database('novacut', 'https://localhost:5004/')"
        )

    def test_server(self):
        db = microfiber.Database('mydb')
        server = db.server()
        self.assertIsInstance(server, microfiber.Server)
        self.assertIs(server.ctx, db.ctx)
        self.assertEqual(server.basepath, '/')

        db = microfiber.Database('mydb', {'url': 'https://example.com/stuff'})
        server = db.server()
        self.assertIsInstance(server, microfiber.Server)
        self.assertIs(server.ctx, db.ctx)
        self.assertEqual(server.url, 'https://example.com/stuff/')
        self.assertEqual(server.basepath, '/stuff/')

    def test_view(self):
        class Mock(microfiber.Database):
            def get(self, *parts, **options):
                self._parts = parts
                self._options = options
                assert not hasattr(self, '_return')
                self._return = random_id()
                return self._return

        db = Mock('mydb')
        self.assertEqual(db.view('foo', 'bar'), db._return)
        self.assertEqual(db._parts, ('_design', 'foo', '_view', 'bar'))
        self.assertEqual(db._options, {'reduce': False})

        db = Mock('mydb')
        self.assertEqual(db.view('foo', 'bar', reduce=True), db._return)
        self.assertEqual(db._parts, ('_design', 'foo', '_view', 'bar'))
        self.assertEqual(db._options, {'reduce': True})

        db = Mock('mydb')
        self.assertEqual(db.view('foo', 'bar', include_docs=True), db._return)
        self.assertEqual(db._parts, ('_design', 'foo', '_view', 'bar'))
        self.assertEqual(db._options, {'reduce': False, 'include_docs': True})

        db = Mock('mydb')
        self.assertEqual(
            db.view('foo', 'bar', include_docs=True, reduce=True),
            db._return
        )
        self.assertEqual(db._parts, ('_design', 'foo', '_view', 'bar'))
        self.assertEqual(db._options, {'reduce': True, 'include_docs': True})


class LiveTestCase(TestCase):
    """
    Base class for tests that can be skipped via the --no-live option.

    When working on code whose tests don't need a live CouchDB instance, its
    annoying to wait for the slow live tests to run.  You can skip the live
    tests like this::

        ./setup.py test --no-live

    Sub-classes should call ``super().setUp()`` first thing in their
    ``setUp()`` methods.
    """

    def setUp(self):
        if os.environ.get('MICROFIBER_TEST_NO_LIVE') == 'true':
            self.skipTest('run with --no-live')


class CouchTestCase(LiveTestCase):
    db = 'test_microfiber'

    def setUp(self):
        super().setUp()
        self.auth = os.environ.get('MICROFIBER_TEST_AUTH', 'basic')
        self.tmpcouch = TempCouch()
        self.env = self.tmpcouch.bootstrap(self.auth)

    def tearDown(self):
        self.tmpcouch = None
        self.env = None


class ReplicationTestCase(LiveTestCase):
    def setUp(self):
        super().setUp()
        self.tmp1 = TempCouch()
        self.env1 = self.tmp1.bootstrap()
        self.tmp2 = TempCouch()
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
        name1 = random_dbname()
        name2 = random_dbname()

        # Create databases
        self.assertEqual(s1.put(None, name1), {'ok': True})
        self.assertEqual(s2.put(None, name2), {'ok': True})

        # Start continuous s1.name1 -> s2.name2 push replication
        result = s1.push(name1, name2, self.env2, continuous=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s1.name1, make sure they show up in s2.name2
        docs1 = [{'_id': test_id()} for i in range(100)]
        for doc in docs1:
            doc['_rev'] = s1.post(doc, name1)['rev']
        time.sleep(1)
        for doc in docs1:
            self.assertEqual(s2.get(name2, doc['_id']), doc)

        # Start continuous s2.name2 -> s1.name1 push replication
        result = s2.push(name2, name1, self.env1, continuous=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s2.name2, make sure they show up in s1.name1
        docs2 = [{'_id': test_id(), 'two': True} for i in range(100)]
        for doc in docs2:
            doc['_rev'] = s2.post(doc, name2)['rev']
        time.sleep(1)
        for doc in docs2:
            self.assertEqual(s1.get(name1, doc['_id']), doc)

    def test_pull(self):
        s1 = microfiber.Server(self.env1)
        s2 = microfiber.Server(self.env2)
        name1 = random_dbname()
        name2 = random_dbname()

        # Create databases
        self.assertEqual(s1.put(None, name1), {'ok': True})
        self.assertEqual(s2.put(None, name2), {'ok': True})

        # Start continuous s1.name1 <- s2.name2 pull replication
        result = s1.pull(name1, name2, self.env2, continuous=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s2.name2, make sure they show up in s1.name1
        docs1 = [{'_id': test_id()} for i in range(100)]
        for doc in docs1:
            doc['_rev'] = s2.post(doc, name2)['rev']
        time.sleep(1)
        for doc in docs1:
            self.assertEqual(s1.get(name1, doc['_id']), doc)

        # Start continuous s2.name2 <- s1.name1 pull replication
        result = s2.pull(name2, name1, self.env1, continuous=True)
        self.assertEqual(set(result), set(['_local_id', 'ok']))
        self.assertIs(result['ok'], True)

        # Save docs in s1.name1, make sure they show up in s2.name2
        docs2 = [{'_id': test_id(), 'two': True} for i in range(100)]
        for doc in docs2:
            doc['_rev'] = s1.post(doc, name1)['rev']
        time.sleep(1)
        for doc in docs2:
            self.assertEqual(s2.get(name2, doc['_id']), doc)


class TestFakeList(CouchTestCase):
    def test_init(self):
        db = microfiber.Database('foo', self.env)
        self.assertTrue(db.ensure())

        # Test when DB is empty
        rows = []
        fake = microfiber.FakeList(rows, db)
        self.assertIsInstance(fake, list)
        self.assertIs(fake._rows, rows)
        self.assertIs(fake._db, db)
        self.assertEqual(len(fake), 0)
        self.assertEqual(list(fake), [])

        # Test when there are some docs
        ids = sorted(test_id() for i in range(201))
        orig = [
            {'_id': _id, 'hello': 'мир', 'welcome': 'все'}
            for _id in ids
        ]
        docs = deepcopy(orig)
        db.save_many(docs)
        rows = db.get('_all_docs')['rows']
        fake = microfiber.FakeList(rows, db)
        self.assertIsInstance(fake, list)
        self.assertIs(fake._rows, rows)
        self.assertIs(fake._db, db)
        self.assertEqual(len(fake), 201)
        self.assertEqual(list(fake), orig)

        # Verify that _attachments get deleted
        for doc in docs:
            db.put_att('application/octet-stream', b'foobar', doc['_id'], 'baz',
                rev=doc['_rev']
            )
        for _id in ids:
            self.assertIn('_attachments', db.get(_id))
        rows = db.get('_all_docs')['rows']
        fake = microfiber.FakeList(rows, db)
        self.assertIsInstance(fake, list)
        self.assertIs(fake._rows, rows)
        self.assertIs(fake._db, db)
        self.assertEqual(len(fake), 201)
        self.assertEqual(list(fake), orig)


class TestCouchBaseLive(CouchTestCase):
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


class TestPermutations(LiveTestCase):
    """
    Test `CouchBase._request()` over all key *env* permutations.
    """

    # FIXME: For some reason OAuth isn't working with IPv6, perhap
    # server and client aren't using same canonical URL when signing?

    bind_addresses = ('127.0.0.1', '::1')
    auths = ('open', 'basic', 'oauth')

    def test_http(self):
        for bind_address in self.bind_addresses:
            for auth in self.auths:
                if auth == 'oauth' and bind_address == '::1':
                    continue
                tmpcouch = TempCouch()
                env = tmpcouch.bootstrap(auth, {'bind_address': bind_address})
                uc = microfiber.CouchBase(env)
                self.assertEqual(uc.get()['couchdb'], 'Welcome')

    def test_https(self):
        certs = TempCerts()
        for bind_address in self.bind_addresses:
            for auth in self.auths:
                if auth == 'oauth' and bind_address == '::1':
                    continue
                config = {
                    'bind_address': bind_address,
                    'ssl': {
                        'cert_file': certs.machine.cert_file,
                        'key_file': certs.machine.key_file,
                        'ca_file': certs.user.ca_file,
                    }
                }
                tmpcouch = TempCouch()
                env = tmpcouch.bootstrap(auth, config)
                env2 = env['env2']
                env2['ssl'] = {
                    'ca_file': certs.user.ca_file,
                    'check_hostname': False,
                }
                uc = microfiber.CouchBase(env2)
                self.assertEqual(uc.get()['couchdb'], 'Welcome')

                # Make sure things fail without ca_file
                bad = deepcopy(env2)
                bad['ssl'] = {'check_hostname': False}
                uc = microfiber.CouchBase(bad)
                with self.assertRaises(ssl.SSLError) as cm:
                    uc.get()

                # Make sure things fail without {'check_hostname': False}
                bad = deepcopy(env2)
                bad['ssl'] = {'ca_file': certs.user.ca_file}
                uc = microfiber.CouchBase(bad)
                with self.assertRaises(ssl.CertificateError) as cm:
                    uc.get()


class TestDatabaseLive(CouchTestCase):
    klass = microfiber.Database

    def test_ensure(self):
        inst = self.klass(self.db, self.env)
        self.assertRaises(NotFound, inst.get)
        self.assertTrue(inst.ensure())
        self.assertEqual(inst.get()['db_name'], self.db)
        self.assertFalse(inst.ensure())
        self.assertEqual(inst.delete(), {'ok': True})
        self.assertRaises(NotFound, inst.get)

    def test_non_ascii(self):
        inst = self.klass(self.db, self.env)
        self.assertTrue(inst.ensure())
        _id = test_id()
        name = '*safe solvent™'
        doc = {'_id': _id, 'name': name}
        inst.save(doc)
        self.assertEqual(inst.get(_id)['name'], name)

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
            [{'id': 'example', 'rev': '2-34e30c39538299cfed3958f6692f794d', 'ok': True}]
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
            [{'id': 'example2', 'rev': '2-34e30c39538299cfed3958f6692f794d', 'ok': True}]
        )
        self.assertEqual(db.get('example2'),
            {
                '_id': 'example2',
                '_rev': '3-074e07f92324e448702162e585e718fb',
                'x': 'foo',
            }
        )

    def test_save_many(self):
        db = microfiber.Database(self.db, self.env)
        self.assertTrue(db.ensure())

        # Test that doc['_id'] gets set automatically
        markers = tuple(test_id() for i in range(10))
        docs = [{'marker': m} for m in markers]
        rows = db.save_many(docs)
        for (marker, doc, row) in zip(markers, docs, rows):
            self.assertEqual(doc['marker'], marker)
            self.assertEqual(doc['_id'], row['id'])
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertTrue(is_microfiber_id(doc['_id']))

        # Test when doc['_id'] is already present
        ids = tuple(test_id() for i in range(10))
        docs = [{'_id': _id} for _id in ids]
        rows = db.save_many(docs)
        for (_id, doc, row) in zip(ids, docs, rows):
            self.assertEqual(doc['_id'], _id)
            self.assertEqual(row['id'], _id)
            self.assertEqual(doc['_rev'], row['rev'])
            self.assertTrue(doc['_rev'].startswith('1-'))
            self.assertEqual(db.get(_id), doc)

        # Let's update all the docs
        for doc in docs:
            doc['x'] = 'foo'    
        rows = db.save_many(docs)
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
            rows = db.save_many(docs)
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
                db.save(d)

        # Now let's update all the docs, test all-or-nothing behavior
        for doc in docs:
            doc['x'] = 'bar'    
        rows = db.bulksave(docs)
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
        rows = db.bulksave(docs)
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
        db.save_many(docs)

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

        # Test with unknown ids
        nope = random_id()
        self.assertEqual(
            db.get_many([nope]), 
            [None]
        )
        self.assertEqual(
            db.get_many([ids[17], nope, ids[18]]),
            [docs[17], None, docs[18]]
        )

    def test_dump(self):
        db = microfiber.Database('foo', self.env)
        self.assertTrue(db.ensure())
        docs = [
            {'_id': test_id(), 'hello': 'мир', 'welcome': 'все'}
            for i in range(200)
        ]
        docs_s = microfiber.dumps(
            sorted(docs, key=lambda d: d['_id']),
            pretty=True
        )
        docs.append(deepcopy(doc_design))
        checksum = md5(docs_s.encode('utf-8')).hexdigest()
        db.save_many(docs)

        # Test with .json
        dst = path.join(self.tmpcouch.paths.dump, 'foo.json')
        db.dump(dst)
        self.assertEqual(open(dst, 'r').read(), docs_s)
        self.assertEqual(
            md5(open(dst, 'rb').read()).hexdigest(),
            checksum
        )

        # Test with .json.gz
        dst = path.join(self.tmpcouch.paths.dump, 'foo.json.gz')
        db.dump(dst)
        gz_checksum = md5(open(dst, 'rb').read()).hexdigest()
        self.assertEqual(
            md5(gzip.GzipFile(dst, 'rb').read()).hexdigest(),
            checksum
        )

        # Test that timestamp doesn't change gz_checksum
        time.sleep(2)
        db.dump(dst)
        self.assertEqual(
            md5(open(dst, 'rb').read()).hexdigest(),
            gz_checksum
        )

        # Test that filename doesn't change gz_checksum
        dst = path.join(self.tmpcouch.paths.dump, 'bar.json.gz')
        db.dump(dst)
        self.assertEqual(
            md5(open(dst, 'rb').read()).hexdigest(),
            gz_checksum
        )

        # Make sure .JSON.GZ also works, that case is ignored
        dst = path.join(self.tmpcouch.paths.dump, 'FOO.JSON.GZ')
        db.dump(dst)
        self.assertEqual(
            md5(open(dst, 'rb').read()).hexdigest(),
            gz_checksum
        )
