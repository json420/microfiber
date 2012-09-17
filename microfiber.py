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
`microfiber` - fabric for a lightweight Couch.

Microfiber is an generic adapter for making HTTP requests to an arbitrary JSON
loving REST API like CouchDB.  Rather than wrapping the API in a bunch of
one-off methods, Microfiber just makes it super easy to call any part of the
CouchDB REST API, current or future.  This approach allows Microfiber to be very
simple and basically maintenance free as it requires no changes to support new
additions to the CouchDB API.

Documentation:

    http://docs.novacut.com/microfiber/index.html

Launchpad project:

    https://launchpad.net/microfiber
"""

from os import urandom
from io import BufferedReader, TextIOWrapper
from base64 import b32encode, b64encode
import json
from gzip import GzipFile
import time
from hashlib import sha1
import hmac
from urllib.parse import urlparse, urlencode, quote_plus
from http.client import HTTPConnection, HTTPSConnection, BadStatusLine
import ssl
import threading
from queue import Queue
import math


__all__ = (
    'random_id',
    'random_id2',

    'Server',
    'Database',

    'BadRequest',
    'Unauthorized',
    'Forbidden',
    'NotFound',
    'MethodNotAllowed',
    'NotAcceptable',
    'Conflict',
    'PreconditionFailed',
    'BadContentType',
    'BadRangeRequest',
    'ExpectationFailed',

    'ServerError',
)

__version__ = '12.09.0'
USER_AGENT = 'microfiber ' + __version__
DC3_CMD = ('/usr/bin/dc3', 'GetEnv')
DMEDIA_CMD = ('/usr/bin/dmedia-cli', 'GetEnv')

RANDOM_BITS = 120
RANDOM_BYTES = RANDOM_BITS // 8
RANDOM_B32LEN = RANDOM_BITS // 5

HTTP_IPv4_URL = 'http://127.0.0.1:5984/'
HTTPS_IPv4_URL = 'https://127.0.0.1:6984/'
HTTP_IPv6_URL = 'http://[::1]:5984/'
HTTPS_IPv6_URL = 'https://[::1]:6984/'
URL_CONSTANTS = (
    HTTP_IPv4_URL,
    HTTPS_IPv4_URL,
    HTTP_IPv6_URL,
    HTTPS_IPv6_URL,
)
DEFAULT_URL = HTTP_IPv4_URL


def random_id(numbytes=RANDOM_BYTES):
    """
    Returns a 120-bit base32-encoded random ID.

    The ID will be 24-characters long, URL and filesystem safe.  For example:

    >>> random_id()  #doctest: +SKIP
    'OVRHK3TUOUQCWIDMNFXGC4TP'

    This is how dmedia/Novacut random IDs are created, so this is "Jason
    approved", for what that's worth.
    """
    return b32encode(urandom(numbytes)).decode('utf-8')


def random_id2():
    """
    Returns a random ID with timestamp + 80 bits of base32-encoded random data.

    The ID will be 27-characters long, URL and filesystem safe.  For example:

    >>> random_id2()  #doctest: +SKIP
    '1313567384.67DFPERIOU66CT56'

    """
    return '-'.join([
        str(int(time.time())),
        b32encode(urandom(10)).decode('utf-8')
    ])



def dc3_env():
    import subprocess
    env_s = subprocess.check_output(DC3_CMD)
    return json.loads(env_s.decode('utf-8'))


def dmedia_env():
    import subprocess
    env_s = subprocess.check_output(DMEDIA_CMD)
    return json.loads(env_s.decode('utf-8'))


def dumps(obj, pretty=False):
    """
    Safe and opinionated use of ``json.dumps()``.

    This function always calls ``json.dumps()`` with *ensure_ascii=False* and
    *sort_keys=True*.

    For example:

    >>> doc = {
    ...     'hello': 'мир',
    ...     'welcome': 'все',
    ... }
    >>> dumps(doc)
    '{"hello":"мир","welcome":"все"}'

    Whereas if you directly call ``json.dumps()`` without *ensure_ascii=False*:

    >>> json.dumps(doc, sort_keys=True)
    '{"hello": "\\\\u043c\\\\u0438\\\\u0440", "welcome": "\\\\u0432\\\\u0441\\\\u0435"}'

    By default compact encoding is used, but if you supply *pretty=True*,
    4-space indentation will be used:

    >>> print(dumps(doc, pretty=True))
    {
        "hello": "мир",
        "welcome": "все"
    }

    """
    if pretty:
        return json.dumps(obj,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',',': '),
            indent=4,
        )
    return json.dumps(obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',',':'),
    )


def _json_body(obj):
    if obj is None:
        return None
    if isinstance(obj, (bytes, BufferedReader)):
        return obj
    return dumps(obj).encode('utf-8')


def _queryiter(options):
    """
    Return appropriately encoded (key, value) pairs sorted by key.

    We JSON encode the value if the key is "key", "startkey", or "endkey", or
    if the value is not an ``str``.
    """
    for key in sorted(options):
        value = options[key]
        if key in ('key', 'startkey', 'endkey') or not isinstance(value, str):
            value = dumps(value)
        yield (key, value)


def _oauth_base_string(method, baseurl, query):
    q = urlencode(tuple((k, query[k]) for k in sorted(query)))
    return '&'.join([method, quote_plus(baseurl), quote_plus(q)])


def _oauth_sign(oauth, base_string):
    key = '&'.join(
        oauth[k] for k in ('consumer_secret', 'token_secret')
    ).encode('utf-8')
    h = hmac.new(key, base_string.encode('utf-8'), sha1)
    return b64encode(h.digest()).decode('utf-8')


def _oauth_header(oauth, method, baseurl, query, testing=None):
    if testing is None:
        timestamp = str(int(time.time()))
        nonce = b32encode(urandom(10)).decode('utf-8')
    else:
        (timestamp, nonce) = testing
    o = {
        'oauth_consumer_key': oauth['consumer_key'],
        'oauth_token': oauth['token'],
        'oauth_timestamp': timestamp,
        'oauth_nonce': nonce,
        'oauth_signature_method': 'HMAC-SHA1',
        'oauth_version': '1.0',
    }
    query.update(o)
    base_string = _oauth_base_string(method, baseurl, query)
    o['oauth_signature'] = quote_plus(_oauth_sign(oauth, base_string))
    o['OAuth realm'] = ''
    value = ', '.join(
        '{}="{}"'.format(k, o[k]) for k in sorted(o)
    )
    return {'Authorization': value}


def _basic_auth_header(basic):
    b = '{username}:{password}'.format(**basic).encode('utf-8')
    b64 = b64encode(b).decode('utf-8')
    return {'Authorization': 'Basic ' + b64}


REPLICATION_KW = frozenset([
    'cancel', 
    'continuous',
    'create_target',
    'doc_ids',
    'filter',
    'proxy',
    'query_params',
])


def replication_body(source, target, **kw):
    assert REPLICATION_KW.issuperset(kw), kw
    body = {
        'source': source,
        'target': target,
    }
    body.update(kw)
    return body


def replication_peer(name, env):
    peer =  {'url': env['url'] + name}
    if env.get('oauth'):
        peer['auth'] = {'oauth': env['oauth']}
    elif env.get('basic'):
        peer['headers'] = _basic_auth_header(env['basic'])
    return peer


def push_replication(local_db, remote_db, remote_env, **kw):
    """
    Build the object to POST for push replication.

    For details on what keyword arguments you might want to use, see:

        http://wiki.apache.org/couchdb/Replication
    """
    source = local_db
    target = replication_peer(remote_db, remote_env)
    return replication_body(source, target, **kw)


def pull_replication(local_db, remote_db, remote_env, **kw):
    """
    Build the object to POST for pull replication.

    For details on what keyword arguments you might want to use, see:

        http://wiki.apache.org/couchdb/Replication
    """
    source = replication_peer(remote_db, remote_env)
    target = local_db
    return replication_body(source, target, **kw)


def id_slice_iter(rows, size=25):
    for i in range(math.ceil(len(rows) / size)):
        yield [row['id'] for row in rows[i*size : (i+1)*size]]


class HTTPError(Exception):
    """
    Base class for custom `microfiber` exceptions.
    """

    def __init__(self, response, data, method, url):
        self.response = response
        self.data = data
        self.method = method
        self.url = url
        super().__init__()

    def __str__(self):
        return '{} {}: {} {}'.format(
            self.response.status, self.response.reason, self.method, self.url
        )

    def loads(self):
        return json.loads(self.data.decode('utf-8'))


class ClientError(HTTPError):
    """
    Base class for all 4xx Client Error exceptions.
    """


class BadRequest(ClientError):
    """
    400 Bad Request.
    """


class Unauthorized(ClientError):
    """
    401 Unauthorized.
    """


class Forbidden(ClientError):
    """
    403 Forbidden.
    """


class NotFound(ClientError):
    """
    404 Not Found.
    """


class MethodNotAllowed(ClientError):
    """
    405 Method Not Allowed.
    """


class NotAcceptable(ClientError):
    """
    406 Not Acceptable.
    """


class Conflict(ClientError):
    """
    409 Conflict.

    Raised when the request resulted in an update conflict.
    """


class PreconditionFailed(ClientError):
    """
    412 Precondition Failed.
    """


class BadContentType(ClientError):
    """
    415 Unsupported Media Type.
    """


class BadRangeRequest(ClientError):
    """
    416 Requested Range Not Satisfiable.
    """


class ExpectationFailed(ClientError):
    """
    417 Expectation Failed.

    Raised when a bulk operation failed.
    """


class ServerError(HTTPError):
    """
    Used to raise exceptions for any 5xx Server Errors.
    """


errors = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound,
    405: MethodNotAllowed,
    406: NotAcceptable,
    409: Conflict,
    412: PreconditionFailed,
    415: BadContentType,
    416: BadRangeRequest,
    417: ExpectationFailed,
}


class BulkConflict(Exception):
    def __init__(self, conflicts, rows):
        self.conflicts = conflicts
        self.rows = rows
        count = len(conflicts)
        msg = ('conflict on {} doc' if count == 1 else 'conflict on {} docs')
        super().__init__(msg.format(count))


def _start_thread(target, *args):
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread


class SmartQueue(Queue):
    """
    Queue with custom get() that raises exception instances from the queue.
    """

    def get(self, block=True, timeout=None):
        item = super().get(block, timeout)
        if isinstance(item, Exception):
            raise item
        return item


def _fakelist_worker(rows, db, queue):
    try:
        for doc_ids in id_slice_iter(rows, 50):
            queue.put(db.get_many(doc_ids))
        queue.put(None)
    except Exception as e:
        queue.put(e)


class FakeList(list):
    """
    Trick ``json.dump()`` into doing memory-efficient incremental encoding.

    This class is a hack to allow `Database.dump()` to dump a large database
    while keeping the memory usage constant.

    It also provides two hacks to improve the performance of `Database.dump()`:

        1. Documents are retrieved 50 at a time using `Database.get_many()`

        2. The CouchDB requests are made in a separate thread so `json.dump()`
           can be busy doing work while we're waiting for a response
    """

    __slots__ = ('_rows', '_db')

    def __init__(self, rows, db):
        super().__init__()
        self._rows = rows
        self._db = db

    def __len__(self):
        return len(self._rows)

    def __iter__(self):
        queue = SmartQueue(2)
        thread = _start_thread(_fakelist_worker, self._rows, self._db, queue)
        while True:
            docs = queue.get()
            if docs is None:
                break
            for doc in docs:
                del doc['_rev']
                try:
                    del doc['_attachments']
                except KeyError:
                    pass
                yield doc
        thread.join()  # Make sure reader() terminates


def build_ssl_context(config):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ctx.verify_mode = ssl.CERT_REQUIRED

    # Configure certificate authorities used to verify server certs
    if 'ca_file' in config or 'ca_path' in config:
        ctx.load_verify_locations(
            cafile=config.get('ca_file'),
            capath=config.get('ca_path'),
        )
    else:
        ctx.set_default_verify_paths()

    # Configure client certificate, if provided
    if 'cert_file' in config:
        ctx.load_cert_chain(config['cert_file'],
            keyfile=config.get('key_file')
        )

    return ctx


class Context:
    """
    Reuse TCP connections between multiple `CouchBase` instances.

    When making serial requests one after another, you get considerably better
    performance when you reuse your ``HTTPConnection`` (or ``HTTPSConnection``).

    Individual `Server` and `Database` instances automatically do this: each
    thread gets its own thread-local connection that will transparently be
    reused.

    But often you'll have multiple `Server` and `Database` instances all using
    the same *env*, and if you were making requests from one to another (say
    copying docs, or saving the same doc to multiple databases), you don't
    automatically get connection reuse.

    To reuse connections among multiple `CouchBase` instances you need to create
    them with the same `Context` instance, like this:

    >>> from usercouch.misc import TempCouch
    >>> from microfiber import Context, Database
    >>> tmpcouch = TempCouch()
    >>> env = tmpcouch.bootstrap()
    >>> ctx = Context(env)
    >>> foo = Database('foo', ctx=ctx)
    >>> bar = Database('bar', ctx=ctx)
    >>> foo.ctx is bar.ctx
    True

    However, this database doesn't use the same `Context`, despite having an
    identical *env*:

    >>> baz = Database('baz', env)
    >>> baz.ctx is foo.ctx
    False

    When connecting to CouchDB via SSL, its highly recommended to use the same
    `Context` because that will allow all your SSL connections to reuse the
    same ``ssl.SSLContext``.
    """
    def __init__(self, env=None):
        if env is None:
            env = DEFAULT_URL
        if not isinstance(env, (dict, str)):
            raise TypeError(
                'env must be a `dict` or `str`; got {!r}'.format(env)
            )
        self.env = ({'url': env} if isinstance(env, str) else env)
        url = self.env.get('url', DEFAULT_URL)
        t = urlparse(url)
        if t.scheme not in ('http', 'https'):
            raise ValueError(
                'url scheme must be http or https; got {!r}'.format(url)
            )
        if not t.netloc:
            raise ValueError('bad url: {!r}'.format(url))
        self.basepath = (t.path if t.path.endswith('/') else t.path + '/')
        self.t = t
        self.url = self.full_url(self.basepath)
        self.threadlocal = threading.local()
        if t.scheme == 'https':
            ssl_config = self.env.get('ssl', {})
            self.ssl_ctx = build_ssl_context(ssl_config)
            self.check_hostname = ssl_config.get('check_hostname')

    def full_url(self, path):
        return ''.join([self.t.scheme, '://', self.t.netloc, path])

    def get_connection(self):
        if self.t.scheme == 'http':
            return HTTPConnection(self.t.netloc)
        else:
            return HTTPSConnection(self.t.netloc,
                context=self.ssl_ctx,
                check_hostname=self.check_hostname
            )

    def get_threadlocal_connection(self):
        if not hasattr(self.threadlocal, 'connection'):
            self.threadlocal.connection = self.get_connection()
        return self.threadlocal.connection

    def get_auth_headers(self, method, path, query, testing=None):
        if 'oauth' in self.env:
            baseurl = self.full_url(path)
            return _oauth_header(
                self.env['oauth'], method, baseurl, dict(query), testing
            )
        if 'basic' in self.env:
            return _basic_auth_header(self.env['basic'])
        return {}


class CouchBase(object):
    """
    Base class for `Server` and `Database`.

    This class is a simple a adapter to make it easy to call a JSON loving REST
    API similar to CouchDB (especially if it happens to be CouchDB).  To
    simplify things, there are some assumptions we can make:

        * Request bodies are empty or JSON, except when you PUT an attachment

        * Response bodies are JSON, except when you GET an attachment

    With just 7 methods you can access the entire CouchDB API quite elegantly:

        * `CouchBase.post()`
        * `CouchBase.put()`
        * `CouchBase.get()`
        * `CouchBase.delete()`
        * `CouchBase.head()`
        * `CouchBase.put_att()`
        * `CouchBase.get_att()`

    The goal of `microfiber` is to be as simple as possible and not require
    constant API work to stay up to date with CouchDB API changes.

    This class is called "CouchBase" because I think it's a really cool name.
    Seriously, someone should start like a band or a company and call it
    "CouchBase".
    """

    def __init__(self, env=None, ctx=None):
        self.ctx = (Context(env) if ctx is None else ctx)
        self.env = self.ctx.env
        self.basepath = self.ctx.basepath
        self.url = self.ctx.url

    def _full_url(self, path):
        return self.ctx.full_url(path)

    def _request(self, method, parts, options, body=None, headers=None):
        h = {
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        }
        if headers:
            h.update(headers)
        path = (self.basepath + '/'.join(parts) if parts else self.basepath)
        query = (tuple(_queryiter(options)) if options else tuple())
        h.update(self.ctx.get_auth_headers(method, path, query))
        if query:
            path = '?'.join([path, urlencode(query)])
        conn = self.ctx.get_threadlocal_connection()
        for retry in range(2):
            try:
                conn.request(method, path, body, h)
                response = conn.getresponse()
                data = response.read()
                break
            except BadStatusLine as e:
                conn.close()
                if retry == 1:
                    raise e
            except Exception as e:
                conn.close()
                raise e
        if response.status >= 500:
            raise ServerError(response, data, method, path)
        if response.status >= 400:
            E = errors.get(response.status, ClientError)
            raise E(response, data, method, path)
        return (response, data)

    def post(self, obj, *parts, **options):
        """
        POST *obj*.

        For example, to create the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.post({'_id': 'bar'}, 'foo')  #doctest: +SKIP
        {'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}

        Or to compact the database "foo":

        >>> cb.post(None, 'foo', '_compact')  #doctest: +SKIP
        {'ok': True}

        """
        (response, data) = self._request('POST', parts, options,
            body=_json_body(obj),
            headers={'Content-Type': 'application/json'},
        )
        return json.loads(data.decode('utf-8'))

    def put(self, obj, *parts, **options):
        """
        PUT *obj*.

        For example, to create the database "foo":

        >>> cb = CouchBase()
        >>> cb.put(None, 'foo')  #doctest: +SKIP
        {'ok': True}

        Or to create the doc "bar" in the database "foo":

        >>> cb.put({'micro': 'fiber'}, 'foo', 'bar')  #doctest: +SKIP
        {'rev': '1-fae0708c46b4a6c9c497c3a687170ad6', 'ok': True, 'id': 'bar'}

        """
        (response, data) = self._request('PUT', parts, options,
            body=_json_body(obj),
            headers={'Content-Type': 'application/json'},
        )
        return json.loads(data.decode('utf-8'))

    def get(self, *parts, **options):
        """
        Make a GET request.

        For example, to get the welcome info from CouchDB:

        >>> cb = CouchBase()
        >>> cb.get()  #doctest: +SKIP
        {'couchdb': 'Welcome', 'version': '1.0.1'}

        Or to request the doc "bar" from the database "foo", including any
        attachments:

        >>> cb.get('foo', 'bar', attachments=True)  #doctest: +SKIP
        {'_rev': '1-967a00dff5e02add41819138abb3284d', '_id': 'bar'}
        """
        (response, data) = self._request('GET', parts, options)
        return json.loads(data.decode('utf-8'))

    def delete(self, *parts, **options):
        """
        Make a DELETE request.

        For example, to delete the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.delete('foo', 'bar', rev='1-fae0708c46b4a6c9c497c3a687170ad6')  #doctest: +SKIP
        {'rev': '2-18995243f0ebd1066fcb191a28d1222a', 'ok': True, 'id': 'bar'}

        Or to delete the database "foo":

        >>> cb.delete('foo')  #doctest: +SKIP
        {'ok': True}

        """
        (response, data) = self._request('DELETE', parts, options)
        return json.loads(data.decode('utf-8'))

    def head(self, *parts, **options):
        """
        Make a HEAD request.

        Returns a ``dict`` containing the response headers from the HEAD
        request.
        """
        (response, data) = self._request('HEAD', parts, options)
        return dict(response.getheaders())

    def put_att(self, mime, data, *parts, **options):
        """
        PUT an attachment.

        For example, to upload the attachment "baz" for the doc "bar" in the
        database "foo":

        >>> cb = CouchBase()
        >>> cb.put_att('image/png', b'da pic', 'foo', 'bar', 'baz')  #doctest: +SKIP
        {'rev': '1-f759cc40458cdd5bd8ae379174bc53d9', 'ok': True, 'id': 'bar'}

        Note that you don't need any attachment-specific method for DELETE -
        just use `CouchBase.delete()`.

        :param mime: The Content-Type, eg ``'image/jpeg'``
        :param data: a ``bytes`` instance or an open file, passed directly to
            HTTPConnection.request()
        :param parts: path components to construct URL relative to base path
        :param options: optional keyword arguments to include in query
        """
        (response, data) = self._request('PUT', parts, options,
            body=data,
            headers={'Content-Type': mime},
        )
        return json.loads(data.decode('utf-8'))

    def get_att(self, *parts, **options):
        """
        GET an attachment.

        Returns a (mime, data) tuple with the attachment's Content-Type and
        data.  For example, to download the attachment "baz" for the doc "bar"
        in the database "foo":

        >>> cb = CouchBase()
        >>> cb.get_att('foo', 'bar', 'baz')  #doctest: +SKIP
        ('image/png', b'da pic')

        Note that you don't need any attachment-specific method for DELETE -
        just use `CouchBase.delete()`.

        :param parts: path components to construct URL relative to base path
        :param options: optional keyword arguments to include in query
        """
        (response, data) = self._request('GET', parts, options)
        return (response.getheader('Content-Type'), data)


class Server(CouchBase):
    """
    All the `CouchBase` methods plus some server-specific niceties.

    For example:

    >>> s = Server('http://localhost:5984/')
    >>> s
    Server('http://localhost:5984/')
    >>> s.url
    'http://localhost:5984/'
    >>> s.basepath
    '/'

    Niceties:

        * Server.database(name) - return a Database instance with server URL
    """

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.url)

    def database(self, name, ensure=False):
        """
        Create a `Database` with the same `Context` as this `Server`.
        """
        db = Database(name, ctx=self.ctx)
        if ensure:
            db.ensure()
        return db

    def push(self, local_db, remote_db, remote_env, **kw):
        obj = push_replication(local_db, remote_db, remote_env, **kw)
        return self.post(obj, '_replicate')

    def pull(self, local_db, remote_db, remote_env, **kw):
        obj = pull_replication(local_db, remote_db, remote_env, **kw)
        return self.post(obj, '_replicate')


class Database(CouchBase):
    """
    All the `CouchBase` methods plus some database-specific niceties.

    For example:

    >>> db = Database('dmedia', 'http://localhost:5984/')
    >>> db
    Database('dmedia', 'http://localhost:5984/')
    >>> db.name
    'dmedia'
    >>> db.url
    'http://localhost:5984/'
    >>> db.basepath
    '/dmedia/'


    Niceties:

        * `Database.server()` - return a `Server` pointing at same URL
        * `Database.ensure()` - ensure the database exists
        * `Database.save(doc)` - save to CouchDB, update doc _id & _rev in place
        * `Database.save_many(docs)` - as above, but with a list of docs
        * `Database.get_many(doc_ids)` - retrieve many docs at once
        * `Datebase.view(design, view, **options)` - shortcut method, that's all
    """
    def __init__(self, name, env=None, ctx=None):
        super().__init__(env, ctx)
        self.name = name
        self.basepath += (name + '/')

    def __repr__(self):
        return '{}({!r}, {!r})'.format(
            self.__class__.__name__, self.name, self.url
        )

    def server(self):
        """
        Create a `Server` with the same `Context` as this `Database`.
        """
        return Server(ctx=self.ctx)

    def ensure(self):
        """
        Ensure the database exists.

        This method will attempt to create the database, and will handle the
        `PreconditionFailed` exception raised if the database already exists.

        Higher level code can safely call this method at any time, and it only
        results in a single PUT /db request being made.
        """
        try:
            self.put(None)
            return True
        except PreconditionFailed:
            return False

    def save(self, doc):
        """
        POST doc to CouchDB, update doc _rev in place.

        For example:

        >>> db = Database('foo')
        >>> doc = {'_id': 'bar'}
        >>> db.save(doc)  #doctest: +SKIP
        {'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}
        >>> doc  #doctest: +SKIP
        {'_rev': '1-967a00dff5e02add41819138abb3284d', '_id': 'bar'}
        >>> doc['a'] = 1  #doctest: +SKIP
        >>> db.save(doc)  #doctest: +SKIP
        {'rev': '2-4f54ab3740f3104eec1cf2ec2b0327ed', 'ok': True, 'id': 'bar'}
        >>> doc  #doctest: +SKIP
        {'a': 1, '_rev': '2-4f54ab3740f3104eec1cf2ec2b0327ed', '_id': 'bar'}

        If *doc* has no _id, one generated using `random_id()` and added to
        *doc* in-place prior to making the request to CouchDB.

        This method is inspired by the identical (and highly useful) method in
        python-couchdb:

            http://packages.python.org/CouchDB/client.html#database
        """
        if '_id' not in doc:
            doc['_id'] = random_id()
        r = self.post(doc)
        doc['_rev'] = r['rev']
        return r

    def save_many(self, docs):
        """
        Bulk-save using non-atomic semantics, updates all _rev in-place.

        This method is similar `Database.save()`, except this method operates on
        a list of many docs at once.

        If there are conflicts, a `BulkConflict` exception will be raised, whose
        ``conflicts`` attribute will be a list of the documents for which there
        were conflicts.  Your request will *not* have modified these conflicting
        documents in the database, similar to `Database.save()`.

        However, all non-conflicting documents will have been saved and their
        _rev updated in-place.
        """
        for doc in filter(lambda d: '_id' not in d, docs):
            doc['_id'] = random_id()
        rows = self.post({'docs': docs}, '_bulk_docs')
        conflicts = []
        for (doc, row) in zip(docs, rows):
            assert doc['_id'] == row['id']
            if 'rev' in row:
                doc['_rev'] = row['rev']
            else:
                conflicts.append(doc)
        if conflicts:
            raise BulkConflict(conflicts, rows)
        return rows

    def bulksave(self, docs):
        """
        Bulk-save using all-or-nothing semantics, updates all _rev in-place.

        This method is similar `Database.save()`, except this method operates on
        a list of many docs at once.

        Note: for subtle reasons that take a while to explain, you probably
        don't want to use this method.
        """
        for doc in filter(lambda d: '_id' not in d, docs):
            doc['_id'] = random_id()
        rows = self.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')
        for (doc, row) in zip(docs, rows):
            assert doc['_id'] == row['id']
            doc['_rev'] = row['rev']
        return rows

    def get_many(self, doc_ids):
        """
        Convenience method to retrieve multiple documents at once.

        As CouchDB has a rather large per-request overhead, retrieving multiple
        documents at once can greatly improve performance.
        """
        result = self.post({'keys': doc_ids}, '_all_docs', include_docs=True)
        return [row.get('doc') for row in result['rows']]

    def view(self, design, view, **options):
        """
        Shortcut for making a GET request to a view.

        No magic here, just saves you having to type "_design" and "_view" over
        and over.  This:

            ``Database.view(design, view, **options)``

        Is just a shortcut for:

            ``Database.get('_design', design, '_view', view, **options)``
        """
        if 'reduce' not in options:
            options['reduce'] = False
        return self.get('_design', design, '_view', view, **options)

    def dump(self, filename):
        """
        Dump this database to regular JSON file *filename*.

        For example:

        >>> db = Database('foo')  #doctest: +SKIP
        >>> db.dump('foo.json')  #doctest: +SKIP

        Or if *filename* ends with ``'.json.gz'``, the file will be
        gzip-compressed as it is written:

        >>> db.dump('foo.json.gz')  #doctest: +SKIP

        CouchDB is a bit awkward in that its API doesn't offer a nice way to
        make a request whose response is suitable for writing directly to a
        file, without decoding/encoding.  It would be nice if that dump could
        be loaded directly from the file as well.  One of the biggest issues is
        that a dump really needs to have doc['_rev'] removed.

        This method is a compromise on many fronts, but it was made with these
        priorities:

            1. Readability of the dumped JSON file

            2. High performance and low memory usage, despite the fact that
               we must encode and decode each doc
        """
        if filename.lower().endswith('.json.gz'):
            _fp = open(filename, 'wb')
            fp = TextIOWrapper(GzipFile('docs.json', fileobj=_fp, mtime=1))
        else:
            fp = open(filename, 'w')
        rows = self.get('_all_docs', endkey='_')['rows']
        docs = FakeList(rows, self)
        json.dump(docs, fp,
            ensure_ascii=False,
            sort_keys=True,
            indent=4,
            separators=(',', ': '),
        )

