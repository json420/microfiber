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

from io import BufferedReader, TextIOWrapper
import os
from base64 import b64encode
import json
from gzip import GzipFile
import time
from hashlib import sha1
import hmac
from urllib.parse import urlparse, urlencode, quote_plus, ParseResult
import ssl
import threading
from queue import Queue
import math
import platform
from collections import namedtuple
import logging

from dbase32 import random_id, RANDOM_BITS, RANDOM_BYTES, RANDOM_B32LEN
from degu.client import Client, SSLClient, build_client_sslctx


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

__version__ = '16.03.0'
log = logging.getLogger()
USER_AGENT = 'Microfiber/{} ({} {}; {})'.format(__version__, 
    platform.dist()[0], platform.dist()[1], platform.machine()
)

DC3_CMD = ('/usr/bin/dc3', 'GetEnv')
DMEDIA_CMD = ('/usr/bin/dmedia-cli', 'GetEnv')

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

Attachment = namedtuple('Attachment', 'content_type data')


def create_client(url, **options):
    """
    Convenience function to create a `degu.client.Client` from a URL.

    For example:

    >>> create_client('http://www.example.com/')
    Client(('www.example.com', 80))

    """
    t = (url if isinstance(url, ParseResult) else urlparse(url))
    if t.scheme != 'http':
        raise ValueError("scheme must be 'http', got {!r}".format(t.scheme))
    port = (80 if t.port is None else t.port)
    return Client((t.hostname, port), **options)


def create_sslclient(sslctx, url, **options):
    """
    Convenience function to create an `SSLClient` from a URL.
    """
    t = (url if isinstance(url, ParseResult) else urlparse(url))
    if t.scheme != 'https':
        raise ValueError("scheme must be 'https', got {!r}".format(t.scheme))
    port = (443 if t.port is None else t.port)
    return SSLClient(sslctx, (t.hostname, port), **options)


class BulkConflict(Exception):
    """
    Raised by `Database.save_many()` when one or more conflicts occur.
    """
    def __init__(self, conflicts, rows):
        self.conflicts = conflicts
        self.rows = rows
        count = len(conflicts)
        msg = ('conflict on {} doc' if count == 1 else 'conflict on {} docs')
        super().__init__(msg.format(count))


class HTTPError(Exception):
    """
    Base class for exceptions raised based on HTTP response status.
    """

    def __init__(self, response, method, url):
        self.response = response
        self.data = (b'' if response.body is None else response.body.read())
        self.method = method
        self.url = url
        super().__init__()

    def __str__(self):
        return '{} {}: {} {}'.format(
            self.response.status, self.response.reason, self.method, self.url
        )


class ClientError(HTTPError):
    """
    Base class for all 4xx Client Error exceptions.
    """


class BadRequest(ClientError):
    '400 Bad Request'

class Unauthorized(ClientError):
    '401 Unauthorized'

class Forbidden(ClientError):
    '403 Forbidden'

class NotFound(ClientError):
    '404 Not Found'

class MethodNotAllowed(ClientError):
    '405 Method Not Allowed'

class NotAcceptable(ClientError):
    '406 Not Acceptable'

class Conflict(ClientError):
    '409 Conflict'

class Gone(ClientError):
    '410 Gone'

class LengthRequired(ClientError):
    '411 Length Required'

class PreconditionFailed(ClientError):
    '412 Precondition Failed'

class BadContentType(ClientError):
    '415 Unsupported Media Type'

class BadRangeRequest(ClientError):
    '416 Requested Range Not Satisfiable'

class ExpectationFailed(ClientError):
    '417 Expectation Failed'

class EnhanceYourCalm(ClientError):
    '420 Enhance Your Calm'


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
    410: Gone,
    411: LengthRequired,
    412: PreconditionFailed,
    415: BadContentType,
    416: BadRangeRequest,
    417: ExpectationFailed,
    420: EnhanceYourCalm,
}


def random_id2():
    """
    Returns a random ID with timestamp + 80 bits of base32-encoded random data.

    The ID will be 27-characters long, URL and filesystem safe.  For example:

    >>> random_id2()  #doctest: +SKIP
    '1313567384.67DFPERIOU66CT56'

    """
    return '-'.join([str(int(time.time())), random_id(10)])


def dc3_env():
    import subprocess
    env_s = subprocess.check_output(DC3_CMD)
    return json.loads(env_s.decode())


def dmedia_env():
    import subprocess
    env_s = subprocess.check_output(DMEDIA_CMD)
    return json.loads(env_s.decode())


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
    return dumps(obj).encode()


def encode_attachment(attachment):
    """
    Encode *attachment* for use in ``doc['_attachments']``.

    For example:

    >>> attachment = Attachment('image/png', b'PNG data')
    >>> dumps(encode_attachment(attachment))
    '{"content_type":"image/png","data":"UE5HIGRhdGE="}'

    :param attachment: an `Attachment` namedtuple
    """
    assert isinstance(attachment, tuple)
    assert len(attachment) == 2
    (content_type, data) = attachment
    assert isinstance(content_type, str)
    return {
        'content_type': content_type,
        'data': b64encode(data).decode(),        
    }


def has_attachment(doc, name):
    """
    Return True if *doc* has an attachment named *name*.

    For example, when the attachment isn't present:

    >>> has_attachment({}, 'thumbnail')
    False
    >>> has_attachment({'_attachments': {}}, 'thumbnail')
    False

    Or when the attachment is present:

    >>> doc= {
    ...    '_attachments': {
    ...         'thumbnail': {
    ...             'content_type': 'image/png',
    ...             'data': 'UE5HIGRhdGE=',
    ...         }
    ...     }
    ... }
    ...
    >>> has_attachment(doc, 'thumbnail')
    True

    """
    try:
        doc['_attachments'][name]
        return True
    except KeyError:
        return False


def _queryiter(options):
    """
    Return appropriately encoded (key, value) pairs sorted by key.

    We JSON encode the value if the key is "key", "startkey", or "endkey", or
    if the value is not an ``str``.
    """
    for key in sorted(options):
        value = options[key]
        if key in ('key', 'startkey', 'endkey') or not isinstance(value, str):
            value = json.dumps(value, sort_keys=True, separators=(',',':'))
        yield (key, value)


def _oauth_base_string(method, baseurl, query):
    q = urlencode(tuple((k, query[k]) for k in sorted(query)))
    return '&'.join([method, quote_plus(baseurl), quote_plus(q)])


def _oauth_sign(oauth, base_string):
    key = '&'.join(
        oauth[k] for k in ('consumer_secret', 'token_secret')
    ).encode()
    h = hmac.new(key, base_string.encode(), sha1)
    return b64encode(h.digest()).decode()


def _oauth_header(oauth, method, baseurl, query, testing=None):
    if testing is None:
        timestamp = str(int(time.time()))
        nonce = random_id()
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
    return {'authorization': value}


def basic_auth_header(basic):
    b = '{username}:{password}'.format(**basic).encode()
    return 'Basic ' + b64encode(b).decode()


def _basic_auth_header(basic):
    return {'authorization': basic_auth_header(basic)}


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
    if 'context' in config:
        ctx = config['context']
        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.options & ssl.OP_NO_COMPRESSION
        return ctx
    return build_client_sslctx(config)


class Context:
    """
    Reuse TCP connections between multiple `CouchBase` instances.

    When making serial requests one after another, you get considerably better
    performance when you reuse your ``degu.client.Connection``.

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

    __slots__ = ('env', 'basepath', 't', 'url', 'threadlocal', 'client')

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
            sslconfig = self.env.get('ssl', {})
            sslctx = build_ssl_context(sslconfig)
            self.client = create_sslclient(sslctx, self.t)
        else:
            self.client = create_client(self.t)

    def full_url(self, path):
        return ''.join([self.t.scheme, '://', self.t.netloc, path])

    def get_threadlocal_connection(self):
        conn = getattr(self.threadlocal, 'connection', None)
        if conn is None or conn.closed:
            conn = self.client.connect()
            self.threadlocal.connection = conn
        return conn

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

    def raw_request(self, method, path, body, headers):
        conn = self.ctx.get_threadlocal_connection()

        # Hack for API compatabilty back to when Microfiber used `http.client`
        # instead of `degu.client`:
        if isinstance(body, BufferedReader):
            if 'content-length' in headers:
                content_length = headers['content-length']
            else:
                content_length = os.stat(body.fileno()).st_size
            body = conn.bodies.Body(body, content_length)

        # We automatically retry once in case connection was closed by server:
        try:
            return conn.request(method, path, headers, body)
        except ConnectionError:
            pass
        conn = self.ctx.get_threadlocal_connection()
        return conn.request(method, path, headers, body)

    def request(self, method, parts, options, body=None, headers=None):
        h = {'user-agent': USER_AGENT}
        if headers:
            h.update(headers)
        path = (self.basepath + '/'.join(parts) if parts else self.basepath)
        query = (tuple(_queryiter(options)) if options else tuple())
        h.update(self.ctx.get_auth_headers(method, path, query))
        if query:
            path = '?'.join([path, urlencode(query)])
        response = self.raw_request(method, path, body, h)
        if response.status >= 500:
            raise ServerError(response, method, path)
        if response.status >= 400:
            E = errors.get(response.status, ClientError)
            raise E(response, method, path)
        return response

    def recv_json(self, method, parts, options, body=None, headers=None):
        if headers is None:
            headers = {}
        headers['accept'] = 'application/json'
        response = self.request(method, parts, options, body, headers)
        data = (b'' if response.body is None else response.body.read())
        return json.loads(data.decode())

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
        return self.recv_json('POST', parts, options, _json_body(obj),
            {'content-type': 'application/json'}
        )

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
        return self.recv_json('PUT', parts, options, _json_body(obj),
            {'content-type': 'application/json'}
        )

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
        return self.recv_json('GET', parts, options)

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
        return self.recv_json('DELETE', parts, options)

    def head(self, *parts, **options):
        """
        Make a HEAD request.

        Returns a ``dict`` containing the response headers from the HEAD
        request.
        """
        response = self.request('HEAD', parts, options)
        return response.headers

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
        return self.recv_json('PUT', parts, options, data,
            {'content-type': mime}
        )

    def put_att2(self, attachment, *parts, **options):
        """
        Experiment for possible CouchBase.put_att() API change.

        WARNING: regardless how the experiment turns out, this method will be
        removed!
        """
        return self.recv_json('PUT', parts, options, attachment.data,
            {'content-type': attachment.content_type}
        )

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
        response = self.request('GET', parts, options)
        content_type = response.headers['content-type']
        data = (b'' if response.body is None else response.body.read())
        return Attachment(content_type, data)


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

    def database(self, name):
        """
        Create a `Database` with the same `Context` as this `Database`.
        """
        return Database(name, ctx=self.ctx)

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

    def compact(self, synchronous=False):
        log.info('compacting %r', self)
        self.post(None, '_compact')
        if synchronous:
            self.wait_for_compact()

    def wait_for_compact(self):
        if not self.get()['compact_running']:
            return
        start = time.monotonic()
        time.sleep(1)
        while self.get()['compact_running']:
            log.info('waiting compact to finish: %r', self)
            time.sleep(1)
        delta = time.monotonic() - start
        log.info('%.3f to compact %r', delta, self)

    def iter_all_docs(self, chunksize=50):
        """
        Iterate through all docs in the database without duplicates.

        Experimental, not part of the stable API yet!
        """
        assert isinstance(chunksize, int)
        assert chunksize >= 10
        kw = {
            'limit': chunksize,
            'include_docs': True,
        }
        while True:
            rows = self.get('_all_docs', **kw)['rows']
            if not rows:
                break
            if rows[0]['id'] != kw.get('startkey_docid'):
                yield rows[0]['doc']
            for row in rows[1:]:
                yield row['doc']
            if len(rows) < chunksize:
                break
            kw['startkey_docid'] = rows[-1]['id']

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

    def delete_many(self, docs):
        """
        Deleted a list of docs.

        Experimental, not part of the stable API yet!
        """
        for doc in docs:
            doc['_deleted'] = True
            assert '_id' in doc
        return self.save_many(docs)

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

    def get_defaults(self, defaults):
        """
        Experiemental, not part of stable API yet!
        """
        assert isinstance(defaults, list)
        docs = self.get_many([d['_id'] for d in defaults])
        return [
            default if doc is None else doc
            for (default, doc) in zip(defaults, docs)
        ]

    def view(self, design, view, **options):
        """
        Shortcut for making a GET request to a view.

        No magic here, just saves you having to type "_design" and "_view" over
        and over.  This:

            ``Database.view(design, view, **options)``

        Is just a shortcut for:

            ``Database.get('_design', design, '_view', view, **options)``
        """
        options.setdefault('reduce', False)
        if 'keys' in options:
            obj = {'keys': options.pop('keys')}
            return self.post(obj, '_design', design, '_view', view, **options)
        else:
            return self.get('_design', design, '_view', view, **options)

    def iter_view(self, design, view, key, chunksize=50):
        """
        Iterate through all docs in a view for a specific key.

        The docs with be yielded in sorted order by ``doc['_id']``.

        Experimental, not part of the stable API yet!
        """
        assert isinstance(chunksize, int) and chunksize >= 10
        kw = {
            'key': key,
            'limit': chunksize,
            'include_docs': True,
        }
        while True:
            rows = self.view(design, view, **kw)['rows']
            if not rows:
                break
            if rows[0]['id'] != kw.get('startkey_docid'):
                yield rows[0]['doc']
            for row in rows[1:]:
                yield row['doc']
            if len(rows) < chunksize:
                break
            kw['startkey_docid'] = rows[-1]['id']

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

    def update(self, func, doc, *args):
        """
        Use *func* to update *doc* and then save, with one conflict retry.

        *func* is expected to apply an update to *doc* in-place.  It will be
        called like this::

            func(doc, *args)

        Then `Database.save()` is used to try to save the doc.  If there is a
        `Conflict`, the latest revision of doc is retrieved with `Database.get()`
        and *func* is called again, this time to update the new doc in-place::

            func(new, *args)

        Then the new doc is saved to CouchDB with `Database.save()`, but this
        time no special handling is done for a `Conflict`.  Only a single retry
        is attempted.

        The return value is the final doc, with the in-place updates performed
        by *func()*, and with doc['_rev'] changed in-place by `Database.save()`.

        The calling code should be sure to keep a reference to the returned doc,
        because in the case of a `Conflict`, it wont be the original ``dict``
        instance that `Database.update()` was called with.

        In general, you'll want to use this pattern::

            doc = db.update(func, doc, 'foo', 'bar')
        """
        _id = doc['_id']
        func(doc, *args)
        try:
            self.save(doc)
            return doc
        except Conflict:
            log.warning('Conflict saving %s', _id)
        doc = self.get(_id)
        func(doc, *args)
        self.save(doc)
        return doc

    def get_tophash(self):
        parts = ('_all_docs',)
        options = {'include_docs': True}
        headers = {'accept': 'application/json'}
        response = self.request('GET', parts, options, None, headers)
        assert response.headers['content-type'] == 'application/json'
        assert response.headers['transfer-encoding'] == 'chunked'
        h = sha1()
        for (extension, data) in response.body:
            h.update(data)
        return h.hexdigest()
