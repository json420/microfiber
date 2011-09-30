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

import sys
from os import urandom
import io
from base64 import b32encode, b64encode
from json import dumps, loads
import time
from hashlib import sha1
import hmac
import subprocess
from urllib.parse import urlparse, urlencode, quote_plus
from http.client import HTTPConnection, HTTPSConnection, BadStatusLine


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

__version__ = '11.10.0'
USER_AGENT = 'microfiber ' + __version__
SERVER = 'http://localhost:5984/'
DC3_CMD = ('/usr/bin/dc3-control', 'GetEnv')

RANDOM_BITS = 120
RANDOM_BYTES = RANDOM_BITS // 8
RANDOM_B32LEN = RANDOM_BITS // 5


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
    return '.'.join([
        str(int(time.time())),
        b32encode(urandom(10)).decode('utf-8')
    ])


def dc3_env():
    env_s = subprocess.check_output(DC3_CMD)
    return loads(env_s.decode('utf-8'))


def _json_body(obj):
    if obj is None:
        return None
    if isinstance(obj, (bytes, io.BufferedReader, io.BytesIO)):
        return obj
    return dumps(obj, sort_keys=True, separators=(',',':')).encode('utf-8')


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
    if sys.version_info >= (3, 0):
        return b64encode(h.digest()).decode('utf-8')
    return b64encode(h.digest())


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


class HTTPError(Exception):
    """
    Base class for custom `microfiber` exceptions.
    """

    def __init__(self, response, data, method, url):
        self.response = response
        self.data = data
        self.method = method
        self.url = url
        super(HTTPError, self).__init__()

    def __str__(self):
        return '{} {}: {} {}'.format(
            self.response.status, self.response.reason, self.method, self.url
        )

    def loads(self):
        return loads(self.data.decode('utf-8'))


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

    def __init__(self, env=SERVER):
        self.env = ({'url': env} if isinstance(env, str) else env)
        assert isinstance(self.env, dict)
        url = self.env.get('url', SERVER)
        t = urlparse(url)
        if t.scheme not in ('http', 'https'):
            raise ValueError(
                'url scheme must be http or https: {!r}'.format(url)
            )
        if not t.netloc:
            raise ValueError('bad url: {!r}'.format(url))
        self.scheme = t.scheme
        self.netloc = t.netloc
        self.basepath = (t.path if t.path.endswith('/') else t.path + '/')
        self.url = self._full_url(self.basepath)
        self._oauth = self.env.get('oauth')
        self._basic = self.env.get('basic')
        klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
        self.conn = klass(t.netloc)

    def _full_url(self, path):
        return ''.join([self.scheme, '://', self.netloc, path])

    def _request(self, method, parts, options, body=None, headers=None):
        h = {
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        }
        if headers:
            h.update(headers)
        path = (self.basepath + '/'.join(parts) if parts else self.basepath)
        query = (tuple(_queryiter(options)) if options else tuple())
        if self._oauth:
            baseurl = self._full_url(path)
            h.update(
                _oauth_header(self._oauth, method, baseurl, dict(query))
            )
        elif self._basic:
            h.update(_basic_auth_header(self._basic))
        if query:
            path = '?'.join([path, urlencode(query)])
        for retry in range(2):
            try:
                self.conn.request(method, path, body, h)
                response = self.conn.getresponse()
                data = response.read()
                break
            except BadStatusLine as e:
                self.conn.close()
                if retry == 1:
                    raise e
            except Exception as e:
                self.conn.close()
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
        return loads(data.decode('utf-8'))

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
        return loads(data.decode('utf-8'))

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
        return loads(data.decode('utf-8'))

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
        return loads(data.decode('utf-8'))

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
        return loads(data.decode('utf-8'))

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

    def __init__(self, env=SERVER):
        super(Server, self).__init__(env)

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.url)

    def database(self, name, ensure=False):
        """
        Return a new `Database` instance for the database *name*.
        """
        db = Database(name, self.env)
        if ensure:
            db.ensure()
        return db


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
        * `Database.bulksave(docs)` - as above, but with a list of docs
        * `Datebase.view(design, view, **options)` - shortcut method, that's all
    """
    def __init__(self, name, env=SERVER):
        super(Database, self).__init__(env)
        self.name = name
        self.basepath += (name + '/')

    def __repr__(self):
        return '{}({!r}, {!r})'.format(
            self.__class__.__name__, self.name, self.url
        )

    def server(self):
        """
        Return a `Server` instance pointing at the same URL as this database.
        """
        return Server(self.env)

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

    def bulksave(self, docs):
        """
        POST a list of docs to _bulk_docs, update all _rev in place.

        This method works just like `Database.save()`, except on a whole list
        of docs all at once.
        """
        for doc in filter(lambda d: '_id' not in d, docs):
            doc['_id'] = random_id()
        rows = self.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')
        for (doc, row) in zip(docs, rows):
            doc['_rev'] = row['rev']
        return rows

    def view(self, design, view, **options):
        """
        Shortcut for making a GET request to a view.

        No magic here, just saves you having to type "_design" and "_view" over
        and over.  This:

            ``Database.view(design, view, **options)``

        Is just a shortcut for:

            ``Database.get('_design', design, '_view', view, **options)``
        """
        return self.get('_design', design, '_view', view, **options)
