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

microfiber is an abstract adapter for making HTTP requests to an arbitrary JSON
loving REST API like CouchDB.  Rather than wrapping the API in a bunch of
one-off methods, microfiber just makes it super easy to call any part of the
CouchDB REST API, current or future.  This approach allows microfiber to be very
simple and basically maintenance free as it requires no changes to support new
additions to the CouchDB API.

For example, with python-couchdb you compact the database like this:

>>> database.compact()  #doctest: +SKIP


With microfiber, you can accomplish the same thing one of two ways:

>>> database.post(None, '_compact')  #doctest: +SKIP
>>> server.post(None, 'mydb', '_compact')  #doctest: +SKIP


Depending on your situation, python-couchdb may still be a better fit, so to
each their own.  If you're new to CouchDB, you will probably find it much easier
to get started with python-couchdb.  Likewise, if you're coming from the SQL
world and like an ORM-style API, you will probably feel more at home with
python-couchdb.

However, if you know the CouchDB REST API or want to learn it, you will find
microfiber a more harmonious experience.  Also, microfiber is *very* lightweight
(1 Python file), fast, and memory efficient.  Unlike python-couchdb, microfiber
doesn't use any wrappers around the results returned from CouchDB, so it's less
prone to high memory usage and memory fragmentation problems in, say, a long
running server process.

Long story short, the microfiber API is the CouchDB REST API, and nothing more.
For example:

>>> from microfiber import Server
>>> s = Server()
>>> s
Server('http://localhost:5984/')


Create a database:

>>> s.put(None, 'mydb')  #doctest: +SKIP
{'ok': True}


Create a doc:

>>> s.post({'_id': 'foo'}, 'mydb')  #doctest: +SKIP
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'foo'}


Also create a doc:

>>> s.put({}, 'mydb', 'bar')  #doctest: +SKIP
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}


Upload attachment:

>>> s.put_att('image/png', b'da picture', 'mydb', 'baz', 'pic')  #doctest: +SKIP
{'rev': '1-7c17d20f43962e360062659b4bcd8aea', 'ok': True, 'id': 'baz'}


For CouchDB API documentation, see:

    http://techzone.couchbase.com/sites/default/files/uploads/all/documentation/couchbase-api.html

For python-couchdb documentation, see:

    http://packages.python.org/CouchDB/
"""

import sys
from os import urandom
from base64 import b32encode
from json import dumps, loads
import time
if sys.version_info >= (3, 0):
    from urllib.parse import urlparse, urlencode
    from http.client import HTTPConnection, HTTPSConnection, BadStatusLine
    strtype = str
else:
    from urlparse import urlparse
    from urllib import urlencode
    from httplib import HTTPConnection, HTTPSConnection, BadStatusLine
    strtype = basestring

try:
    from oauth import oauth
except ImportError:
    oauth = None


__all__ = (
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
)

__version__ = '0.2.0'
USER_AGENT = 'microfiber ' + __version__
SERVER = 'http://localhost:5984/'


def random_id():
    """
    Returns a 120-bit base32-encoded random ID.

    The ID will be 24-characters long, URL and filesystem safe.  For example:

    >>> random_id()  #doctest: +SKIP
    'OVRHK3TUOUQCWIDMNFXGC4TP'

    This is how dmedia/Novacut random IDs are created, so this is "Jason
    approved", for what that's worth.
    """
    return b32encode(urandom(15)).decode('ascii')


def random_id2():
    """
    Returns a random ID with timestamp + 80 bits of base32-encoded random data.

    The ID will be 27-characters long, URL and filesystem safe.  For example:

    >>> random_id2()  #doctest: +SKIP
    '1313567384.67DFPERIOU66CT56'

    """
    return '.'.join([
        str(int(time.time())),
        b32encode(urandom(10)).decode('ascii')
    ])


def _json_body(obj):
    if isinstance(obj, (dict, list)):
        return dumps(obj, sort_keys=True, separators=(',',':')).encode('utf-8')
    elif isinstance(obj, str):
        return obj.encode('utf-8')
    return obj


def queryiter(options):
    """
    Return appropriately encoded (key, value) pairs sorted by key.

    We JSON encode the value if the key is "key", "startkey", or "endkey", or
    if the value is not an ``str``.
    """
    for key in sorted(options):
        value = options[key]
        if key in ('key', 'startkey', 'endkey') or not isinstance(value, strtype):
            value = dumps(value)
        yield (key, value)


def query(**options):
    """
    Transform keyword arguments into the query portion of a request URL.

    For example:

    >>> query(attachments=True)
    'attachments=true'
    >>> query(limit=1000, endkey='foo+bar', group=True)
    'endkey=%22foo%2Bbar%22&group=true&limit=1000'
    >>> query(json=None)
    'json=null'

    Notice that ``True``, ``False``, and ``None`` are transformed into their
    JSON-equivalents.
    """
    return urlencode(tuple(queryiter(options)))


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


if oauth is not None:
    class Session(object):
        __slots__ = ('_consumer', '_token')

        def __init__(self, tokens):
            self._consumer = oauth.OAuthConsumer(
                tokens['consumer_key'],
                tokens['consumer_secret']
            )
            self._token = oauth.OAuthToken(
                tokens['token'],
                tokens['token_secret']
            )

        def sign(self, method, url, query):
            req = oauth.OAuthRequest.from_consumer_and_token(
                self._consumer,
                self._token,
                http_method=method,
                http_url=url,
                parameters=query,
            )
            req.sign_request(
                oauth.OAuthSignatureMethod_HMAC_SHA1(),
                self._consumer,
                self._token
            )
            return req.to_header()


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

    def __init__(self, url=SERVER, session=None):
        t = urlparse(url)
        if t.scheme not in ('http', 'https'):
            raise ValueError(
                'url scheme must be http or https: {!r}'.format(url)
            )
        if not t.netloc:
            raise ValueError('bad url: {!r}'.format(url))
        self.basepath = (t.path if t.path.endswith('/') else t.path + '/')
        self.url = ''.join([t.scheme, '://', t.netloc, self.basepath])
        self.session = session
        klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
        self.conn = klass(t.netloc)

    def _path(self, parts, options):
        url = (self.basepath + '/'.join(parts) if parts else self.basepath)
        if options:
            q = tuple(queryiter(options))
            url = '?'.join([url, urlencode(q)])
            return (url, q)
        return (url, tuple())

    def _request(self, method, parts, options, body=None, headers=None):
        h = {
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        }
        if headers:
            h.update(headers)
        (url, q) = self._path(parts, options)
        if self.session is not None:
            h.update(self.session.sign(method, url, dict(q)))
        for retry in range(2):
            try:
                self.conn.request(method, url, body, h)
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
            raise ServerError(response, data, method, url)
        if response.status >= 400:
            E = errors.get(response.status, ClientError)
            raise E(response, data, method, url)
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

    def __init__(self, url=SERVER, session=None):
        super(Server, self).__init__(url, session)

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.url)

    def database(self, name, ensure=False):
        """
        Return a new `Database` instance for the database *name*.
        """
        db = Database(name, self.url, self.session)
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
    def __init__(self, name, url=SERVER, session=None):
        super(Database, self).__init__(url, session)
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
        return Server(self.url, self.session)

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
        except PreconditionFailed:
            pass

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
