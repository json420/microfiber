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

The microfiber API is the CouchDB REST API, and nothing more.  For example:

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

"""

from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse, urlencode
import json

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

__version__ = '0.0.2'
USER_AGENT = 'microfiber ' + __version__
SERVER = 'http://localhost:5984/'
DATABASE = SERVER + '_users/'
errors = {}


def dumps(obj):
    """
    JSON encode *obj*.

    Returns a ``bytes`` instance with a compact JSON encoding of *obj*.

    :param obj: a JSON serialize-able object, likely a ``dict`` or ``list``
    """
    return json.dumps(obj, sort_keys=True, separators=(',',':')).encode('utf-8')


def loads(data):
    """
    Decode object from JSON bytes *data*.

    :param data: a ``bytes`` instance containing a UTF-8 encoded, JSON
        serialized object
    """
    return json.loads(data.decode('utf-8'))


def queryiter(**options):
    for key in sorted(options):
        value = options[key]
        if isinstance(value, bool) or value is None:
            value = json.dumps(value)
        yield (key, value)


def query(**options):
    """
    Transform keyword arguments into the query portion of a request URL.

    For example:

    >>> query(attachments=True)
    'attachments=true'
    >>> query(limit=1000, endkey='foo+bar', group=True)
    'endkey=foo%2Bbar&group=true&limit=1000'
    >>> query(json=None)
    'json=null'

    Notice that ``True``, ``False``, and ``None`` are transformed into their
    JSON-equivalents.
    """
    return urlencode(tuple(queryiter(**options)))


class HTTPErrorMeta(type):
    """
    Metaclass to build mapping of status code to `HTTPError` subclasses.

    If the class has a ``status`` attribute, it will be added to the `errors`
    dictionary.
    """
    def __new__(meta, name, bases, dict):
        cls = type.__new__(meta, name, bases, dict)
        if isinstance(getattr(cls, 'status', None), int):
            errors[cls.status] = cls
        return cls


class HTTPError(Exception, metaclass=HTTPErrorMeta):
    """
    Base class for custom `microfiber` exceptions.
    """

    def __init__(self, response, method, url):
        self.response = response
        self.method = method
        self.url = url
        self.data = response.read()
        super().__init__()

    def __str__(self):
        return '{} {}: {} {}'.format(
            self.response.status, self.response.reason, self.method, self.url
        )

    def loads(self):
        return loads(self.data)


class ClientError(HTTPError):
    """
    Base class for all 4xx Client Error exceptions.
    """


class BadRequest(ClientError):
    """
    400 Bad Request.
    """
    status = 400


class Unauthorized(ClientError):
    """
    401 Unauthorized.
    """
    status = 401


class Forbidden(ClientError):
    """
    403 Forbidden.
    """
    status = 403


class NotFound(ClientError):
    """
    404 Not Found.
    """
    status = 404


class MethodNotAllowed(ClientError):
    """
    405 Method Not Allowed.
    """
    status = 405


class NotAcceptable(ClientError):
    """
    406 Not Acceptable.
    """
    status = 406


class Conflict(ClientError):
    """
    409 Conflict.

    Raised when the request resulted in an update conflict.
    """
    status = 409


class PreconditionFailed(ClientError):
    """
    412 Precondition Failed.
    """
    status = 412


class BadContentType(ClientError):
    """
    415 Unsupported Media Type.
    """
    status = 415


class BadRangeRequest(ClientError):
    """
    416 Requested Range Not Satisfiable.
    """
    status = 416


class ExpectationFailed(ClientError):
    """
    417 Expectation Failed.

    Raised when a bulk operation failed.
    """
    status = 417


class ServerError(HTTPError):
    """
    Used to raise exceptions for any 5xx Server Errors.
    """


class CouchBase(object):
    """
    Base class for `Server` and `Database`.

    This class is a simple a adapter to make it easy to call a JSON loving REST
    API similar to CouchDB (especially if it happens to be CouchDB).  To
    simplify things, there are some assumptions we can make:

        * Request bodies are empty or JSON, except when you PUT an attachment

        * Response bodies are JSON, except when you GET an attachment or make a
          HEAD request

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
    """

    def __init__(self, url=SERVER):
        t = urlparse(url)
        if t.scheme not in ('http', 'https'):
            raise ValueError(
                'url scheme must be http or https: {!r}'.format(url)
            )
        if not t.netloc:
            raise ValueError('bad url: {!r}'.format(url))
        self.basepath = (t.path if t.path.endswith('/') else t.path + '/')
        self.url = ''.join([t.scheme, '://', t.netloc, self.basepath])
        klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
        self.conn = klass(t.netloc)

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.url)

    def path(self, *parts, **options):
        """
        Construct URL from base path.

        For example:

        >>> cc = CouchBase('http://localhost:5001/dmedia/')
        >>> cc.path()
        '/dmedia/'
        >>> cc.path('_design', 'file', '_view', 'bytes')
        '/dmedia/_design/file/_view/bytes'
        >>> cc.path('mydoc', rev='1-3e812567', attachments=True)
        '/dmedia/mydoc?attachments=true&rev=1-3e812567'

        :param parts: path components to construct URL relative to base path
        :param options: optional keyword arguments to include in query
        """
        url = (self.basepath + '/'.join(parts) if parts else self.basepath)
        if options:
            return '?'.join([url, query(**options)])
        return url

    def request(self, method, url, body=None, headers=None):
        h = {
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        }
        if headers:
            h.update(headers)
        self.conn.close()
        self.conn.request(method, url, body, h)
        response = self.conn.getresponse()
        if response.status >= 500:
            raise ServerError(response, method, url)
        if response.status >= 400:
            E = errors.get(response.status, ClientError)
            raise E(response, method, url)
        return response

    def json(self, method, obj, *parts, **options):
        """
        Make a PUT or POST request with a JSON body.
        """
        url = self.path(*parts, **options)
        body = (None if obj is None else dumps(obj))
        headers = {'Content-Type': 'application/json'}
        return self.request(method, url, body, headers)

    def post(self, obj, *parts, **options):
        response = self.json('POST', obj, *parts, **options)
        return loads(response.read())

    def put(self, obj, *parts, **options):
        response = self.json('PUT', obj, *parts, **options)
        return loads(response.read())

    def get(self, *parts, **options):
        response = self.request('GET', self.path(*parts, **options))
        return loads(response.read())

    def delete(self, *parts, **options):
        response = self.request('DELETE', self.path(*parts, **options))
        return loads(response.read())

    def head(self, *parts, **options):
        response = self.request('HEAD', self.path(*parts, **options))
        response.read()
        return dict(response.getheaders())

    def put_att(self, mime, date, *parts, **options):
        """
        PUT an attachment.

        Note that you don't need any attachment-specific method for DELETE -
        just use `CouchBase.delete()`.

        :param mime: The Content-Type, eg ``'image/jpeg'``
        :param data: a ``bytes`` instance or an open file, passed directly to
            HTTPConnection.request()
        :param parts: path components to construct URL relative to base path
        :param options: optional keyword arguments to include in query
        """
        url = self.path(*parts, **options)
        headers = {'Content-Type': mime}
        response = self.request('PUT', url, date, headers)
        return loads(response.read())

    def get_att(self, *parts, **options):
        """
        GET an attachment.

        Returns a (mime, data) tuple with the attachment's Content-Type and
        data.

        Note that you don't need any attachment-specific method for DELETE -
        just use `CouchBase.delete()`.

        :param parts: path components to construct URL relative to base path
        :param options: optional keyword arguments to include in query
        """
        response = self.request('GET', self.path(*parts, **options))
        return (response.getheader('Content-Type'), response.read())


class Server(CouchBase):

    def database(self, name, ensure=True):
        return Database(self.url + name, ensure)


class Database(CouchBase):

    def __init__(self, url=DATABASE, ensure=False):
        super().__init__(url)
        if ensure:
            self.ensure()

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
        r = self.post(doc)
        doc.update(_id=r['id'], _rev=r['rev'])
        return r

    def bulksave(self, docs):
        rows = self.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')
        for (doc, r) in zip(docs, rows):
            doc.update(_id=r['id'], _rev=r['rev'])
        return rows
