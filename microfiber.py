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
"""

from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse, urlencode
import json


__version__ = '0.0.1'
USER_AGENT = 'microfiber ' + __version__
SERVER = 'http://localhost:5984/'
DATABASE = SERVER + '_users/'


def dumps(obj):
    return json.dumps(obj, sort_keys=True, separators=(',',':')).encode('utf-8')


def queryiter(**options):
    for key in sorted(options):
        value = options[key]
        if isinstance(value, bool) or value is None:
            value = json.dumps(value)
        yield (key, value)


def query(**options):
    return urlencode(tuple(queryiter(**options)))


errors = {}


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
        super().__init__('%s %s: %s %s' %
            (response.status, response.reason, method, url)
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


class Response(object):
    __slots__ = ('response', '_data')

    def __init__(self, response):
        self.response = response
        self._data = None

    @property
    def data(self):
        if self._data is None:
            self._data = self.response.read()
        return self._data

    def head(self):
        self.data
        return dict(self.response.getheaders())

    def loads(self):
        return json.loads(self.data.decode('utf-8'))


class CouchCore(object):
    """
    Base class for `Server` and `Database`.
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
        return '%s(%r)' % (self.__class__.__name__, self.url)

    def path(self, *parts, **options):
        """
        Construct URL from base path.

        For example:

        >>> cc = CouchCore('http://localhost:5001/dmedia/')
        >>> cc.path()
        '/dmedia/'
        >>> cc.path('_design', 'file', '_view', 'bytes')
        '/dmedia/_design/file/_view/bytes'
        >>> cc.path('mydoc', rev='1-3e812567', attachments=True)
        '/dmedia/mydoc?attachments=true&rev=1-3e812567'

        :param parts: path components to add to base path
        :param options: keyword arguments from which to construct the query
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
        return Response(response)

    def json(self, method, obj, *parts, **options):
        """
        Make a PUT or POST request with a JSON body.
        """
        url = self.path(*parts, **options)
        body = (None if obj is None else dumps(obj))
        headers = {'Content-Type': 'application/json'}
        return self.request(method, url, body, headers)

    def post(self, obj, *parts, **options):
        return self.json('POST', obj, *parts, **options).loads()

    def put(self, obj, *parts, **options):
        return self.json('PUT', obj, *parts, **options).loads()

    def get(self, *parts, **options):
        return self.request('GET', self.path(*parts, **options)).loads()

    def delete(self, *parts, **options):
        return self.request('DELETE', self.path(*parts, **options)).loads()

    def head(self, *parts, **options):
        return self.request('HEAD', self.path(*parts, **options)).head()


class Server(CouchCore):

    def database(self, name, check=True):
        if check:
            try:
                self.put(None, name)
            except PreconditionFailed:
                pass
        return Database(self.url + name)


class Database(CouchCore):

    def __init__(self, url=DATABASE):
        super().__init__(url)

    def save(self, doc):
        r = self.post(doc)
        doc.update(_id=r['id'], _rev=r['rev'])
        return r

    def bulksave(self, docs):
        rows = self.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')
        for (r, doc) in zip(rows, docs):
            doc.update(_id=r['id'], _rev=r['rev'])
        return rows
