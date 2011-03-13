"""
`microfiber` - fabric for a lightweight Couch.
"""

from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlencode, urlparse
import json


__version__ = '0.1.0'
USER_AGENT = 'microfiber ' + __version__


def dumps(obj):
    return json.dumps(obj, sort_keys=True, separators=(',',':'))


def jsonquery(**options):
    return urlencode(
        tuple((key, dumps(options[key])) for key in sorted(options))
    )


class HTTPError(Exception):
    """
    Base class for custom `microfiber` exceptions.
    """

    __slots__ = ('response', 'method', 'url')

    def __init__(self, response, method, url):
        self.response = response
        self.method = method
        self.url = url
        super().__init__('%r %r %r %r' %
            (response.status, response.reason, method, url)
        )


class ClientError(HTTPError):
    """
    Base class for 4xx Client Error exceptions.
    """


class BadRequest(ClientError):
    status = 400


class NotFound(ClientError):
    status = 404


class Conflict(ClientError):
    status = 409


class PreconditionFailed(ClientError):
    status = 412


errors = dict(
    (E.status, E) for E in [BadRequest, NotFound, Conflict, PreconditionFailed]
)


class ServerError(HTTPError):
    """
    Used to raise exceptions for all 5xx Server Errors.
    """


class Response(object):
    __slots__ = ('response', 'data')

    def __init__(self, response, data):
        self.response = response
        self.data = data

    def loads(self):
        return json.loads(self.data.decode('utf-8'))


class CouchCore(object):
    def __init__(self, url, **connargs):
        self.url = url
        t = urlparse(self.url)
        self.base = t.path
        assert self.base.endswith('/')
        assert t.scheme in ('http', 'https')
        klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
        self.conn = klass(t.netloc, **connargs)
        self.conn.set_debuglevel(10)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.url)

    def request_url(self, *parts, **options):
        url = self.base + '/'.join(parts)
        if options:
            return '?'.join([url, jsonquery(**options)])
        return url

    def request(self, method, obj, *parts, **options):
        url = self.request_url(*parts, **options)
        headers = {
            'Accept': 'application/json',
            'User-Agent': USER_AGENT,
        }
        if obj is None:
            body = None
        else:
            headers['Content-Type'] = 'application/json'
            body = dumps(obj)
        self.conn.request(method, url, body, headers)
        r = self.conn.getresponse()
        data = r.read()
        if r.status >= 500:
            raise ServerError(r, method, url)
        if r.status >= 400:
            E = errors.get(r.status, ClientError)
            raise E(r, method, url)
        return Response(r, data)

    def post(self, obj, *parts, **options):
        return self.request('POST', obj, *parts, **options)

    def put(self, obj, *parts, **options):
        return self.request('PUT', obj, *parts, **options)

    def get(self, *parts, **options):
        return self.request('GET', None, *parts, **options)

    def head(self, *parts, **options):
        return self.request('HEAD', None, *parts, **options)

    def delete(self, *parts, **options):
        return self.request('DELETE', None, *parts, **options)


class Server(CouchCore):
    def __init__(self, url='http://localhost:5984/', **connargs):
        super().__init__(url, **connargs)


s = Server()
try:
    print(s.put(None, 'dmedia').loads())
except PreconditionFailed:
    pass
print(s.get('dmedia').loads())
