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


class CouchError(Exception):
    """
    Base class for custom `microfiber` exceptions.
    """

    __slots__ = ('response', 'url')

    def __init__(self, response, url):
        self.response = response
        self.url = url
        super().__init__('%r %r %r' % (response.status, response.reason, url))



class BadRequest(CouchError):
    status = 400

class Unauthorized(CouchError):
    status = 401

class NotFound(CouchError):
    status = 404



errors = dict(
    (E.status, E) for E in [BadRequest, Unauthorized, NotFound]
)


class ServerError(CouchError):
    pass


class Response(object):
    __slots__ = ('response', 'data')

    def __init__(self, response):
        self.response = response
        self.data = response.read()

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
        #self.conn.set_debuglevel(10)

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
        if r.status >= 500:
            raise ServerError(r, url)
        if r.status >= 400:
            E = errors.get(r.status, CouchError)
            raise E(r, url)
        return Response(r)

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



class Server(object):
    def __init__(self, url='http://localhost:5984/', **connargs):
        self.url = (url if url.endswith('/') else url + '/')
        t = urlparse(self.url)
        self.base = t.path
        assert self.base.endswith('/')
        klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
        self.conn = klass(t.netloc, **connargs)
        #self.conn.set_debuglevel(10)

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
        if r.status >= 500:
            raise ServerError(r, url)
        if r.status >= 400:
            E = errors.get(r.status, CouchError)
            raise E(r, url)
        return Response(r)

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



s = Server()
print
