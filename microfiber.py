"""
`microfiber` - fabric for a lightweight Couch.
"""

from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse, urlencode
import json


__version__ = '0.1.0'
USER_AGENT = 'microfiber ' + __version__


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

    __slots__ = ('response', 'method', 'url', 'data')

    def __init__(self, response, method, url):
        self.response = response
        self.method = method
        self.url = url
        self.data = response.read()
        super().__init__('%s %s: %s %s' %
            (response.status, response.reason, method, url)
        )


class ClientError(HTTPError):
    """
    Base class for 4xx Client Error exceptions.
    """


class BadRequest(ClientError):
    status = 400


class Unauthorized(ClientError):
    status = 401


class Forbidden(ClientError):
    status = 403


class NotFound(ClientError):
    status = 404


class MethodNotAllowed(ClientError):
    status = 405


class Conflict(ClientError):
    status = 409


class PreconditionFailed(ClientError):
    status = 412


class UnsupportedMediaType(ClientError):
    status = 415


class ServerError(HTTPError):
    """
    Used to raise exceptions for all 5xx Server Errors.
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

    def __init__(self, url):
        self.url = (url if url.endswith('/') else url + '/')
        t = urlparse(self.url)
        self.basepath = t.path
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
    def __init__(self, url='http://localhost:5984/'):
        super().__init__(url)

    def __iter__(self):
        for name in sorted(self.get('_all_dbs')):
            yield name

    def database(self, name):
        try:
            self.put(None, name)
        except PreconditionFailed:
            pass
        return Database(self.url + name)


class Database(CouchCore):
    def __init__(self, url='http://localhost:5984/_users/'):
        super().__init__(url)

    def __iter__(self):
        for row in self.get('_all_docs')['rows']:
            yield row['id']

    def __getitem__(self, _id):
        return self.get(_id, attachments=True)

    def compact(self):
        return self.post(None, '_compact')

    def save(self, doc):
        ret = self.post(doc)
        doc['_id'] = ret['id']
        doc['_rev'] = ret['rev']
        return ret

    def bulksave(self, docs):
        ret = self.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')
        for (r, d) in zip(ret, docs):
            d['_id'] = r['id']
            d['_rev'] = r['rev']
        return ret
