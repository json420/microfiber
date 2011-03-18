"""
`microfiber` - fabric for a lightweight Couch.
"""

from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlencode, urlparse
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


class HTTPError(Exception):
    """
    Base class for custom `microfiber` exceptions.
    """

    __slots__ = ('response', 'method', 'url', 'data')

    def __init__(self, response, method, url):
        self.response = response
        self.method = method
        self.url = url
        self.data = response.read()
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
    def __init__(self, url, **connargs):
        if url.endswith('/'):
            self.url = url
        else:
            self.url = url + '/'
        self.connargs = connargs
        t = urlparse(self.url)
        self.basepath = t.path
        klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
        self.conn = klass(t.netloc, **connargs)
        #self.conn.set_debuglevel(1)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.url)

    def path(self, *parts, **options):
        url = self.basepath + '/'.join(parts)
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
        r = self.conn.getresponse()
        if r.status >= 500:
            raise ServerError(r, method, url)
        if r.status >= 400:
            E = errors.get(r.status, ClientError)
            raise E(r, method, url)
        return Response(r)

    def _json(self, method, obj, *parts, **options):
        """
        Make a PUT or POST request with a JSON body.
        """
        url = self.path(*parts, **options)
        body = (None if obj is None else dumps(obj))
        headers = {'Content-Type': 'application/json'}
        return self.request(method, url, body, headers)

    def post(self, obj, *parts, **options):
        return self._json('POST', obj, *parts, **options).loads()

    def put(self, obj, *parts, **options):
        return self._json('PUT', obj, *parts, **options).loads()

    def get(self, *parts, **options):
        return self.request('GET', self.path(*parts, **options)).loads()

    def delete(self, *parts, **options):
        return self.request('DELETE', self.path(*parts, **options)).loads()

    def head(self, *parts, **options):
        return self.request('HEAD', self.path(*parts, **options)).head()


class Server(CouchCore):
    def __init__(self, url='http://localhost:5984/', **connargs):
        super().__init__(url, **connargs)

    def __iter__(self):
        for name in sorted(self.get('_all_dbs')):
            yield name

    def db(self, name):
        try:
            self.put(None, name)
        except PreconditionFailed:
            pass
        return Database(self.url + name, **self.connargs)


class Database(CouchCore):
    def __init__(self, url='http://localhost:5984/_users/', **connargs):
        super().__init__(url, **connargs)

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


s = Server()
print(list(s))

try:
    print(s.delete('dmedia_test'))
except NotFound:
    pass
db = s.db('dmedia_test')
docs = [{'foo': 'bar'} for i in range(100)]
db.bulksave(docs)
for d in docs:
    d['stuff'] = 17
db.bulksave(docs)
print(db.compact())

print(db.head(docs[0]['_id']))

print('')
for _id in db:
    print(db[_id])
