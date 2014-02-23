========================
:mod:`microfiber` module
========================

.. py:module:: microfiber
    :synopsis: fabric for a lightweight couch

In a nutshell, Microfiber is generic REST adapter that allows you to make
requests to an arbitrary JSON-centric REST API like CouchDB.  This means that by
and large the Microfiber API *is*  `CouchDB REST API`_, as expressed through the
Microfiber REST adapter.

In all our examples, we'll create throw-away CouchDB instances using
`UserCouch`_.

First, well create a ``TempCouch``, and bootstrap it get the *env* we'll pass to
a :class:`Server` or :class:`Database`.

>>> from usercouch.misc import TempCouch
>>> couch = TempCouch()
>>> env = couch.bootstrap()

A :class:`Server` expose the REST API relative to the root URL.  For example:

>>> from microfiber import Server
>>> server = Server(env)
>>> server.get()['couchdb']  # GET /
'Welcome'
>>> server.put(None, 'mydb')  # PUT /mydb
{'ok': True}
>>> doc = {'_id': 'mydoc'}
>>> doc['_rev'] = server.post(doc, 'mydb')['rev']  # POST /mydb
>>> server.get('mydb', 'mydoc') == doc  # GET /mydb/mydoc
True

A :class:`Database` expose the exact same REST API relative to the root URL.
For example:

>>> from microfiber import Database
>>> db = Database('mydb', env)
>>> db.get()['db_name']  # GET /mydb
'mydb'
>>> db.put(None)  # PUT /mydb ('mydb' already exists!)
Traceback (most recent call last):
  ...
microfiber.PreconditionFailed: 412 Precondition Failed: PUT /mydb/
>>> db.get('mydoc') == doc  # GET /mydb/mydoc
True

Chances are you'll use the :class:`Database` class most of all.


Know thy env
============

Actually, the point is that you don't have to know your *env*, you just pass it
around and let Microfiber handle the details.

As Microfiber is being developed for the `Novacut`_ project, it needs to work
equally well with a system-wide CouchDB or a per-user CouchDB started with
`UserCouch`_.  Some example *env* will help make it clear why we want to
describe the CouchDB environment with a single, extensible data structure.

For example, this is the default *env*, what would be typical for system-wide
CouchDB:

>>> env1 = 'http://localhost:5984/'
>>> db = Database('mydb', env1)
>>> db.env
{'url': 'http://localhost:5984/'}
>>> db.url
'http://localhost:5984/'


This is the same *env*, but in its normalized form:

>>> env2 = {'url': 'http://localhost:5984/'}
>>> db = Database('mydb', env2)
>>> db.env
{'url': 'http://localhost:5984/'}
>>> db.url
'http://localhost:5984/'


This is a typical *env* for `UserCouch`_:

>>> env3 = {
...     'oauth': {
...         'consumer_key': 'VXKZRHHGHYE5GIXI',
...         'consumer_secret': 'O4UIX73BIKWBDPD3',
...         'token': 'XZOT23SOO2DQJUZE',
...         'token_secret': '3CP6FFY2VXEXJZKQ'
...     },
...     'url': 'http://localhost:41289/'
... }
>>> db = Database('mydb', env3)
>>> db.env is env3
True
>>> db.url
'http://localhost:41289/'


And this is also a typical *env* for `UserCouch`_, except this time using
basic-auth instead of OAuth:

>>> env4 = {
...     'basic': {
...         'password': 'LEJT4Q7PGGE33KHX',
...         'username': 'BNLS6U5S7I32A6RQ'
...     },
...     'url': 'http://localhost:45612/'
... }
>>> db = Database('mydb', env4)
>>> db.env is env4
True
>>> db.url
'http://localhost:45612/'

(Note that if both ``env['oauth']`` and ``env['basic']`` are present, OAuth will
be used.)

Microfiber currently supports OAuth and basic HTTP auth, but support for other
types of authentication might be added in the future.  We've designed *env* so
that only 2 places need to understand the details:

    1. Microfiber - it obviously needs to understand *env* so that it can make
       correctly authenticated requests to CouchDB
       
    2. The process entry point - for example, the `Dmedia`_ DBus service knows
       it needs a per-user CouchDB, so it will get the appropriate *env* from
       `UserCouch`_

Because of this design, all the code in the middle (which is the vast majority
of the code) just needs to take the *env* and pass it to Microfiber, without
needing any special-case code for running against system-wide vs per-user
CouchDB.  Likewise, the code in the middle wont need changes should new types of
authentication be added.


SSL and HTTPS
=============

Microfiber has comprehensive SSL support, including support for client
certificates.

Whenever your *env* has a URL that starts with ``'https://'``, Microfiber will
configure an ``ssl.SSLContext`` instance for you.  The server certificates are
always verified (the *verify_mode* is set to ``CERT_REQUIRED``).

By default, the standard openssl *ca_path* will be used, and hostname
verification will be done.

Additional SSL configuration can be supplied via ``env['ssl']``.  For example,
an SSL *env* looks like this:

>>> env = {
...     'url': 'https://example.com/',
...     'ssl': {
...         'ca_file': '/trusted/server.ca',
...         'ca_path': '/trusted/ca.directory/',
...         'check_hostname': False,
...         'cert_file': '/my/client.cert',
...         'key_file': '/my/client.key',
...     }
... }

If you provide ``'ca_file'`` and/or ``'ca_path'``, only those certificates will
be trusted for verifying the server.

If you provide neither, then Microfiber will call
``SSLContext.set_default_verify_paths()`` in order to use the standard openssl
*ca_path*.  This is configured by the openssl packagers and should work across
distributions, regardless where the distribution keeps their system-wide 
certificates.

If you provide ``{'check_hostname': False}``, hostname verification will not
be done.  When you're only trusting a private CA provided via ``'ca_file'``,
it's perfectly secure to turn off hostname checking.  This allows you to use
SSL in, for example, P2P environments where the hostnames are meaningless and
change frequently.

Lastly, provide ``'cert_file'`` to specify a client certificate that Microfiber
should use to identify itself to the server.  Assuming the private key isn't in
the *cert_file*, you must also provide ``'key_file'``.


CouchBase class
===============

Although Microfiber is quite generic, it assumes you're using a JSON-loving
REST API similar to CouchDB (especially if it happens to be CouchDB).  To
simplify things, Microfiber makes 2 key assumptions:

    1. Request bodies are empty or JSON, except when you PUT an attachment

    2. Response bodies are JSON, except when you GET an attachment

:class:`CouchBase` is the base class for the :class:`Server` and
:class:`Database` classes.  You typically wont use the :class:`CouchBase` class
directly, but it provides the seven methods that make up the generic REST
adapter:

    * :meth:`CouchBase.put()`
    * :meth:`CouchBase.post()`
    * :meth:`CouchBase.get()`
    * :meth:`CouchBase.head()`
    * :meth:`CouchBase.delete()`
    * :meth:`CouchBase.put_att()`
    * :meth:`CouchBase.get_att()`

All these methods are inherited unchanged by the :class:`Server` and
:class:`Database` classes.

All the method examples below assume this setup:

>>> from usercouch.misc import TempCouch
>>> from microfiber import CouchBase, dumps
>>> couch = TempCouch()
>>> env = couch.bootstrap()


.. class:: CouchBase(env='http://localhost:5984/')


    .. method:: put(obj, *parts, **options)
    
        PUT *obj*.

        For example, to create the database "db1":

        >>> cb = CouchBase(env)
        >>> cb.put(None, 'db1')
        {'ok': True}

        Or to create the doc "doc1" in the database "db1":

        >>> cb.put({'micro': 'fiber'}, 'db1', 'doc1')['rev']
        '1-fae0708c46b4a6c9c497c3a687170ad6'


    .. method:: post(obj, *parts, **options)

        POST *obj*.

        For example, to create the doc "doc2" in the database "db2", we'll first
        create "db2":

        >>> cb = CouchBase(env)
        >>> cb.put(None, "db2")
        {'ok': True}

        And now we'll save the "doc2" document:

        >>> cb.post({'_id': 'doc2'}, 'db2')['rev']
        '1-967a00dff5e02add41819138abb3284d'

        Or to compact the database "db2":

        >>> cb.post(None, 'db2', '_compact')
        {'ok': True}


    .. method:: get(*parts, **options)
    
        Make a GET request.

        For example, to get the welcome info from CouchDB:

        >>> cb = CouchBase(env)
        >>> cb.get()['couchdb']
        'Welcome'

        Or to request the doc "db1" from the database "doc1":

        >>> doc = cb.get('db1', 'doc1')
        >>> print(dumps(doc, pretty=True))
        {
            "_id": "doc1",
            "_rev": "1-fae0708c46b4a6c9c497c3a687170ad6",
            "micro": "fiber"
        }

    .. method:: head(*parts, **options)
    
        Make a HEAD request.

        Returns a ``dict`` containing the response headers from the HEAD
        request.

        For example, to make a HEAD request on the doc "doc1" in the database
        "db1":

        >>> cb = CouchBase(env)
        >>> cb.head('db1', 'doc1')['etag']
        '"1-fae0708c46b4a6c9c497c3a687170ad6"'


    .. method:: delete(*parts, **options)
    
        Make a DELETE request.

        For example, to delete the doc "doc2" in the database "db2":

        >>> cb = CouchBase(env)
        >>> cb.delete('db2', 'doc2', rev='1-967a00dff5e02add41819138abb3284d')['rev']
        '2-eec205a9d413992850a6e32678485900'

        Or two delete the "db2" database:

        >>> cb.delete('db2')
        {'ok': True}


    .. method:: put_att(content_type, data, *parts, **options)
    
        PUT an attachment.

        If uploading an attachment for a document that already exist, you don't
        need to specify the *rev*.  For example, to upload the attachment "att1"
        for the doc "doc1" in the database "db1".

        >>> cb = CouchBase(env)
        >>> cb.put_att('text/plain', b'hello, world', 'db1', 'doc1', 'att1',
        ...     rev='1-fae0708c46b4a6c9c497c3a687170ad6',
        ... )['rev']
        '2-bd4ac0c5ca963e5b4f0f3b09ea540de2'

        On the other hand, if uploading an attachment for a doc that doesn't
        exist yet, you don't need to specify the *rev*.  For example, to upload
        the attachment "newatt" for the doc "newdoc" in "db":

        >>> cb.put_att('text/plain', b'New', 'db1', 'newdoc', 'newatt')['rev']
        '1-b2c33fbf19cadc92ab7b9860e116bb25'

        Note that you don't need any attachment-specific method for DELETE. 
        Just use :meth:`CouchBase.delete()`, like this:

        >>> cb.delete('db1', 'newdoc', 'newatt', rev='1-b2c33fbf19cadc92ab7b9860e116bb25')['rev']
        '2-5a5ecda09b7010bc3f190d8766398cff'


    .. method:: get_att(*parts, **options)
    
        GET an attachment.

        Returns a ``(content_type, data)`` tuple.  For example, to download the
        attachment "att1" for the doc "doc1" in the database "db1":

        >>> cb = CouchBase(env)
        >>> cb.get_att('db1', 'doc1', 'att1')
        Attachment(content_type='text/plain', data=b'hello, world')



Server class
============

In addition to the seven REST adapter methods inherited from :class:`CouchBase`,
the :class:`Server` class provides one convenience method:

    * :meth:`Server.database()`

.. class:: Server(env='http://localhost:5984/')

    Makes requests relative to a CouchDB server URL.
    
    Create a :class:`Server` like this:
    
    >>> from microfiber import Server
    >>> s = Server({'url': 'http://localhost:41289/'})
    >>> s.env
    {'url': 'http://localhost:41289/'}
    >>> s.url
    'http://localhost:41289/'
    >>> s.basepath
    '/'

    .. method:: database(name, ensure=False)
    
        Return a :class:`Database` instance for the database *name*.
        
        This will create :class:`Database` instance, passing it the same *env*
        that this :class:`Server` was created with.  For example:
        
        >>> s = Server('http://localhost:41289/')
        >>> s.database('foo')
        Database('foo', 'http://localhost:41289/')
        
        If you call this method with ``ensure=True``, a call to
        :meth:`Database.ensure()` is made prior to returning the instance.



Database class
==============

In addition to the seven REST adapter methods inherited from :class:`CouchBase`,
the :class:`Database` class provides these convenience methods:

    * :meth:`Database.server()`
    * :meth:`Database.ensure()`
    * :meth:`Database.save()`
    * :meth:`Database.save_many()`
    * :meth:`Database.get_many()`
    * :meth:`Database.view()`
    * :meth:`Database.bulksave()`


.. class:: Database(name, env='http://localhost:5984/')

    Makes requests relative to a CouchDB database URL.
    
    Create a :class:`Database` like this:
    
    >>> from microfiber import Database
    >>> db = Database('foo', {'url': 'http://localhost:41289/'})
    >>> db.name
    'foo'
    >>> db.env
    {'url': 'http://localhost:41289/'}
    >>> db.url
    'http://localhost:41289/'
    >>> db.basepath
    '/foo/'


    .. method:: server()
    
        Return a :class:`Server` instance with the same *env* as this database.
        
        For example:
        
        >>> db = Database('foo', 'http://localhost:41289/')
        >>> db.server()
        Server('http://localhost:41289/')
        
        
    .. method:: ensure()
    
        Ensure the database exists.

        This method will attempt to create the database, and will handle the
        :exc:`PreconditionFailed` exception raised if the database already
        exists.

        Higher level code can safely call this method at any time, and it only
        results in a single PUT /db request being made.

    .. method:: save(doc)
    
        POST *doc* to CouchDB and update ``doc['_rev']`` in-place.

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

        If *doc* has no ``'_id'``, one is generated using :func:`random_id()`
        and added to *doc* in-place prior to making the request to CouchDB.

        This method is inspired by the identical (and highly useful) method in
        `python-couchdb`_.

    .. method:: save_many(docs)

        Bulk-save using non-atomic semantics, updating all ``_rev`` in-place.

        This method is similar :meth:`Database.save()`, except this method
        operates on a list of many docs at once.

        If there are conflicts, a :exc:`BulkConflict` exception is raised, whose
        ``conflicts`` attribute will be a list of the documents for which there
        were conflicts.  Your request will *not* have modified these conflicting
        documents in the database.

        However, all non-conflicting documents will have been saved and their
        ``_rev`` updated in-place.
        
    .. method:: get_many(doc_ids)

        Convenience method to retrieve multiple documents at once.

        As CouchDB has a rather large per-request overhead, retrieving multiple
        documents at once can greatly improve performance.

    .. method:: view(design, view, **options)
    
        Shortcut for making a GET request to a view.

        No magic here, just saves you having to type "_design" and "_view" over
        and over.  This:

            ``Database.view(design, view, **options)``

        Is just a shortcut for:

            ``Database.get('_design', design, '_view', view, **options)``
    
        For example:
    
        >>> db = Database('dmedia-0')
        >>> db.view('file', 'bytes')  #doctest: +SKIP
        {u'rows': []}
        >>> db.get('_design', 'file', '_view', 'bytes')  #doctest: +SKIP
        {u'rows': []}

    .. method:: bulksave(docs)

        Bulk-save using all-or-nothing semantics, updating all ``_rev`` in-place.

        This method is similar :meth:`Database.save()`, except this method
        operates on a list of many docs at once.

        *Note:* for subtle reasons that take a while to explain, you probably
        don't want to use this method.  Instead use
        :meth:`Database.save_many()`.
        
    .. method:: dump(filename)

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



Functions
=========


.. function:: random_id()

    Returns a 120-bit base32-encoded random ID.

    The ID will be 24-characters long, URL and filesystem safe.  For example:

    >>> random_id()  #doctest: +SKIP
    'OVRHK3TUOUQCWIDMNFXGC4TP'

    This is how Dmedia/Novacut random IDs are created, so this is "Jason
    approved", for what that's worth.


.. function:: random_id2()

    Returns a random ID with timestamp + 80 bits of base32-encoded random data.

    The ID will be 27-characters long, URL and filesystem safe.  For example:

    >>> random_id2()  #doctest: +SKIP
    '1313567384-67DFPERIOU66CT56'

    The idea with this 2nd type of random ID is that it will be used for the
    Dmedia activity log.


.. function:: dumps(obj, pretty=False)

    Safe and opinionated use of ``json.dumps()``.

    This function always calls ``json.dumps()`` with *ensure_ascii=False* and
    *sort_keys=True*.

    For example:

    >>> from microfiber import dumps
    >>> doc = {
    ...     'hello': 'мир',
    ...     'welcome': 'все',
    ... }
    >>> dumps(doc)
    '{"hello":"мир","welcome":"все"}'

    Whereas if you directly call ``json.dumps()`` without *ensure_ascii=False*:

    >>> import json
    >>> json.dumps(doc, sort_keys=True)
    '{"hello": "\\u043c\\u0438\\u0440", "welcome": "\\u0432\\u0441\\u0435"}'

    By default compact encoding is used, but if you supply *pretty=True*,
    4-space indentation will be used:

    >>> print(dumps(doc, pretty=True))
    {
        "hello": "мир",
        "welcome": "все"
    }



.. function:: dmedia_env()

    Return the Dmedia environment information.

    For example, to create a :class:`Database` with the correct per-user
    `Dmedia`_ environment:

    >>> from microfiber import dmedia_env, Database
    >>> db = Database('dmedia-1', dmedia_env())  #doctest: +SKIP 

    If you're using Microfiber to work with `Dmedia`_ or `Novacut`_, please use
    this function instead of :func:`dc3_env()` as starting with the Dmedia 12.01
    release, Dmedia itself will be what starts CouchDB. 


Exceptions
==========

.. exception:: HTTPError

    Base class for all custom microfiber exceptions.



.. exception:: ClientError

    Base class for all 4xx Client Error exceptions.



.. exception:: BadRequest

    400 Bad Request.



.. exception:: Unauthorized

    401 Unauthorized.



.. exception:: Forbidden

    403 Forbidden.



.. exception:: NotFound

    404 Not Found.



.. exception:: MethodNotAllowed

    405 Method Not Allowed.



.. exception:: NotAcceptable

    406 Not Acceptable.



.. exception:: Conflict

    409 Conflict.

    This is raised when the request resulted in an update conflict.



.. exception:: PreconditionFailed

    412 Precondition Failed.



.. exception:: BadContentType

    415 Unsupported Media Type.



.. exception:: BadRangeRequest

    416 Requested Range Not Satisfiable.



.. exception:: ExpectationFailed

    417 Expectation Failed.

    This is raised when a bulk operation failed.



.. exception:: ServerError

    Used to raise exceptions for any 5xx Server Errors.
    

.. exception:: BulkConflict(conflicts, rows)

    Raised by :meth:`Database.save_many()` when one or more docs have conflicts.

    .. attribute:: conflicts

        A list of docs for which conflicts occurred.  The docs will be
        unmodified, will have the exact value and ``_rev`` as they did prior to
        calling :meth:`Database.save_many()`.

    .. attribute:: rows

        The exact return value from the CouchDB request.


.. _`Novacut`: https://wiki.ubuntu.com/Novacut
.. _`UserCouch`: https://launchpad.net/usercouch
.. _`CouchDB REST API`: http://couchdb.readthedocs.org/en/latest/api/index.html
.. _`dc3`: https://launchpad.net/dc3
.. _`Dmedia`: https://launchpad.net/dmedia
.. _`python-couchdb`: http://packages.python.org/CouchDB/client.html#database
