=================
microfiber module
=================

In a nutshell, the Microfiber API is the CouchDB API, nothing more.  For
example:

>>> from microfiber import Database
>>> db = Database('foo')
>>> db.put(None)  # PUT /foo
{'ok': True}
>>> db.put({}, 'bar')  # PUT /foo/bar
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}
>>> db.get('bar')  # GET /foo/bar
{'_rev': '1-967a00dff5e02add41819138abb3284d', '_id': 'bar'}
>>> db.delete('bar', rev='1-967a00dff5e02add41819138abb3284d')  # DELETE /foo/bar
{'rev': '2-eec205a9d413992850a6e32678485900', 'ok': True, 'id': 'bar'}
>>> db.delete()  # DELETE /foo
{'ok': True}

Chances are you'll use the :class:`Database` class most of all.


Know thy env
============

Actually, the point is that you don't have to know your *env*, you just pass it
around and let Microfiber handle the details.

As Microfiber is being developed for the `Novacut`_ project, it needs to work
equally well with a system-wide CouchDB or a per-user CouchDB launched by
`desktopcouch`_ or `dc3`_.  Some example *env* will help make it clear why we
want to describe the CouchDB environment with a single, extensible data
structure.

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


This is a typical *env* for `desktopcouch`_ or `dc3`_:

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


And this is also a typical *env* for `desktopcouch`_ or `dc3`_, except this time
using basic auth instead of OAuth:

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
       
    2. The process entry point - for example, the dmedia DBus service knows it
       needs a per-user CouchDB, so it will get the appropriate *env* from
       `desktopcouch`_ or `dc3`_

Because of this design, all the code in the middle (which is the vast majority
of the code) just needs to take the *env* and pass it to Microfiber, without
needing any special-case code for running against system-wide vs per-user
CouchDB.  Likewise, the code in the middle wont need changes should new types of
authentication be added.



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

.. class:: CouchBase(env='http://localhost:5984/')


    .. method:: put(obj, *parts, **options)
    
        PUT *obj*.

        For example, to create the database "foo":

        >>> cb = CouchBase()
        >>> cb.put(None, 'foo')  #doctest: +SKIP
        {'ok': True}

        Or to create the doc "baz" in the database "foo":

        >>> cb.put({'micro': 'fiber'}, 'foo', 'baz')  #doctest: +SKIP
        {'rev': '1-fae0708c46b4a6c9c497c3a687170ad6', 'ok': True, 'id': 'bar'}


    .. method:: post(obj, *parts, **options)
    
        POST *obj*.

        For example, to create the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.post({'_id': 'bar'}, 'foo')  #doctest: +SKIP
        {'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}

        Or to compact the database "foo":

        >>> cb.post(None, 'foo', '_compact')  #doctest: +SKIP
        {'ok': True}
    
    
    .. method:: get(*parts, **options)
    
        Make a GET request.

        For example, to get the welcome info from CouchDB:

        >>> cb = CouchBase()
        >>> cb.get()  #doctest: +SKIP
        {'couchdb': 'Welcome', 'version': '1.1.0'}

        Or to request the doc "bar" from the database "foo", including any
        attachments:

        >>> cb.get('foo', 'bar', attachments=True)  #doctest: +SKIP
        {'_rev': '1-967a00dff5e02add41819138abb3284d', '_id': 'bar'}


    .. method:: head(*parts, **options)
    
        Make a HEAD request.

        Returns a ``dict`` containing the response headers from the HEAD
        request.
        
        For example, to make a HEAD request on the doc "bar" in the database
        "foo":
        
        >>> cb = CouchBase()
        >>> cb.head('foo', 'baz')['Etag']  #doctest: +SKIP
        '"1-967a00dff5e02add41819138abb3284d"'


    .. method:: delete(*parts, **options)
    
        Make a DELETE request.

        For example, to delete the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.delete('foo', 'bar', rev='1-967a00dff5e02add41819138abb3284d')  #doctest: +SKIP
        {'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}

        Or to delete the database "foo":

        >>> cb.delete('foo')  #doctest: +SKIP
        {'ok': True}


    .. method:: put_att(content_type, data, *parts, **options)
    
        PUT an attachment.

        For example, to upload the attachment "baz" for the doc "bar" in the
        database "foo":

        >>> cb = CouchBase()
        >>> cb.put_att('image/png', b'da pic', 'foo', 'bar', 'baz')  #doctest: +SKIP
        {'rev': '1-d536771b631a30c2ab4c0340adc72570', 'ok': True, 'id': 'bar'}

        Note that you don't need any attachment-specific method for DELETE. 
        Just use :meth:`CouchBase.delete()`, like this:
        
        >>> cb.delete('foo', 'bar', 'baz', rev='1-d536771b631a30c2ab4c0340adc72570')  #doctest: +SKIP
        {'rev': '2-082e66867f6d4d1753d7d0bf08122425', 'ok': True, 'id': 'bar'}

        
    .. method:: get_att(*parts, **options)
    
        GET an attachment.

        Returns a ``(content_type, data)`` tuple.  For example, to download the
        attachment "baz" for the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.get_att('foo', 'bar', 'baz')  #doctest: +SKIP
        ('image/png', b'da pic')



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
the :class:`Database` class provides five convenience methods:

    * :meth:`Database.server()`
    * :meth:`Database.ensure()`
    * :meth:`Database.save()`
    * :meth:`Database.bulksave()`
    * :meth:`Database.view()`


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


    .. method:: bulksave(docs)
    
        POST a list of docs to _bulk_docs, update all _rev in place.

        For example:

        >>> db = Database('foo')
        >>> doc1 = {'_id': 'bar'}
        >>> doc2 = {'_id': 'baz'}
        >>> db.bulksave([doc1, doc2])
        [{'rev': '1-967a00dff5e02add41819138abb3284d', 'id': 'bar'}, {'rev': '1-967a00dff5e02add41819138abb3284d', 'id': 'baz'}]
        >>> doc1
        {'_rev': '1-967a00dff5e02add41819138abb3284d', '_id': 'bar'}
        >>> doc2
        {'_rev': '1-967a00dff5e02add41819138abb3284d', '_id': 'baz'}


        This method works just like :meth:`Database.save()`, except on a whole
        list of docs all at once.  As only a single request is made to CouchDB,
        this is a high-performance way to update a large number of documents.



    .. method:: view(design, view, **options)
    
        Shortcut for making a GET request to a view.

        No magic here, just saves you having to type "_design" and "_view" over
        and over.  This:

            ``Database.view(design, view, **options)``

        Is just a shortcut for:

            ``Database.get('_design', design, '_view', view, **options)``
    
        For example:
    
        >>> db = Database('dmedia')
        >>> db.view('file', 'bytes')  #doctest: +SKIP
        {u'rows': []}
        >>> db.get('_design', 'file', '_view', 'bytes')  #doctest: +SKIP
        {u'rows': []}



Functions
=========


.. function:: random_id()

    Returns a 120-bit base32-encoded random ID.

    The ID will be 24-characters long, URL and filesystem safe.  For example:

    >>> random_id()  #doctest: +SKIP
    'OVRHK3TUOUQCWIDMNFXGC4TP'

    This is how dmedia/Novacut random IDs are created, so this is "Jason
    approved", for what that's worth.


.. function:: random_id2()

    Returns a random ID with timestamp + 80 bits of base32-encoded random data.

    The ID will be 27-characters long, URL and filesystem safe.  For example:

    >>> random_id2()  #doctest: +SKIP
    '1313567384.67DFPERIOU66CT56'
    
    The idea with this 2nd type of random ID is that it will be used for the
    dmedia activity log.


.. function:: dc3_env()

    Return the dc3 environment information.

    For example, to create a :class:`Database` with the correct per-user `dc3`_
    environment:

    >>> from microfiber import dc3_env, Database
    >>> db = Database('dmedia', dc3_env())
    >>> db.url
    'http://localhost:41289/'


.. function:: dmedia_env()

    Return the Dmedia environment information.

    For example, to create a :class:`Database` with the correct per-user
    `Dmedia`_ environment:

    >>> from microfiber import dmedia_env, Database
    >>> db = Database('dmedia', dmedia_env())
    >>> db.url
    'http://localhost:41289/'

    If you are using Microfiber to work with Dmedia or Novacut, please use this
    function instead of :func:`dc3_env()` as starting with the Dmedia 12.01
    release, Dmedia itself will be what starts CouchDB. 


Exceptions
==========

.. exception:: HTTPError

    Base class for custom all microfiber exceptions.



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



.. _`Novacut`: https://wiki.ubuntu.com/Novacut
.. _`desktopcouch`: https://launchpad.net/desktopcouch
.. _`dc3`: https://launchpad.net/dc3
.. _`Dmedia`: https://launchpad.net/dmedia
.. _`python-couchdb`: http://packages.python.org/CouchDB/client.html#database







