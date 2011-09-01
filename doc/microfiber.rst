=====================
The microfiber module
=====================


The CouchBase class
===================

Although Microfiber is quite generic, it assumes you're using a JSON-loving
REST API similar to CouchDB (especially if it happens to be CouchDB).  To
simplify things, Microfiber makes 2 key assumptions:

    1. Request bodies are empty or JSON, except when you PUT an attachment

    2. Response bodies are JSON, except when you GET an attachment

:class:`CouchBase` is the base class for the :class:`Server` and
:class:`Database` classes.  You typically wont use the :class:`CouchBase` class
directly, but it provides the seven methods that make up the generic REST
adapter:

    * :meth:`CouchBase.post()`
    * :meth:`CouchBase.put()`
    * :meth:`CouchBase.get()`
    * :meth:`CouchBase.delete()`
    * :meth:`CouchBase.head()`
    * :meth:`CouchBase.put_att()`
    * :meth:`CouchBase.get_att()`
    
All these methods are inherited unchanged by the :class:`Server` and
:class:`Database` classes.

.. class:: CouchBase(env)


    .. method:: post(obj, *parts, **options)
    
        POST *obj*.

        For example, to create the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.post({'_id': 'bar'}, 'foo')  #doctest: +SKIP
        {'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}

        Or to compact the database "foo":

        >>> cb.post(None, 'foo', '_compact')  #doctest: +SKIP
        {'ok': True}


    .. method:: put(obj, *parts, **options)
    
        PUT *obj*.

        For example, to create the database "foo":

        >>> cb = CouchBase()
        >>> cb.put(None, 'foo')  #doctest: +SKIP
        {'ok': True}

        Or to create the doc "baz" in the database "foo":

        >>> cb.put({'micro': 'fiber'}, 'foo', 'baz')  #doctest: +SKIP
        {'rev': '1-fae0708c46b4a6c9c497c3a687170ad6', 'ok': True, 'id': 'bar'}
    
    
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
    
    
    .. method:: delete(*parts, **options)
    
        Make a DELETE request.

        For example, to delete the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.delete('foo', 'bar', rev='1-967a00dff5e02add41819138abb3284d')  #doctest: +SKIP
        {'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'bar'}

        Or to delete the database "foo":

        >>> cb.delete('foo')  #doctest: +SKIP
        {'ok': True}


    .. method:: head(*parts, **options)
    
        Make a HEAD request.

        Returns a ``dict`` containing the response headers from the HEAD
        request.
        
        For example, to make a HEAD request on the doc "bar" in the database
        "foo":
        
        >>> cb = CouchBase()
        >>> cb.head('foo', 'baz')['Etag']  #doctest: +SKIP
        '"1-967a00dff5e02add41819138abb3284d"'

    
    .. method:: put_att(content_type, data, *parts, **options)
    
        PUT an attachment.

        For example, to upload the attachment "baz" for the doc "bar" in the
        database "foo":

        >>> cb = CouchBase()
        >>> cb.put_att('image/png', b'da pic', 'foo', 'bar', 'baz')  #doctest: +SKIP
        {'rev': '1-d536771b631a30c2ab4c0340adc72570', 'ok': True, 'id': 'bar'}

        Note that you don't need any attachment-specific method for DELETE. 
        Just use CouchBase.delete(), like this:
        
        >>> cb.delete('foo', 'bar', 'baz', rev='1-d536771b631a30c2ab4c0340adc72570')  #doctest: +SKIP
        {'rev': '2-082e66867f6d4d1753d7d0bf08122425', 'ok': True, 'id': 'bar'}

        
    .. method:: get_att(*parts, **options)
    
        GET an attachment.

        Returns a ``(content_type, data)`` tuple.  For example, to download the
        attachment "baz" for the doc "bar" in the database "foo":

        >>> cb = CouchBase()
        >>> cb.get_att('foo', 'bar', 'baz')  #doctest: +SKIP
        ('image/png', b'da pic')



The Server class
================

.. class:: Server(env)

    .. method:: database(name, ensure=False)


The Database class
==================


.. class:: Database(name, env)

    .. method:: server()
    
    .. method:: ensure()
    
    .. method:: save(doc)
    
    .. method:: bulksave(docs)
    
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



Random ID functions
===================


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








