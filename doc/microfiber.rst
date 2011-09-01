=====================
The microfiber module
=====================


The CouchBase class
===================

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
        >>> cb.head('foo', 'baz')['Etag']
        '"1-967a00dff5e02add41819138abb3284d"'

    
    .. method:: put_att(content_type, data, *parts, **options)
    
    .. method:: put_att(*parts, **options)



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








