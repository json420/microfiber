Microfiber = Python3 + CouchDB
==============================

`Microfiber`_ is an abstract adapter for making HTTP requests to an arbitrary
JSON loving REST API like `CouchDB`_.  Rather than wrapping the API in a bunch
of one-off methods, Microfiber just makes it super easy to call any part of the
`CouchDB REST API`_, current or future.  This approach allows Microfiber to be
very simple and basically maintenance free as it requires no changes to support
new additions to the CouchDB API.

In a nutshell, the Microfiber API is the CouchDB API, nothing more.  For
example:

>>> from microfiber import Database
>>> db = Database('foo', env)
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


.. _`Microfiber`: https://launchpad.net/microfiber
.. _`CouchDB`: http://couchdb.apache.org/
.. _`CouchDB REST API`: http://www.couchbase.org/sites/default/files/uploads/all/documentation/couchbase-api.html


Contents:

.. toctree::
   :maxdepth: 2
   
   couchdb_api
   microfiber

