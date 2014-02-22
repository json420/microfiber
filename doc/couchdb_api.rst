================
CouchDB REST API
================

.. py:currentmodule:: microfiber

This is a tour of some key aspects of CouchDB REST API, as used from Microfiber.
This is intended as a quick reference, and as such not all the API is documented
here.  For that, see the full `CouchDB REST API`_ documentation.

All the examples below need the following setup:

>>> from usercouch.misc import TempCouch
>>> from microfiber import Database, Server, dumps

.. _`CouchDB REST API`: http://couchdb.readthedocs.org/en/latest/api/index.html



Databases
=========

You'll generally perform database-level actions using a :class:`Database`
instance, but you can do the same using a :class:`Server` instance.


Create
------

**PUT /db**

This will create a new CouchDB database.  A :exc:`PreconditionFailed` exception
is raised if a database with the same name already exists.

Using a :class:`Database`:

>>> couch = TempCouch()
>>> db = Database('mydb', couch.bootstrap())
>>> db.put(None)
{'ok': True}

Or using a :class:`Database`, when the database already exists:

>>> db.put(None)
Traceback (most recent call last):
  ...
microfiber.PreconditionFailed: 412 Precondition Failed: PUT /mydb/

Using a :class:`Server`:

>>> couch = TempCouch()
>>> s = Server(couch.bootstrap())
>>> s.put(None, 'mydb')
{'ok': True}

Or using a :class:`Server`, when the database already exists:

>>> s.put(None, 'mydb')
Traceback (most recent call last):
  ...
microfiber.PreconditionFailed: 412 Precondition Failed: PUT /mydb/


Retrieve
--------

**GET /db**

This will return a ``dict`` with useful information about the database. A
:exc:`NotFound` exception is raised if the database does not exist.

Using a :class:`Database`, when the database does *not* exist:

>>> couch = TempCouch()
>>> db = Database('mydb', couch.bootstrap())
>>> db.get()
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: GET /mydb/

Or using a :class:`Database`, when the database exists:

>>> db.put(None)
{'ok': True}
>>> sorted(db.get())
['committed_update_seq', 'compact_running', 'data_size', 'db_name', 'disk_format_version', 'disk_size', 'doc_count', 'doc_del_count', 'instance_start_time', 'purge_seq', 'update_seq']

Using a :class:`Server`, when the database does *not* exist:

>>> couch = TempCouch()
>>> s = Server(couch.bootstrap())
>>> s.get('mydb')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: GET /mydb/

Or using a :class:`Server`, when the database exists:

>>> s.put(None, 'mydb')
{'ok': True}
>>> sorted(s.get('mydb'))
['committed_update_seq', 'compact_running', 'data_size', 'db_name', 'disk_format_version', 'disk_size', 'doc_count', 'doc_del_count', 'instance_start_time', 'purge_seq', 'update_seq']


Changes
-------

**GET /db/_changes**

Using a :class:`Database`:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}
>>> doc = {'_id': 'mydoc'}
>>> doc['_rev'] = db.post(doc)['rev']
>>> changes = db.get('_changes')
>>> print(dumps(changes, pretty=True))
{
    "last_seq": 1,
    "results": [
        {
            "changes": [
                {
                    "rev": "1-967a00dff5e02add41819138abb3284d"
                }
            ],
            "id": "mydoc",
            "seq": 1
        }
    ]
}

Or using a :class:`Server`:

>>> s = Server(env)
>>> changes = s.get('mydb', '_changes')
>>> print(dumps(changes, pretty=True))
{
    "last_seq": 1,
    "results": [
        {
            "changes": [
                {
                    "rev": "1-967a00dff5e02add41819138abb3284d"
                }
            ],
            "id": "mydoc",
            "seq": 1
        }
    ]
}


``POST /db/_compact``
---------------------

This will trigger database compaction.  Note this has no effect if compaction
is already running (in other words, only a single compaction task will ever be
running per database).  As setup for our examples, we'll do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> server.put(None, 'db1')
{'ok': True}
>>> db = Database('db2', env)
>>> db.put(None)
{'ok': True}

To compact "db1" using our :class:`Server`:

>>> server.post(None, 'db1', '_compact')
{'ok': True}

And to compact "db2" using our :class:`Database`:

>>> db.post(None, '_compact')
{'ok': True}


``DELETE /db``
--------------

This will delete the CouchDB database.  As setup for our examples, we'll do
this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> server.put(None, 'db1')
{'ok': True}
>>> db = Database('db2', env)
>>> db.put(None)
{'ok': True}

For example, to delete "db1" using our :class:`Server`:

>>> server.delete('db1')
{'ok': True}

Or to delete "db2' using our :class:`Database`:

>>> db.delete()
{'ok': True}

A :exc:`NotFound` exception is raised if the database does not exist.  For
example, if we try to delete the now non-existent "db1" using our
:class:`Server`:

>>> server.delete('db1')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: DELETE /db1

And if we try to delete the now non-existent "db2" using our :class:`Database`:

>>> db.delete()
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: DELETE /db2



Documents
=========

You'll generally perform document-level actions using a :class:`Database`
instance, but you can do the same using a :class:`Server` instance.


``PUT /db/doc``
---------------

This can be used to create a new document, and likewise to update an existing
document.

.. note::

    :meth:`Database.save()` is a better way to create and update documents as
    it will automatically update ``doc['_rev']`` in-place for you

As setup for our examples, we'll do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}

For example, we'll create "doc1" using our :class:`Server`:

>>> server.put({'foo': 'bar'}, 'mydb', 'doc1')['rev']
'1-4c6114c65e295552ab1019e2b046b10e'

And we'll create "doc2" using our :class:`Database`:

>>> db.put({'foo': 'bar'}, 'doc2')['rev']
'1-4c6114c65e295552ab1019e2b046b10e'


``POST /db``
------------

This can be used to create a new document, and likewise to update an existing
document.

.. note::

    :meth:`Database.save()` is a better way to create and update documents as
    it will automatically update ``doc['_rev']`` in-place for you

As setup for our examples, we'll do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}

For example, we can create "doc1" using our :class:`Server`:

>>> doc1 = {'_id': 'doc1'}
>>> doc1['_rev'] = server.post(doc1, 'mydb')['rev']
>>> doc1['_rev']
'1-967a00dff5e02add41819138abb3284d'

And we can create "doc2" using our :class:`Database`:

>>> doc2 = {'_id': 'doc2'}
>>> doc2['_rev'] = db.post(doc2)['rev']
>>> doc2['_rev']
'1-967a00dff5e02add41819138abb3284d'

When updated a document, ``doc['_rev']`` must be included, otherwise a
:exc:`Conflict` exception will be raised.

Note that above we updated ``doc1`` and ``doc2`` in-place with the correct
revision.  So now we can update "doc1" using our :class:`Server` like this:

>>> server.post(doc1, 'mydb')['rev']
'2-7051cbe5c8faecd085a3fa619e6e6337'

And update "doc2" using our :class:`Database` like this:

>>> db.post(doc2)['rev']
'2-7051cbe5c8faecd085a3fa619e6e6337'

A :exc:`Conflict` exception is raised if ``doc['_rev']`` doesn't match the
latest revision of the document in CouchDB (meaning the document has been
updated since you last retrieved it).

Note that in the above updates, we did not update ``doc1`` and ``doc2`` with the
correct revision:

>>> print(dumps(doc1))
{"_id":"doc1","_rev":"1-967a00dff5e02add41819138abb3284d"}
>>> print(dumps(doc2))
{"_id":"doc2","_rev":"1-967a00dff5e02add41819138abb3284d"}

So when we try to update "doc1" this time using our :class:`Server`, a
:exc:`Conflict` will be raised:

>>> server.post(doc1, 'mydb')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: POST /mydb

And likewise when we try to update "doc2" using our :class:`Database`:

>>> db.post(doc2)
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: POST /


``GET /db/doc``
---------------

This will retrieve a document from a database.  As setup for our examples, we'll
do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}

A :exc:`NotFound` exception is raised if the document does not exist.  For
example, using our :class:`Server`:

>>> server.get('mydb', 'mydoc')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: GET /mydb/mydoc

Or using our :class:`Database`:

>>> db.get('mydoc')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: GET /mydb/mydoc

On the other hand, if we create "mydoc":

>>> mydoc = {'_id': 'mydoc'}
>>> mydoc['_rev'] = db.post(mydoc)['rev']
>>> mydoc['_rev']
'1-967a00dff5e02add41819138abb3284d'

We can retrieve it using our :class:`Server`:

>>> doc = server.get('mydb', 'mydoc')
>>> print(dumps(doc, pretty=True))
{
    "_id": "mydoc",
    "_rev": "1-967a00dff5e02add41819138abb3284d"
}

Or retrieve it using our :class:`Database`:

>>> doc = db.get('mydoc')
>>> print(dumps(doc, pretty=True))
{
    "_id": "mydoc",
    "_rev": "1-967a00dff5e02add41819138abb3284d"
}

If you supply the *rev* keyword argument, you can retrieve the contents of an
older revisions of a document (assuming the database hasn't yet been compacted).

.. warning::

    You should *never* assume that older document revisions will be available!
    When a database is compacted, only the latest revision of each document
    will be preserved!

    The term "revision" is quite suggestive, but CouchDB is *not* a version
    control system.  CouchDB uses "revisions" as a concurrency control
    mechanism, and nothing more.

For example, let's create a new revision of "mydoc":

>>> mydoc['message'] = 'hello, world'
>>> db.post(mydoc)['rev']
'2-91babf69deda1e2767615ba457c80807'

We can explicitly retrieve ``'2-91babf69deda1e2767615ba457c80807'`` (which also
happens to be the latest revision):

>>> doc = db.get('mydoc', rev='2-91babf69deda1e2767615ba457c80807')
>>> print(dumps(doc, pretty=True))
{
    "_id": "mydoc",
    "_rev": "2-91babf69deda1e2767615ba457c80807",
    "message": "hello, world"
}

Or we can retrieve ``'1-967a00dff5e02add41819138abb3284d'``, the previous
revision:

>>> doc = db.get('mydoc', rev='1-967a00dff5e02add41819138abb3284d')
>>> print(dumps(doc, pretty=True))
{
    "_id": "mydoc",
    "_rev": "1-967a00dff5e02add41819138abb3284d"
}


``DELETE /db/doc``
------------------

This will delete a document from a database.  Note that a small document
tombstone will still exist so that the deletion can be replicated between nodes.

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}

For example, using a :class:`Server`:

>>> server.post({'_id': 'doc1'}, 'mydb')['rev']
'1-967a00dff5e02add41819138abb3284d'
>>> server.delete('mydb', 'doc1', rev='1-967a00dff5e02add41819138abb3284d')['rev']
'2-eec205a9d413992850a6e32678485900'

Or using a :class:`Database`:

>>> db.post({'_id': 'doc2'})['rev']
'1-967a00dff5e02add41819138abb3284d'
>>> db.delete('doc2', rev='1-967a00dff5e02add41819138abb3284d')['rev']
'2-eec205a9d413992850a6e32678485900'

A :exc:`NotFound` exception is raised if the document does not exist.  For
example, using a :class:`Server`:

>>> server.delete('mydb', 'mydoc')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: DELETE /mydb/mydoc

Or using a :class:`Database`:

>>> db.delete('mydoc')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: DELETE /mydb/mydoc

When the document exists, a :exc:`Conflict` exception is raised if you don't
supply the *rev* keyword argument.

For example, we'll create a document:

>>> mydoc = {'_id': 'mydoc'}
>>> mydoc['_rev'] = db.post(mydoc)['rev']
>>> mydoc['_rev']
'1-967a00dff5e02add41819138abb3284d'

And then try to delete it using a :class:`Server`:

>>> server.delete('mydb', 'mydoc')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc

Or try deleting it using a :class:`Database`:

>>> db.delete('mydoc')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc

Likewise, a :exc:`Conflict` exception is raised the *rev* you supply doesn't
match the latest revision of the document in CouchDB (meaning the document has
been updated since you last retrieved it).

For example, we'll modify "mydoc":

>>> mydoc['message'] = 'hello, world'
>>> db.post(mydoc)['rev']
'2-91babf69deda1e2767615ba457c80807'

And then try to delete the document using the outdated revision
``'1-967a00dff5e02add41819138abb3284d'``, first using a :class:`Server`:

>>> server.delete('mydb', 'mydoc', rev='1-967a00dff5e02add41819138abb3284d')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc?rev=1-967a00dff5e02add41819138abb3284d

And second using a :class:`Database`:

>>> db.delete('mydoc', rev='1-967a00dff5e02add41819138abb3284d')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc?rev=1-967a00dff5e02add41819138abb3284d



Attachments
===========

You'll generally perform attachment-level actions using a :class:`Database`
instance, but you can do the same using a :class:`Server` instance.


``PUT /db/doc/att``
-------------------

You create document attachments using the :meth:`CouchBase.put_att()` method.

For setup, we'll do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}

If you're creating an attachment for a document that does not yet exists, the
*rev* keyword argument isn't needed, and the document will be implicitly created
by CouchDB.  For example, using a :class:`Server`:

>>> server.put_att('text/plain', b'Foo', 'mydb', 'doc1', 'foo')['rev']
'1-383671a0277edeb17918f714d1c5b63e'

Or using using a :class:`Database`:

>>> db.put_att('text/plain', b'Foo', 'mydb', 'doc2', 'foo')['rev']
'1-183074fa494ac6e04d360e6354057360'

If the document exists, you must provide *rev* keyword argument.  For example,
to add a 2nd attachment to "doc1" using a :class:`Server`:

>>> server.put_att('text/plain', b'Bar', 'mydb', 'doc1', 'bar', rev='1-383671a0277edeb17918f714d1c5b63e')['rev']
'2-8654772d9053f1c949bffe3cf7ef4aa2'

Or to add a 2nd attachment to "doc2" using using a :class:`Database`:

>>> db.put_att('text/plain', b'Bar', 'mydb', 'doc2', 'bar', rev='1-183074fa494ac6e04d360e6354057360')['rev']
'2-d37c9c0cedace0a2a857deed922b330e'

A :exc:`Conflict` exception is raised if you don't include the *rev* keyword
argument, of if the *rev* doesn't match the latest revision of the document in
CouchDB (meaning the document has been updated since you last retrieved it).

For example, if trying to add a 3rd attachment to "doc1" using a
:class:`Server` and the outdated revision
``'1-383671a0277edeb17918f714d1c5b63e'``:

>>> server.put_att('text/plain', b'Baz', 'mydb', 'doc1', 'baz', rev='1-383671a0277edeb17918f714d1c5b63e')['rev']
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: PUT /mydb/doc1/baz?rev=1-383671a0277edeb17918f714d1c5b63e

Or if trying to add a 3rd attachment to "doc2" using a
:class:`Database` and the outdated revision
``'1-183074fa494ac6e04d360e6354057360'``:

>>> db.put_att('text/plain', b'Baz', 'mydb', 'doc2', 'baz', rev='1-183074fa494ac6e04d360e6354057360')['rev']
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: PUT /mydb/doc2/baz?rev=1-183074fa494ac6e04d360e6354057360


``GET /db/doc/att``
-------------------

You retrieve document attachments using the :meth:`CouchBase.get_att()` method.

For setup, we'll do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}
>>> db.post({'_id': 'mydoc'})['rev']
'1-967a00dff5e02add41819138abb3284d'

A :exc:`NotFound` exception is raised if the attachment does not exist.  For
example, using a :class:`Server`:

>>> server.get('mydb', 'mydoc', 'myatt')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: GET /mydb/mydoc/myatt

Or using a :class:`Database`:

>>> db.get('mydoc', 'myatt')
Traceback (most recent call last):
  ...
microfiber.NotFound: 404 Object Not Found: GET /mydb/mydoc/myatt

Finally, we'll create an attachment with this setup:

>>> db.put_att('text/plain', b'hello, world', 'mydoc', 'myatt', rev='1-967a00dff5e02add41819138abb3284d')['rev']
'2-d403ee4d0528a7be93cffb89c4beb3e4'

For example, we'll retrieve this attachment using a :class:`Server`:

>>> server.get_att('mydb', 'mydoc', 'myatt')
Attachment(content_type='text/plain', data=b'hello, world')

Or using :class:`Database`:

>>> db.get_att('mydoc', 'myatt')
Attachment(content_type='text/plain', data=b'hello, world')


``DELETE /db/doc/att``
----------------------

For setup, we'll do this:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> server = Server(env)
>>> db = Database('mydb', env)
>>> db.put(None)
{'ok': True}
>>> db.post({'_id': 'mydoc'})['rev']
'1-967a00dff5e02add41819138abb3284d'

And then add an attachment using a :class:`Server`:

>>> server.put_att('text/plain', b'hello, world', 'mydb', 'mydoc', 'att1', rev='1-967a00dff5e02add41819138abb3284d')['rev']
'2-f2d88125f27039a1af069b76c398d21e'

And then add an attachment using a :class:`Database`:

>>> db.put_att('text/plain', b'hello, naughty nurse', 'mydoc', 'att2', rev='2-f2d88125f27039a1af069b76c398d21e')['rev']
'3-00d0d01ee1cc715522f060ea49e4df22'

A :exc:`Conflict` exception is raised if the *rev* keyword argument isn't
provided, for example:

>>> server.delete('mydb', 'mydoc', 'att1')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc/att1

Or using :class:`Database`:

>>> db.delete('mydoc', 'att1')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc/att1

Likewise, a :exc:`Conflict` exception is raised if the *rev* keyword argument
doesn't match the latest revision of the document in CouchDB (meaning the 
document has been updated since you last retrieved it):

>>> server.delete('mydb', 'mydoc', 'att1', rev='2-f2d88125f27039a1af069b76c398d21e')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc/att1

Or using :class:`Database`:

>>> db.delete('mydoc', 'att1', rev='2-f2d88125f27039a1af069b76c398d21e')
Traceback (most recent call last):
  ...
microfiber.Conflict: 409 Conflict: DELETE /mydb/mydoc/att1

Finally, two examples in which the attachment is deleted:

>>> server.delete('mydb', 'mydoc', 'att1', rev='3-00d0d01ee1cc715522f060ea49e4df22')['rev']
'4-b3726f26cdcf3c7101e14ca0caf701f0'

Or using :class:`Database`:

>>> db.delete('mydoc', 'att2', rev='4-b3726f26cdcf3c7101e14ca0caf701f0')['rev']
'5-aca674de3a1607e3003e5d4e7c0337d6'



Server
======

To perform server-level actions, you must use a :class:`Server` instance.

Setup for the examples:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> s = Server(env)


``GET /``
---------

This will retrieve a ``dict`` containing the CouchDB welcome response, which
will include the CouchDB version and other useful info.

>>> sorted(s.get())
['couchdb', 'uuid', 'vendor', 'version']


``GET /_all_dbs``
-----------------

This will retrieve the list of databases in this CouchDB instance.  For example,
when no user-created databases exists:

>>> s.get('_all_dbs')
['_replicator', '_users']

And now if we create a database:

>>> s.put(None, 'foo')
{'ok': True}
>>> s.get('_all_dbs')
['_replicator', '_users', 'foo']

And finally if we create another database (note the database names are returned
in sorted order):

>>> s.put(None, 'bar')
{'ok': True}
>>> s.get('_all_dbs')
['_replicator', '_users', 'bar', 'foo']

