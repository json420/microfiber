================
CouchDB REST API
================

.. py:currentmodule:: microfiber

This is a quick tour of the CouchDB REST API, as you would use it from
Microfiber.  This is indented as a quick reference, and as such not all the API
is documented here.  For more details, see the full `CouchDB REST API`_.

All the examples assume the following setup:

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


Compact
-------

**POST /db/_compact**

Using a :class:`Database`:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> db = Database('mydb', env)
>>> db.put(None)  # Create database 'mydb'
>>> db.post(None, '_compact')
{'ok': True}

Or using a :class:`Server`:

>>> s = Server(env)
>>> s.post(None, 'mydb', '_compact')
{'ok': True}


Delete
------

**DELETE /db**

This will delete the CouchDB database.  A :exc:`NotFound` exception is raised if
the database does not exist.

Using a :class:`Database`, when the database does *not* exist:

>>> couch = TempCouch()
>>> env = couch.bootstrap()
>>> db = Database('mydb', env)
>>> db.delete()
{'ok': True}

Or using a :class:`Server`:

>>> s = Server()
>>> s.delete('mydb')
{'ok': True}



Documents
=========

You'll generally perform document-level actions using a :class:`Database`
instance, but you can do the same using a :class:`Server` instance.


Create
------

**POST /db**

This will create a new document.  A :exc:`Conflict` exception is raised if the
document already exists.

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.post({'_id': 'mydoc'})
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'mydoc'}


Or using a :class:`Server`:

>>> s = Server()
>>> s.post({'_id': 'mydoc'}, 'mydb')
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'mydoc'}



Update
------

**POST /db**

This will update an existing document.  A :exc:`Conflict` exception is raised if
``doc['_rev']`` doesn't match the latest revision of the document in CouchDB
(meaning the document has been updated since you last retrieved it).

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.post({'_id': 'mydoc', '_rev': '1-967a00dff5e02add41819138abb3284d'})
{'rev': '2-7051cbe5c8faecd085a3fa619e6e6337', 'ok': True, 'id': 'mydoc'}


Or using a :class:`Server`:

>>> s = Server()
>>> s.post({'_id': 'mydoc', '_rev': '1-967a00dff5e02add41819138abb3284d'}, 'mydb')
{'rev': '2-7051cbe5c8faecd085a3fa619e6e6337', 'ok': True, 'id': 'mydoc'}



Retrieve
--------

**GET /db/doc**

A :exc:`NotFound` exception is raised if the document does not exist.

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.get('mydoc')
{'_rev': '2-7051cbe5c8faecd085a3fa619e6e6337', '_id': 'mydoc'}


Or using a :class:`Server`:

>>> s = Server()
>>> s.get('mydb', 'mydoc')
{'_rev': '2-7051cbe5c8faecd085a3fa619e6e6337', '_id': 'mydoc'}



Delete
------

**DELETE /db/doc**

This will delete a document.  A :exc:`NotFound` exception is raised if the
document does not exist.

A :exc:`Conflict` exception is raised if the *rev* keyword argument doesn't
match the latest revision of the document in CouchDB (meaning the document has
been updated since you last retrieved it).

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.delete('mydoc', rev='2-7051cbe5c8faecd085a3fa619e6e6337')
{'rev': '3-7379b9e515b161226c6559d90c4dc49f', 'ok': True, 'id': 'mydoc'}


Or using a :class:`Server`:

>>> s = Server()
>>> s.delete('mydb', 'mydoc', rev='2-7051cbe5c8faecd085a3fa619e6e6337')
{'rev': '3-7379b9e515b161226c6559d90c4dc49f', 'ok': True, 'id': 'mydoc'}



Attachments
===========

You'll generally perform attachment-level actions using a :class:`Database`
instance, but you can do the same using a :class:`Server` instance.

PUT /db/doc/att
---------------

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
'1-03cca5d29ff7a7f15562e52789ff4e8f'

Or using using a :class:`Database`:

>>> db.put_att('text/plain', b'Foo', 'mydb', 'doc1', 'foo')['rev']
'1-03cca5d29ff7a7f15562e52789ff4e8f'

If the document exists, you must include *rev*.

A :exc:`Conflict` exception is raised if the *rev* keyword argument doesn't
match the latest revision of the document in CouchDB (meaning the document has
been updated since you last retrieved it).

Using a :class:`Database` when the document does *not* exists:

>>> db = Database('mydb')
>>> db.put_att('image/png', b'PNG Data', 'mydoc', 'myatt')
{'rev': '1-904eb7a25f6c4df64f49b0eeeb27dbbc', 'ok': True, 'id': 'mydoc'}

Or using a :class:`Database` when the document does exists:

>>> db.put_att('image/png', b'PNG Data', 'mydoc', 'myatt2',
...     rev='1-904eb7a25f6c4df64f49b0eeeb27dbbc'
... )
{'rev': '2-1e294b322cd16610bf3becb62167f7f2', 'ok': True, 'id': 'mydoc'}


Using a :class:`Server` when the document does *not* exists:

>>> s = Server()
>>> s.put_att('image/png', b'PNG Data', 'mydb', 'mydoc', 'myatt')
{'rev': '1-904eb7a25f6c4df64f49b0eeeb27dbbc', 'ok': True, 'id': 'mydoc'}

Or using a :class:`Server` when the document does exists:

>>> s.put_att('image/png', b'PNG Data', 'mydb', 'mydoc', 'myatt2',
...     rev='1-904eb7a25f6c4df64f49b0eeeb27dbbc'
... )
{'rev': '2-1e294b322cd16610bf3becb62167f7f2', 'ok': True, 'id': 'mydoc'}


GET /db/doc/att
---------------

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


DELETE /db/doc/att
------------------

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


GET /
-----

This will retrieve a ``dict`` containing the CouchDB welcome response, which
will include the CouchDB version and other useful info.

>>> sorted(s.get())
['couchdb', 'uuid', 'vendor', 'version']


GET /_all_dbs
-------------

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

