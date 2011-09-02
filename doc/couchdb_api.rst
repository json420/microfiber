================
CouchDB REST API
================

This is a quick tour of the CouchDB REST API, as you would use it from
Microfiber.  This is indented as a quick reference, and as such not all the API
is documented here.  For more details, see the full `CouchDB REST API`_.

.. _`CouchDB REST API`: http://www.couchbase.org/sites/default/files/uploads/all/documentation/couchbase-api.html


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

>>> db = Database('mydb')
>>> db.put(None)
{'ok': True}


Or using a :class:`Server`:

>>> s = Server()
>>> s.put(None, 'mydb')
{'ok': True}


Retrieve
--------

**GET /db**

This will retrieve useful information about the database.  A :exc:`NotFound`
exception is raised if the database does not exist.

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.get()
{'update_seq': 0, 'disk_size': 79, 'purge_seq': 0, 'doc_count': 0, 'compact_running': False, 'db_name': 'mydb', 'doc_del_count': 0, 'instance_start_time': '1314934043214745', 'committed_update_seq': 0, 'disk_format_version': 5}


Or using a :class:`Server`:

>>> s = Server(env)
>>> s.get('mydb')
{'update_seq': 0, 'disk_size': 79, 'purge_seq': 0, 'doc_count': 0, 'compact_running': False, 'db_name': 'mydb', 'doc_del_count': 0, 'instance_start_time': '1314934043214745', 'committed_update_seq': 0, 'disk_format_version': 5}


Changes
-------

**GET /db/_changes**

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.get('_changes')
{'last_seq': 0, 'results': []}


Or using a :class:`Server`:

>>> s = Server()
>>> s.get('mydb', '_changes')
{'last_seq': 0, 'results': []}


Compact
-------

**POST /db/_compact**

Using a :class:`Database`:

>>> db = Database('mydb')
>>> db.post(None, '_compact')
{'ok': True}


Or using a :class:`Server`:

>>> s = Server()
>>> s.post(None, 'mydb', '_compact')
{'ok': True}


Delete
------

**DELETE /db**

This will delete the CouchDB database.  A :exc:`NotFound` exception is raised if
the database does not exist.

Using a :class:`Database`:

>>> db = Database('mydb')
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


Server
======

To perform server-level actions, you must use a :class:`Server` instance.


Welcome
-------

**GET /**

This will retrieve the CouchDB welcome response, which includes the CouchDB
version.

>>> s = Server()
>>> s.get()
{'couchdb': 'Welcome', 'version': '1.1.0'}


Databases
---------

**GET /_all_dbs**

This will retrieve the list of databases in this CouchDB instance.

>>> s.get('_all_dbs')
['_replicator', '_users', 'dmedia', 'mydb']







