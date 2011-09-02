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

Using a :class:`Database`:

>>> db = Database('mydb')


Or using a :class:`Server`:

>>> s = Server()



Retrieve
--------

**GET /db/doc**

Using a :class:`Database`:

>>> db = Database('mydb')


Or using a :class:`Server`:

>>> s = Server()


Delete
------

**DELETE /db/doc**

Using a :class:`Database`:

>>> db = Database('mydb')


Or using a :class:`Server`:

>>> s = Server()




Server
======


Welcome
-------


Config
------








