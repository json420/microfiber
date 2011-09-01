================
CouchDB REST API
================

This is a quick tour of the CouchDB REST API, as you would use it from
Microfiber.  This is indented as a quick reference, and as such not all the API
is documented here.  For more details, see the full `CouchDB REST API`_.

.. _`CouchDB REST API`: http://www.couchbase.org/sites/default/files/uploads/all/documentation/couchbase-api.html


Databases
=========

All the following database method examples assume you started out with this:

>>> from microfiber import Database, Server
>>> database = Database('db1')
>>> server = Server()


Create
------

>>> database.put(None)
{'ok': True}
>>> server.put(None, 'db2')
{'ok': True}


Info
----

>>> database.get()
{'update_seq': 0, 'disk_size': 79, 'purge_seq': 0, 'doc_count': 0, 'compact_running': False, 'db_name': 'db1', 'doc_del_count': 0, 'instance_start_time': '1314870632421649', 'committed_update_seq': 0, 'disk_format_version': 5}
>>> server.get('db2')
{'update_seq': 0, 'disk_size': 79, 'purge_seq': 0, 'doc_count': 0, 'compact_running': False, 'db_name': 'db2', 'doc_del_count': 0, 'instance_start_time': '1314870646021676', 'committed_update_seq': 0, 'disk_format_version': 5}


Changes
-------

>>> database.get('_changes')
{'last_seq': 0, 'results': []}
>>> server.get('db2', '_changes')
{'last_seq': 0, 'results': []}


Compact
-------

>>> database.post(None, '_compact')
{'ok': True}
>>> server.post(None, 'db2', '_compact')
{'ok': True}


Delete
------

>>> database.delete()
{'ok': True}
>>> server.delete('db2')
{'ok': True}



Documents
=========

All the following document method examples assume you started out with this:

>>> from microfiber import Database, Server
>>> database = Database('db1')
>>> server = Server()  # 'db2'


Create
------

>>> database.post({'_id': 'mydoc'})
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'mydoc'}
>>> server.post({'_id': 'mydoc'}, 'db2')
{'rev': '1-967a00dff5e02add41819138abb3284d', 'ok': True, 'id': 'mydoc'}



Update
------

>>> database.post({'_id': 'mydoc', 'hello': 'world', '_rev': '1-967a00dff5e02add41819138abb3284d'})
{'rev': '2-0a8fff77f08f178bd1e2905f7dfb54b2', 'ok': True, 'id': 'mydoc'}

Retrieve
--------


Delete
------


Server
======


Welcome
-------


Config
------








