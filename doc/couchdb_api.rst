================
CouchDB REST API
================

This is a quick tour of the CouchDB REST API, as you would use it from
Microfiber.  This is indented as a quick reference, and as such not all the API
is documented here.  For more details, see the full `CouchDB REST API`_.

.. _`CouchDB REST API`: http://www.couchbase.org/sites/default/files/uploads/all/documentation/couchbase-api.html


Database Methods
================

All the following database method examples assume you started out with this:

>>> from microfiber import Database, Server
>>> database = Database('db1')
>>> server = Server()


Create a database
-----------------

>>> database.put(None)
{'ok': True}
>>> server.put(None, 'db2')
{'ok': True}


Database info
-------------

>>> database.get()
{'update_seq': 0, 'disk_size': 79, 'purge_seq': 0, 'doc_count': 0, 'compact_running': False, 'db_name': 'db1', 'doc_del_count': 0, 'instance_start_time': '1314870632421649', 'committed_update_seq': 0, 'disk_format_version': 5}
>>> server.get('db2')
{'update_seq': 0, 'disk_size': 79, 'purge_seq': 0, 'doc_count': 0, 'compact_running': False, 'db_name': 'db2', 'doc_del_count': 0, 'instance_start_time': '1314870646021676', 'committed_update_seq': 0, 'disk_format_version': 5}


Database changes
----------------

>>> database.get('_changes')
{'last_seq': 0, 'results': []}
>>> server.get('db2', '_changes')
{'last_seq': 0, 'results': []}


Compact a database
------------------

>>> database.post(None, '_compact')
{'ok': True}
>>> server.post(None, 'db2', '_compact')
{'ok': True}


Delete a database
-----------------

>>> database.delete()
{'ok': True}
>>> server.delete('db2')
{'ok': True}



Document Methods
================








