Microfiber = Python3 + CouchDB
==============================

`Microfiber`_ is an abstract adapter for making HTTP requests to an arbitrary
JSON loving REST API like `CouchDB`_.  Rather than wrapping the API in a bunch
of one-off methods, Microfiber just makes it super easy to call any part of the
`CouchDB REST API`_, current or future.  This approach allows Microfiber to be
very simple and basically maintenance free as it requires no changes to support
new additions to the CouchDB API.

Microfiber is being developed as part of the `Novacut`_ project.  Microfiber
packages are available for Ubuntu in the `Novacut Stable Releases PPA`_ and the
`Novacut Daily Builds PPA`_.

If you have questions or need help getting started with Microfiber, please stop
by the `#novacut`_ IRC channel on freenode.

Microfiber is licensed `LGPLv3+`_.


Contents:

.. toctree::
   :maxdepth: 2
   
   microfiber
   couchdb_api



.. _`Microfiber`: https://launchpad.net/microfiber
.. _`CouchDB`: http://couchdb.apache.org/
.. _`CouchDB REST API`: http://www.couchbase.org/sites/default/files/uploads/all/documentation/couchbase-api.html
.. _`LGPLv3+`: http://www.gnu.org/licenses/lgpl-3.0.html

.. _`Novacut`: https://wiki.ubuntu.com/Novacut
.. _`Novacut Stable Releases PPA`: https://launchpad.net/~novacut/+archive/stable
.. _`Novacut Daily Builds PPA`: https://launchpad.net/~novacut/+archive/daily
.. _`#novacut`: http://webchat.freenode.net/?channels=novacut

