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

Microfiber is licensed `LGPLv3+`_, requires `Python 3.4`_ or newer, and depends
upon `Degu`_ and `Dbase32`_.  To run the Microfiber unit tests, you will also
need `UserCouch`_ (a build dependency).


Contents:

.. toctree::
    :maxdepth: 2

    install 
    microfiber
    couchdb_api



.. _`Microfiber`: https://launchpad.net/microfiber
.. _`CouchDB`: http://couchdb.apache.org/
.. _`CouchDB REST API`: http://couchdb.readthedocs.org/en/latest/index.html
.. _`LGPLv3+`: http://www.gnu.org/licenses/lgpl-3.0.html

.. _`Novacut`: https://wiki.ubuntu.com/Novacut
.. _`Novacut Stable Releases PPA`: https://launchpad.net/~novacut/+archive/stable
.. _`Novacut Daily Builds PPA`: https://launchpad.net/~novacut/+archive/daily
.. _`#novacut`: http://webchat.freenode.net/?channels=novacut
.. _`Python 3.4`: https://docs.python.org/3.4/
.. _`Degu`: https://launchpad.net/degu
.. _`Dbase32`: https://launchpad.net/dbase32
.. _`UserCouch`: https://launchpad.net/usercouch

