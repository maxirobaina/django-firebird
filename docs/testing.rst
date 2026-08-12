=================
Running the tests
=================

The suite in ``tests/test_main`` is derived from Django's own test suite
(synced with Django 5.2, updated for Django 6.1 behavior) plus
backend-specific apps, and runs against a real Firebird server.

Quick start with Docker
=======================

Start a server (any of ``3``, ``4``, ``5``, ``6-snapshot``)::

    docker run -d --name fb-test \
        -e FIREBIRD_ROOT_PASSWORD=masterkey \
        -p 3050:3050 firebirdsql/firebird:5

Install the package and Django, then run the suite::

    pip install "Django>=6.1,<6.2" .
    cd tests/test_main
    export FIREBIRD_DATABASE=/var/lib/firebird/data/django-test-default.fdb
    export FIREBIRD_DATABASE_OTHER=/var/lib/firebird/data/django-test-other.fdb
    python manage.py test --noinput

The database paths must be creatable *inside the server container*, hence
the ``/var/lib/firebird/data`` prefix (that image's data directory). All
connection details are environment-driven — see `settings.rst
<settings.rst>`_ for the full list of ``FIREBIRD_*`` variables.

The schema app
==============

``manage.py test schema`` exercises the schema editor with over 200 DDL
tests. It is slower than the rest of the suite (DDL plus lock waits) and
is still being hardened for Firebird < 6, so CI runs it as a
non-blocking step. Everything else — 1000+ tests — must pass.

Client library
==============

firebird-driver needs ``libfbclient``. Use a client at least as new as
your newest server: a Firebird 3 era client (such as Ubuntu's
``libfbclient2`` package) cannot describe the INT128-backed ``NUMERIC``
types returned by Firebird 4+ servers. CI copies the client out of the
``firebirdsql/firebird:5`` image::

    id=$(docker create firebirdsql/firebird:5)
    sudo docker cp "$id":/opt/firebird/lib/. /opt/firebird-client/
    docker rm "$id"
    export FIREBIRD_CLIENT_LIB=/opt/firebird-client/libfbclient.so.2

(the client also needs ``libtommath``, e.g. ``apt install libtommath1``).

Continuous integration
======================

``.github/workflows/tests.yml`` runs on every push and pull request:

* Firebird 3, 4, 5 and the 6 development snapshot (official Docker
  images) × Python 3.11 and 3.12;
* the non-schema suite is a required check on Firebird 3–5;
* the schema app step and the whole Firebird 6-snapshot leg are
  informational.

Writing backend-affecting changes
=================================

* Sync tests from Django upstream where possible; put Firebird-specific
  expectations behind ``connection.vendor == 'firebird'`` checks or —
  better — behind feature flags on ``connection.features``.
* Genuine engine limitations get a skip with a reason, not a deleted
  test (see `limitations.rst <limitations.rst>`_ for the catalogue).
* If a run is killed, a leftover test database can remain on the server;
  drop it with the driver (``connect(...).drop_database()``) or remove
  the file inside the container.
