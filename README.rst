===============
django-firebird
===============

.. image:: https://img.shields.io/pypi/v/django-firebird.svg
    :target: https://pypi.org/project/django-firebird/

.. image:: https://github.com/maxirobaina/django-firebird/actions/workflows/tests.yml/badge.svg
    :target: https://github.com/maxirobaina/django-firebird/actions/workflows/tests.yml


Firebird SQL backend for Django
-------------------------------

django-firebird lets Django applications use `Firebird <https://firebirdsql.org/>`_
as their database, on top of the official `firebird-driver
<https://pypi.org/project/firebird-driver/>`_.

Since version 5.0 the Python package is called ``django_firebird`` (the
previous ``firebird`` package name conflicted with the namespace used by
firebird-driver itself, see issue #130).

Compatibility
-------------

Every push is tested by `continuous integration
<https://github.com/maxirobaina/django-firebird/actions>`_ against the
official Firebird Docker images.

===================  ==========  =================  ===============  =========
django-firebird      Django      Firebird server    firebird-driver  Python
===================  ==========  =================  ===============  =========
5.1.x / master       6.1         3.0, 4.0, 5.0 [1]  2.x              3.12–3.14
stable/5.0.x         5.2 (LTS)   3.0, 4.0, 5.0 [1]  2.x              3.11–3.12
stable/2.2.x         2.2         2.x (fdb driver)   fdb              3.x
===================  ==========  =================  ===============  =========

[1] Firebird 6 development snapshots are also exercised in CI, as an
informational (non-blocking) target.

Installation
------------

**Using pip**::

    pip install django-firebird

This installs the ``django_firebird`` package and pulls in
``firebird-driver``. The driver loads the Firebird *client library*
(``libfbclient``) at runtime; install it from your distribution
(e.g. ``apt install libfbclient2``) or a Firebird server/client package.
Note that a Firebird 3 era client cannot describe the INT128-backed
``NUMERIC`` types of Firebird 4+ servers — use a client at least as new
as your newest server (see `docs/troubleshooting.rst
<docs/troubleshooting.rst>`_).

**From the repository**::

    git clone https://github.com/maxirobaina/django-firebird.git
    cd django-firebird
    pip install .

Configuration
-------------

Add a database to your ``settings.py``::

    DATABASES = {
        'default': {
            'ENGINE': 'django_firebird',
            'NAME': '/var/lib/firebird/data/mydb.fdb',  # path or alias on the server
            'USER': 'SYSDBA',
            'PASSWORD': '*****',
            'HOST': '127.0.0.1',
            'PORT': '3050',                # empty string uses the default 3050
            'OPTIONS': {
                'charset': 'UTF8',         # connection character set
                'lock_timeout': 10,        # seconds to wait on record locks
                                           # (optional; default: wait forever)
            },
        },
    }

All settings, including the test-database options and the environment
variables understood by the test suite, are documented in
`docs/settings.rst <docs/settings.rst>`_.

Then create your schema as usual::

    python manage.py migrate

Documentation
-------------

*  `docs/settings.rst <docs/settings.rst>`_ — every supported setting and
   ``OPTIONS`` key.
*  `docs/limitations.rst <docs/limitations.rst>`_ — Firebird capabilities and
   their consequences for the ORM (regex lookups, timestamp resolution,
   identifier case, decimal precision per server version, time zones, ...).
*  `docs/internals.rst <docs/internals.rst>`_ — how the backend maps Django
   concepts to Firebird: auto-increment fields, type mapping, introspection
   conventions, connection recovery.
*  `docs/testing.rst <docs/testing.rst>`_ — running the test suite locally
   and in CI, including ready-made Docker commands.
*  `docs/troubleshooting.rst <docs/troubleshooting.rst>`_ — common errors and
   what they mean.
*  `docs/changelog.txt <docs/changelog.txt>`_ — release history.

Upgrading from django-firebird 4.x or earlier
---------------------------------------------

*  Set ``'ENGINE': 'django_firebird'`` (the old ``firebird`` /
   ``django.db.backends.firebird`` values no longer exist).
*  firebird-driver 2.x replaces fdb; Firebird 3.0 is the minimum server.
*  ``BooleanField`` uses the native ``BOOLEAN`` type on new columns and no
   longer creates check constraints. Existing columns keep working.

Contributing
------------

Code and issues live on GitHub:

    https://github.com/maxirobaina/django-firebird

There is also a mailing list:

    http://groups.google.com/group/django-firebird-dev

License
-------

BSD. See ``LICENSE``.
