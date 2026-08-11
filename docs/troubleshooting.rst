===============
Troubleshooting
===============

Common errors and what they actually mean.

``'firebird' isn't an available database backend``
==================================================

You are using a pre-5.0 engine name. Since 5.0 the engine is::

    'ENGINE': 'django_firebird'

``django.db.backends.firebird`` never worked without a symlink hack and
is gone as well.

``TypeError: issubclass() arg 1 must be a class`` at import
===========================================================

firebird-driver 1.10.x combined with firebird-base 2.x (their APIs
diverged). Upgrade to ``firebird-driver>=2.0`` — django-firebird 5.x
requires it anyway.

``Data type unknown`` (SQLCODE -804)
====================================

Two distinct causes:

1. **Old client library with a new server.** A Firebird 3 era
   ``libfbclient`` (e.g. Ubuntu's ``libfbclient2``) cannot describe the
   INT128-backed ``NUMERIC`` types of Firebird 4+ servers. Install a
   newer client and, if needed, point the driver at it::

       from firebird.driver import driver_config
       driver_config.fb_client_library.value = '/path/to/libfbclient.so.2'

2. **A query parameter with no inferable type** (e.g. comparing two bare
   parameters). The backend inlines literal ``Value()`` expressions to
   avoid this; if you hit it with raw SQL, add an explicit
   ``CAST(? AS <type>)``.

``connection shutdown``
=======================

The server terminated the attachment — typically ``gfix -shut``, a
server restart, or an administrative kill. The backend discards the dead
connection and the next operation reconnects automatically (outside
``atomic()``). If you see this steadily on a Firebird 6 *development*
build under heavy DDL, it is a known snapshot-server behaviour, not an
application error.

``lock conflict on no wait transaction`` / ``lock time-out``
============================================================

Another transaction holds the record or metadata object. With
``OPTIONS['lock_timeout']`` set, waits give up after that many seconds
instead of blocking forever — decide per application which you want.

``arithmetic exception ... numeric value is out of range`` / SQLCODE -842
=========================================================================

On Firebird 3 the precision ceiling for ``NUMERIC``/``DECIMAL`` is 18
digits. ``DecimalField`` columns above it are created clamped to 18
digits (see `limitations.rst <limitations.rst>`_); values beyond the
clamped range cannot be stored on that server version.

``Token unknown ... COLLATE``
=============================

Raised by Firebird < 6 when ``COLLATE`` appears in ``ALTER ... TYPE`` or
before ``NOT NULL`` in a column definition. The backend emits the
correct grammar since 5.0.1 — upgrade if you see this from a migration.

``unsuccessful metadata update ... Cannot delete PRIMARY KEY being used
in FOREIGN KEY definition``
=======================================================================

You are dropping or rebuilding a table that other tables reference.
Drop the referencing foreign keys first (or migrate the referencing
models first). Table *renames* copy data into a new table and drop the
old one, so the same applies to them.

Emulated features behaving differently
======================================

If a query behaves differently than on PostgreSQL — regex lookups,
sub-millisecond timestamps, case of identifiers, time-zone conversions —
check `limitations.rst <limitations.rst>`_ first: several ORM features
are emulated within Firebird's capabilities and the differences are
documented there.
