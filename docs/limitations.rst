==========================
Limitations and behaviours
==========================

Firebird differs from PostgreSQL/MySQL in several areas. This page lists
everything the backend cannot fully hide, in rough order of how likely it
is to affect an application. Items marked *(tested)* correspond to
documented skips in the test suite.

Regular expression lookups *(tested)*
=====================================

Firebird has no regular expression engine. ``__regex`` and ``__iregex``
are implemented with SQL ``SIMILAR TO``, whose semantics differ from
Python/POSIX regexes:

* the pattern must match the **whole value** (no unanchored search),
* ``^`` and ``$`` anchors, ``\d``-style classes, lookarounds etc. are not
  supported — only SQL pattern syntax (``%``, ``_``, ``[...]``, ``|``,
  ``*``, ``+``, ``?``, ``{n,m}``, grouping),
* ``__iregex`` is made case-insensitive by uppercasing both sides.

If you need real regex filtering, do it in Python.

Timestamp resolution *(tested)*
===============================

Firebird timestamps store units of 100 microseconds. Values with finer
precision are rounded by the server, so exact-microsecond round-trips are
impossible and duration arithmetic is accurate to 100µs at best.

Identifier handling *(tested)*
==============================

The backend uppercases and quotes every identifier it creates, and
identifiers are truncated to Firebird's classic 31-character limit
(longer Django names are hashed into unique short names). Consequences:

* mixed-case table/column/constraint names cannot round-trip through
  introspection or ``inspectdb`` — everything comes back lowercase,
* when modelling an existing database with quoted mixed-case identifiers,
  introspection preserves their case, but names that are entirely
  uppercase in the system tables are reported lowercase (Django
  convention).

Decimal precision by server version *(tested)*
==============================================

* Firebird 4+ supports ``NUMERIC``/``DECIMAL`` up to 38 digits.
* Firebird 3 supports at most 18. ``DecimalField`` columns declared larger
  are created **clamped** to 18 digits with the field's integer digits
  preserved (e.g. ``max_digits=32, decimal_places=30`` becomes
  ``DECIMAL(18,16)``). Values still validate against the model's Python
  precision, but the stored value is rounded to the clamped scale.
  ``connection.features.supports_high_precision_decimals`` reports the
  capability.

The driver validates parameters against the column's range: filtering a
``DECIMAL(5,3)`` column with a number that cannot fit raises an error
instead of returning an empty result.

JSONField
=========

Firebird has no JSON type or functions; ``supports_json_field`` is False
and ``JSONField`` is unavailable.

Time zones
==========

``USE_TZ = False`` is fully supported. With ``USE_TZ = True``:

* On Firebird 4+, ``DateTimeField``/``TimeField`` columns are created
  ``WITH TIME ZONE`` and basic storage round-trips work.
* SQL-side conversion between zones (used by ``QuerySet.datetimes()``,
  ``__date`` lookups and ``Trunc`` with time zones) relies on
  ``AT TIME ZONE`` and is only partially correct today;
  ``has_zoneinfo_database`` is therefore declared False. Firebird 3 has no
  ``AT TIME ZONE`` at all.

Transactions
============

The backend commits statement-by-statement when autocommit is off
(``autocommit_when_autocommit_is_off``), so other transactions see
changes immediately and one documented ``atomic()`` recovery scenario
behaves differently than on other backends *(tested)*. DDL executed
through the schema editor is committed per statement as well. Treat
``transaction.atomic()`` guarantees with care in this backend.

Collations *(tested)*
=====================

* Collations can be applied to ``CharField`` columns (``db_collation``),
  including per-column case-insensitive collations such as
  ``UNICODE_CI``; ``TextField`` (blob) columns cannot be collated.
* On Firebird 6+ a collation change is a single ``ALTER``; on older
  servers the column is rebuilt through a temporary column, because
  ``ALTER ... TYPE`` rejects ``COLLATE`` there.

Booleans
========

``BooleanField`` maps to the native ``BOOLEAN`` type (Firebird 3+),
without check constraints. Databases created by django-firebird 4.x and
earlier keep their ``SMALLINT`` + check-constraint columns, which continue
to work through the value converters.

Foreign keys and indexes *(tested)*
===================================

Firebird creates the index backing a foreign key implicitly, named after
the constraint. Introspection therefore reports a single entry carrying
both the ``foreign_key`` and ``index`` flags, and the backend never
creates a separate index for plain foreign key fields.

Auto-increment fields
=====================

Implemented with a sequence plus a before-insert trigger
(``<TABLE>_SQ`` / ``<TABLE>_PK``) — see `internals.rst <internals.rst>`_.
They introspect as plain integer columns
(``introspected_field_types``), so ``inspectdb`` renders them as
``IntegerField``.

Other details
=============

* ``max_query_params`` is 255, which bounds ``bulk_create`` batch sizes.
* Very long ``VARCHAR`` columns cannot be indexed directly; the schema
  editor falls back to an expression index over ``hash(column)`` (usable
  for equality, not for range scans).
* The ``RDB$DB_KEY`` pseudo-column may be used as ``db_column`` on a
  ``managed = False`` model, e.g. as a surrogate primary key for legacy
  tables without one. It contains 8 raw bytes.
* Renaming a table (``db_table`` change) is implemented by creating the
  new table, copying data, and dropping the old one — plan for the extra
  I/O and for the table's dependent objects on big tables.
