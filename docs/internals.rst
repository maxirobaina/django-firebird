=================
Backend internals
=================

How the backend maps Django concepts onto Firebird. Useful when debugging,
or when working with a django-firebird database from other tools.

Type mapping
============

===========================  ======================================
Django field                 Firebird column type
===========================  ======================================
AutoField                    ``integer`` + sequence & trigger
BigAutoField                 ``bigint`` + sequence & trigger
SmallAutoField               ``smallint`` + sequence & trigger
BooleanField                 ``boolean``
CharField / SlugField        ``varchar(n)``
TextField                    ``blob sub_type 1``
BinaryField                  ``blob sub_type 0``
DateField / TimeField        ``date`` / ``time``
DateTimeField                ``timestamp`` (``... with time zone``
                             on Firebird 4+ when ``USE_TZ=True``)
DecimalField                 ``decimal(digits, places)`` (clamped to
                             18 digits on Firebird 3)
DurationField                ``bigint`` (microseconds)
FloatField                   ``double precision``
UUIDField                    ``char(32)`` (hex, no dashes)
GenericIPAddressField        ``char(39)``
PositiveIntegerField etc.    integer type + ``>= 0`` check constraint
===========================  ======================================

Auto-increment implementation
=============================

For every auto field the backend creates::

    CREATE SEQUENCE "<TABLE>_SQ";
    CREATE TRIGGER "<TABLE>_PK" FOR "<TABLE>"
    BEFORE INSERT AS
    BEGIN
        IF (NEW."ID" IS NULL) THEN
            NEW."ID" = NEXT VALUE FOR "<TABLE>_SQ";
    END

Long table names are hashed to fit the 31-character identifier limit.
The pair is dropped when the field is removed or altered to a
non-auto field, and created when a field becomes an auto field.
``sqlflush``/``flush`` restarts the sequences with ``ALTER SEQUENCE``.

Identifier quoting
==================

``quote_name`` uppercases and double-quotes every identifier, truncating
to 31 characters (Django's standard hashed truncation). The single
exception is the ``RDB$DB_KEY`` pseudo-column, which Firebird only
accepts unquoted.

Introspection conventions
=========================

* Identifiers that are stored all-uppercase (the normal case for objects
  created by this backend) are reported lowercase; quoted mixed-case
  identifiers created outside Django keep their case.
* Column collations are reported only when they differ from the character
  set default; domain-level collations are included.
* A foreign key's implicit backing index is part of the constraint's
  entry (``foreign_key`` set and ``index=True``) rather than a separate
  entry.
* Asking for a nonexistent table raises ``DatabaseError`` (so
  ``inspectdb`` reports it instead of generating an empty model).

Expression handling
===================

Firebird cannot infer the type of a query parameter that appears without
any column operand ("Data type unknown"). The backend registers
``as_firebird`` handlers that render plain literal ``Value()``
expressions inline (with typed ``TIMESTAMP``/``DATE``/``TIME`` literals
for temporal values), rewrite boolean-to-integer ``Cast`` as
``CASE WHEN``, trim ``CASE`` expressions built purely from string
literals (which would otherwise be blank-padded ``CHAR``), and emulate
``REPEAT`` with ``RPAD``.

Duration arithmetic uses ``DATEADD`` with fractional milliseconds;
datetime subtraction combines ``DATEDIFF(SECOND)`` with the
``EXTRACT(MILLISECOND)`` fractional parts to keep the server's full
100µs resolution.

Row limiting uses standard ``OFFSET n ROWS FETCH FIRST m ROWS ONLY``.

Transaction management
======================

Cursors run in READ COMMITTED transactions started per statement batch;
when autocommit is off the backend still commits statement-by-statement
(``autocommit_when_autocommit_is_off = True``). The optional
``OPTIONS['lock_timeout']`` is applied to every transaction the backend
starts, converting indefinite lock waits into errors.

Dead connection recovery
========================

When the server shuts an attachment down (``gfix -shut``, a server
restart, or administrative disconnect), the backend discards the broken
connection so the next operation transparently reconnects — but only
outside ``atomic()`` blocks, where no transaction state can be lost.
Driver objects belonging to the dead attachment are quarantined so their
finalizers cannot attempt network calls from the garbage collector.
``is_usable()`` is implemented, so Django's ``CONN_HEALTH_CHECKS`` and
``close_if_unusable_or_obsolete()`` work as documented.

Schema editor notes
===================

* Check constraints are table-level (``supports_column_check_constraints
  = False``).
* Columns converted from/to blobs — and collation changes on Firebird
  < 6 — are rebuilt through a temporary column (add, copy, drop old,
  rename), restoring ``NOT NULL`` afterwards.
* ``COLLATE`` is emitted at the end of column definitions, as Firebird's
  grammar requires; ``ALTER ... TYPE`` names the connection character set
  explicitly whenever it names a collation.
* Indexes that exceed Firebird's key-size limit fall back to an
  expression index over ``hash(...)``.
* ``sql_flush`` orders ``DELETE`` statements so referencing tables are
  cleared before the tables they reference.
