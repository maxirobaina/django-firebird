========
Settings
========

Database settings
=================

A complete ``DATABASES`` entry::

    DATABASES = {
        'default': {
            'ENGINE': 'django_firebird',
            'NAME': '/var/lib/firebird/data/mydb.fdb',
            'USER': 'SYSDBA',
            'PASSWORD': '*****',
            'HOST': '127.0.0.1',
            'PORT': '3050',
            'ROLE': 'MYROLE',                        # optional SQL role
            'OPTIONS': {
                'charset': 'UTF8',
                'lock_timeout': 10,
            },
            'TEST': {
                'NAME': '/var/lib/firebird/data/test_mydb.fdb',
                'CHARSET': 'UTF8',
                'PAGE_SIZE': 8192,
                'SERIALIZE': False,
            },
        },
    }

``ENGINE``
    Always ``'django_firebird'``.

``NAME``
    Path of the database file *on the server*, or a database alias defined
    in the server's ``databases.conf``. The backend builds the DSN as
    ``HOST[/PORT]:NAME``; with an empty ``HOST`` the name is passed through
    unchanged, which allows embedded/local access.

``USER`` / ``PASSWORD`` / ``ROLE``
    Credentials, and optionally the SQL role to assume.

``HOST`` / ``PORT``
    Server location. An empty ``PORT`` uses Firebird's default 3050.

``OPTIONS``
    Extra keyword arguments for ``firebird.driver.connect()``, plus the
    backend-specific keys below. Anything not listed here (for example
    ``no_db_triggers`` or ``session_time_zone``) is passed to the driver
    unchanged.

    ``charset``
        Connection character set. ``UTF8`` is strongly recommended and is
        what the test suite runs with. This also becomes the character set
        named in DDL that needs one (e.g. collation changes).

    ``lock_timeout``
        Transaction lock wait timeout in *seconds*. Firebird's default is to
        wait forever when two transactions touch the same record or when DDL
        waits on an object lock; with this option such waits fail with a
        lock timeout error instead. The test suite sets it to a few seconds
        so that schema-editor conflicts surface as errors rather than hangs.

``TEST``
    Options used when Django creates the test database:

    ``NAME``
        Test database path/alias. Give it an explicit server-side path when
        the server runs in a container or restricts database locations.

    ``CHARSET``
        Default character set of the created database (``UTF8``
        recommended).

    ``PAGE_SIZE``
        Page size of the created database (default 8192).

Time zones
==========

``USE_TZ = False`` (naive datetimes) is fully supported: ``DateTimeField``
maps to a plain ``TIMESTAMP`` and values round-trip naive, on every server
version.

With ``USE_TZ = True`` on Firebird 4+ the backend creates
``TIMESTAMP WITH TIME ZONE`` / ``TIME WITH TIME ZONE`` columns. SQL-side
time zone *conversion* (``AT TIME ZONE``) is only partially implemented —
see `limitations.rst <limitations.rst>`_ — so time-zone-heavy applications
should currently prefer ``USE_TZ = False`` or validate their queries
carefully.

Environment variables (test suite)
==================================

``tests/test_main/test_main/settings.py`` reads its connection details from
the environment, which is how CI points the suite at its service
containers:

=============================  ======================================  =======================
Variable                       Meaning                                 Default
=============================  ======================================  =======================
``FIREBIRD_HOST``              Server host                             ``127.0.0.1``
``FIREBIRD_PORT``              Server port                             ``''`` (3050)
``FIREBIRD_USER``              User                                    ``SYSDBA``
``FIREBIRD_PASSWORD``          Password                                ``masterkey``
``FIREBIRD_DATABASE``          Default-alias test database path        ``django-test-default``
``FIREBIRD_DATABASE_OTHER``    ``other``-alias test database path      ``django-test-other``
``FIREBIRD_LOCK_TIMEOUT``      ``OPTIONS['lock_timeout']`` (seconds)   ``5``
``FIREBIRD_CLIENT_LIB``        Path to ``libfbclient`` to load         unset (system lookup)
=============================  ======================================  =======================

``FIREBIRD_CLIENT_LIB`` sets ``firebird.driver.driver_config
.fb_client_library`` before the first connection. Applications that need
the same control can do exactly that in their own settings module::

    from firebird.driver import driver_config
    driver_config.fb_client_library.value = '/opt/firebird/lib/libfbclient.so.2'
