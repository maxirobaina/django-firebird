"""
Firebird database backend for Django.
"""

import sys

try:
    import fdb as Database
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading fdb module: %s" % e)

from fdb.ibase import charset_map

from django.db import utils
from django.db.backends.base.base import BaseDatabaseWrapper

from django.utils.encoding import smart_str
from django.utils.functional import cached_property
from django.utils import six

from .operations import DatabaseOperations
from .features import DatabaseFeatures
from .client import DatabaseClient
from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection
from .schema import DatabaseSchemaEditor
from .validation import DatabaseValidation


DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'firebird'

    # This dictionary maps Field objects to their associated Firebird column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    #
    # Any format strings starting with "qn_" are quoted before being used in the
    # output (the "qn_" prefix is stripped before the lookup is performed.

    data_types = {
        'AutoField': 'integer',
        'BigAutoField': 'bigint',
        'BinaryField': 'blob sub_type 0',
        'BooleanField': 'smallint',
        'CharField': 'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'timestamp',
        'DecimalField': 'decimal(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'bigint',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'char(15)',
        'GenericIPAddressField': 'char(39)',
        'NullBooleanField': 'smallint',
        'OneToOneField': 'integer',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField': 'blob sub_type 1',
        'TimeField': 'time',
        'UUIDField': 'char(32)',
    }

    data_type_check_constraints = {
        'BooleanField': '%(qn_column)s IN (0,1)',
        'NullBooleanField': '(%(qn_column)s IN (0,1)) OR (%(qn_column)s IS NULL)',
        'PositiveIntegerField': '%(qn_column)s >= 0',
        'PositiveSmallIntegerField': '%(qn_column)s >= 0',
    }

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': "LIKE %s ESCAPE'\\'",
        'icontains': "LIKE UPPER(%s) ESCAPE'\\'",  # 'CONTAINING %s', #case is ignored
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE'\\'",  # 'STARTING WITH %s', #looks to be faster than LIKE
        'endswith': "LIKE %s ESCAPE'\\'",
        'istartswith': "LIKE UPPER(%s) ESCAPE'\\'",  # 'STARTING WITH UPPER(%s)',
        'iendswith': "LIKE UPPER(%s) ESCAPE'\\'",
        'regex': "SIMILAR TO %s",
        'iregex': "SIMILAR TO %s",  # Case Sensitive depends on collation
    }

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    #
    # Note: we use str.format() here for readability as '%' is used as a wildcard for
    # the LIKE operator.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor

    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self._server_version = None
        self._db_charset = None
        self.encoding = None

        opts = self.settings_dict["OPTIONS"]
        RC = Database.ISOLATION_LEVEL_READ_COMMITED
        self.isolation_level = opts.get('isolation_level', RC)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)

    # #### Backend-specific methods for creating connections and cursors #####

    def get_connection_params(self):
        """Returns a dict of parameters suitable for get_new_connection."""
        settings_dict = self.settings_dict
        if settings_dict['NAME'] == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME value.")

        # The port param is not used by fdb. It must be setting by dsn string
        if settings_dict['PORT']:
            dsn = '%(HOST)s/%(PORT)s:%(NAME)s'
        else:
            dsn = '%(HOST)s:%(NAME)s'
        conn_params = {'charset': 'UTF8'}
        conn_params['dsn'] = dsn % settings_dict
        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        if 'ROLE' in settings_dict:
            conn_params['role'] = settings_dict['ROLE']
        options = settings_dict['OPTIONS'].copy()
        conn_params.update(options)

        self._db_charset = conn_params.get('charset')
        self.encoding = charset_map.get(self._db_charset, 'utf_8')

        return conn_params

    def get_new_connection(self, conn_params):
        """Opens a connection to the database."""
        return Database.connect(**conn_params)

    def init_connection_state(self):
        """Initializes the database connection settings."""
        pass

    def create_cursor(self, name=None):
        """Creates a cursor. Assumes that a connection is established."""
        cursor = self.connection.cursor()
        return FirebirdCursorWrapper(cursor, self.encoding)

    # #### Backend-specific transaction management methods #####

    def _set_autocommit(self, autocommit):
        """
        Backend-specific implementation to enable or disable autocommit.

        FDB doesn't support auto-commit feature directly, but developers may
        achieve the similar result using explicit transaction start, taking
        advantage of default_action and its default value (commit).
        See:
        http://www.firebirdsql.org/file/documentation/drivers_documentation/python/fdb/usage-guide.html#auto-commit

        Pay attention at _close() method below
        """
        pass

    # #### Backend-specific wrappers for PEP-249 connection methods #####

    def _close(self):
        if self.connection is not None:
            with self.wrap_database_errors:
                if self.autocommit is True:
                    self.connection.commit()
                return self.connection.close()

    # #### Connection termination handling #####

    def is_usable(self):
        """
        Tests if the database connection is usable.
        This function may assume that self.connection is not None.
        """
        try:
            cur = self.connection.cursor()
            cur.execute('SELECT 1 FROM RDB$DATABASE')
        except DatabaseError:
            return False
        else:
            return True

    @cached_property
    def server_version(self):
        """
        Access method for engine_version property.
        engine_version return a full version in string format
        (ie: 'WI-V6.3.5.4926 Firebird 1.5' )
        """
        if not self._server_version:
            if not self.connection:
                self.cursor()
            self._server_version = self.connection.db_info(Database.isc_info_firebird_version)
        return self._server_version


class FirebirdCursorWrapper(object):
    """
    Django uses "format" style placeholders, but firebird uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    """
    codes_for_integrityerror = (-803, -625, -530)

    def __init__(self, cursor, encoding):
        self.cursor = cursor
        self.encoding = encoding

    def execute(self, query, params=None):
        if params is None:
            params = []
        try:
            q = self.convert_query(query, len(params))
            return self.cursor.execute(q, params)
        except Database.IntegrityError as e:
            six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, params)), sys.exc_info()[2])
        except Database.DatabaseError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            # fdb: raise exception as tuple with (error_msg, sqlcode, error_code)
            code = self.get_sql_code(e)
            if code in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, params)), sys.exc_info()[2])
            raise

    def executemany(self, query, param_list):
        try:
            q = self.convert_query(query, len(param_list[0]))
            return self.cursor.executemany(q, param_list)
        except Database.IntegrityError as e:
            six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2])
        except Database.DatabaseError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            # fdb: raise exception as tuple with (error_msg, sqlcode, error_code)
            code = self.get_sql_code(e)
            if code in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2])
            raise

    def convert_query(self, query, num_params):
        # kinterbasdb tries to convert the passed SQL to string.
        # But if the connection charset is NONE, ASCII or OCTETS it will fail.
        # So we convert it to string first.
        if num_params == 0:
            return smart_str(query, self.encoding)
        return smart_str(query % tuple("?" * num_params), self.encoding)

    def get_sql_code(self, e):
        try:
            sql_code = e.args[1]
        except IndexError:
            sql_code = None
        return sql_code

    def error_info(self, e, q, p):
        # fdb: raise exception as tuple with (error_msg, sqlcode, error_code)
        # just when it uses exception_from_status function. Ticket #44.
        try:
            error_msg = e.args[0]
        except IndexError:
            error_msg = ''

        try:
            sql_code = e.args[1]
        except IndexError:
            sql_code = None

        try:
            error_code = e.args[2]
        except IndexError:
            error_code = None

        if q:
            sql_text = q % tuple(p)
        else:
            sql_text = q
        return tuple([error_msg, sql_code, error_code, {'sql': sql_text, 'params': p}])

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)
