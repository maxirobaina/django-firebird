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
from django.db.backends import *
from django.db.backends.signals import connection_created
from django.utils.encoding import smart_str
from django.utils.functional import cached_property
from django.utils import six

from .operations import DatabaseOperations
from .client import DatabaseClient
from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError


class DatabaseFeatures(BaseDatabaseFeatures):
    allows_group_by_pk = True
    supports_forward_references = False
    has_bulk_insert = False
    can_return_id_from_insert = True
    has_select_for_update = True
    supports_tablespaces = False
    supports_timezones = False

    @cached_property
    def supports_transactions(self):
        return True


class DatabaseValidation(BaseDatabaseValidation):
    pass


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'firebird'
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': "LIKE %s ESCAPE'\\'",
        'icontains': "LIKE UPPER(%s) ESCAPE'\\'", #'CONTAINING %s', #case is ignored
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE'\\'", #'STARTING WITH %s', #looks to be faster than LIKE
        'endswith': "LIKE %s ESCAPE'\\'",
        'istartswith': "LIKE UPPER(%s) ESCAPE'\\'", #'STARTING WITH UPPER(%s)',
        'iendswith': "LIKE UPPER(%s) ESCAPE'\\'",
        'regex': "SIMILAR TO %s",
        'iregex': "SIMILAR TO %s", # Case Sensitive depends on collation
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self._server_version = None
        self._db_charset = None
        self.encoding = None

        try:
            self.features = DatabaseFeatures()
        except TypeError:
            self.features = DatabaseFeatures(self)

        try:
            self.ops = DatabaseOperations()
        except TypeError:
            self.ops = DatabaseOperations(self)

        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)

    def _cursor(self):
        if self.connection is None:
            settings_dict = self.settings_dict
            if settings_dict['NAME'] == '':
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured(
                        "settings.DATABASES is improperly configured. "
                        "Please supply the NAME value.")

            conn_params = {'charset': 'UTF8'}
            conn_params['dsn'] = settings_dict['NAME']
            if settings_dict['HOST']:
                conn_params['dsn'] = ('%s:%s') % (settings_dict['HOST'], conn_params['dsn'])
            if settings_dict['PORT']:
                conn_params['port'] = settings_dict['PORT']
            if settings_dict['USER']:
                conn_params['user'] = settings_dict['USER']
            if settings_dict['PASSWORD']:
                conn_params['password'] = settings_dict['PASSWORD']
            options = settings_dict['OPTIONS'].copy()
            conn_params.update(options)
            self._db_charset = conn_params.get('charset')
            self.encoding = charset_map.get(self._db_charset, 'utf_8')
            self.connection = Database.connect(**conn_params)
            connection_created.send(sender=self.__class__)

        return FirebirdCursorWrapper(self.connection.cursor(), self.encoding)

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
    codes_for_integrityerror = (-803, -625)

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
            if e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, params)), sys.exc_info()[2])
            six.reraise(utils.DatabaseError, utils.DatabaseError(*self.error_info(e, query, params)), sys.exc_info()[2])

    def executemany(self, query, param_list):
        try:
            q = self.convert_query(query, len(param_list[0]))
            return self.cursor.executemany(q, param_list)
        except Database.IntegrityError as e:
            six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2])
        except Database.DatabaseError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_integrityerror:
                six.reraise(utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2])
            six.reraise(utils.DatabaseError, utils.DatabaseError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2])

    def convert_query(self, query, num_params):
        # kinterbasdb tries to convert the passed SQL to string.
        # But if the connection charset is NONE, ASCII or OCTETS it will fail.
        # So we convert it to string first.
        if num_params == 0:
            return smart_str(query, self.encoding)
        return smart_str(query % tuple("?" * num_params), self.encoding)

    def error_info(self, e, q, p):
        return tuple([e[0], '%s -- %s' % (e[1], q % tuple(p))])

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)
