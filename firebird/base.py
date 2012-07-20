"""
Firebird database backend for Django.

Requires kinterbasdb: http://www.firebirdsql.org/index.php?op=devel&sub=python
"""

import sys
import datetime

try:
    from decimal import Decimal
except ImportError:
    from django.utils._decimal import Decimal


try:
    import kinterbasdb as Database
    import kinterbasdb.typeconv_datetime_stdlib as typeconv_dt
    import kinterbasdb.typeconv_fixed_decimal as typeconv_fd
    import kinterbasdb.typeconv_text_unicode as typeconv_tu
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading kinterbasdb module: %s" % e)

from django.db import utils
from django.db.backends import *
from django.db.backends.signals import connection_created
from django.utils.encoding import smart_str, smart_unicode

from operations import DatabaseOperations
from client import DatabaseClient
from creation import DatabaseCreation
from introspection import DatabaseIntrospection

DB_CHARSET_TO_PYTHON_CHARSET = typeconv_tu.DB_CHAR_SET_NAME_TO_PYTHON_ENCODING_MAP
del DB_CHARSET_TO_PYTHON_CHARSET['OCTETS']

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError

class DatabaseFeatures(BaseDatabaseFeatures):
    can_return_id_from_insert = False
    uses_savepoints = False
    allows_group_by_pk = True
    supports_forward_references = False
    has_bulk_insert = False

    def _supports_transactions(self):
        "Confirm support for transactions"
        cursor = self.connection.cursor()
        cursor.execute('CREATE TABLE ROLLBACK_TEST (X INT)')
        self.connection._commit()

        cursor.execute('INSERT INTO ROLLBACK_TEST (X) VALUES (8)')
        self.connection._rollback()

        cursor.execute('SELECT COUNT(X) FROM ROLLBACK_TEST')
        count, = cursor.fetchone()

        cursor.execute('DROP TABLE ROLLBACK_TEST')
        #self.connection._commit()

        return count == 0

class DatabaseValidation(BaseDatabaseValidation):
    pass

class TypeTranslator(object):
    #db_charset_code = None
    #charset = None
    encoding = None

    def set_charset(self, db_charset, encoding):
        #self.db_charset_code = DB_CHARSET_TO_DB_CHARSET_CODE[db_charset]
        #self.charset = DB_CHARSET_TO_PYTHON_CHARSET[db_charset]
        self.encoding = DB_CHARSET_TO_PYTHON_CHARSET.get(db_charset, encoding)

    @property
    def type_translate_in(self):
        return {
            'DATE': self.in_date,
            'TIME': self.in_time,
            'TIMESTAMP': self.in_timestamp,
            'FIXED': self.in_fixed,
            'TEXT': self.in_text,
            'TEXT_UNICODE': self.in_text_unicode, #typeconv_tu.unicode_conv_in,
            'BLOB': self.in_text
        }

    def in_date(self, value):
        if isinstance(value, basestring):
            value = value[:24]
        return typeconv_dt.date_conv_in(value)

    def in_time(self, value):
        if isinstance(value, datetime.datetime):
            value = value.time()
        return typeconv_dt.time_conv_in(value)

    def in_timestamp(self, value):
        if isinstance(value, basestring):
            value = value[:24]
        return typeconv_dt.timestamp_conv_in(value)

    def in_fixed(self, (value, scale)):
        if value is not None:
            if isinstance(value, basestring):
                value = Decimal(value)
            return typeconv_fd.fixed_conv_in_precise((value, scale))

    def in_text(self, value):
        if value is not None:
            value = smart_str(value, self.encoding)
        return value

    def in_text_unicode(self, (value, charset)):
        if value is not None:
            return smart_str(value, self.encoding)
        return value
        #return typeconv_tu.unicode_conv_in((value, charset))

    @property
    def type_translate_out(self):
        return {
            'DATE': typeconv_dt.date_conv_out,
            'TIME': typeconv_dt.time_conv_out,
            'TIMESTAMP': typeconv_dt.timestamp_conv_out,
            'FIXED': typeconv_fd.fixed_conv_out_precise,
            'TEXT': self.out_text,
            'TEXT_UNICODE': typeconv_tu.unicode_conv_out,
            'BLOB': self.out_text
        }

    def out_text(self, value):
        if value is not None:
            value = smart_unicode(value, self.encoding)
        return value

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
        self._type_translator = TypeTranslator()

        self.features = DatabaseFeatures(self)
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
                raise ImproperlyConfigured("You need to specify DATABASE_NAME in your Django settings file.")
            conn_params = {
                'charset': 'UNICODE_FSS'
            }
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
            # Normalization for databases with 'NONE', 'OCTETS' or 'ASCII' charset.
            encoding = options.pop('encoding', 'utf_8')
            conn_params.update(options)
            self.connection = Database.connect(**conn_params)
            self._type_translator.set_charset(self.connection.charset, encoding)
            connection_created.send(sender=self.__class__)

            if self.ops.firebird_version[0] >= 2:
                self.features.can_return_id_from_insert = True

        return FirebirdCursorWrapper(self.connection.cursor(), self._type_translator)

    def get_server_version(self):
        """
        Access method for engine_version property.
        engine_version return a full version in string format
        (ie: 'WI-V6.3.5.4926 Firebird 1.5' )
        """
        if not self._server_version:
            if not self.connection:
                self.cursor()
            self._server_version = self.connection.server_version
        return self._server_version

class FirebirdCursorWrapper(object):
    """
    Django uses "format" style placeholders, but firebird uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".

    We need to do some data translation too.
    See: http://kinterbasdb.sourceforge.net/dist_docs/usage.html for Dynamic Type Translation
    """
    codes_for_integrityerror = (-803, -625)

    def __init__(self, cursor, type_translator):
        self.cursor = cursor
        self.encoding = type_translator.encoding
        self.cursor.set_type_trans_in(type_translator.type_translate_in)
        self.cursor.set_type_trans_out(type_translator.type_translate_out)

    def execute(self, query, params=None):
        if params is None:
            params = []

        cquery = self.convert_query(query, len(params))
        try:
            return self.cursor.execute(cquery, params)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, params)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_integrityerror:
                raise utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, params)), sys.exc_info()[2]
            raise utils.DatabaseError, utils.DatabaseError(*self.error_info(e, query, params)), sys.exc_info()[2]

    def executemany(self, query, param_list):
        try:
            query = self.convert_query(query, len(param_list[0]))
            return self.cursor.executemany(query, param_list)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2]
        except Database.DatabaseError, e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            if e[0] in self.codes_for_integrityerror:
                raise utils.IntegrityError, utils.IntegrityError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2]
            raise utils.DatabaseError, utils.DatabaseError(*self.error_info(e, query, param_list[0])), sys.exc_info()[2]

    def convert_query(self, query, num_params):
        # kinterbasdb tries to convert the passed SQL to string.
        # But if the connection charset is NONE, ASCII or OCTETS it will fail.
        # So we convert it to string first.
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
