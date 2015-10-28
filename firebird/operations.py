from datetime import datetime

from django.conf import settings
from django.db.backends import BaseDatabaseOperations, util
from django.utils.functional import cached_property
from django.utils import six
from django.utils import timezone


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "firebird.compiler"

    def __init__(self, connection, *args, **kwargs):
        try:
            super(DatabaseOperations, self).__init__(connection)
        except TypeError:
            super(DatabaseOperations, self).__init__(*args, **kwargs)

    @cached_property
    def firebird_version(self):
        """
        Access method for firebird_version property.
        firebird_version return the version number in an object list format
        Useful for ask for just a part of a version number.
        (e.g. major version is firebird_version[0])
        """
        server_version = self.connection.server_version
        return [int(val) for val in server_version.split()[-1].split('.')]

    def autoinc_sql(self, table, column):
        sequence_name = get_autoinc_sequence_name(self, table)
        trigger_name = get_autoinc_trigger_name(self, table)
        table_name = self.quote_name(table)
        column_name = self.quote_name(column)
        sequence_sql = 'CREATE SEQUENCE %s;' % sequence_name
        next_value_sql = 'NEXT VALUE FOR %s' % sequence_name

        trigger_sql = '\n'.join([
            'CREATE TRIGGER %(trigger_name)s FOR %(table_name)s',
            'BEFORE INSERT AS',
            'BEGIN',
            '   IF(new.%(column_name)s IS NULL) THEN',
            '      new.%(column_name)s = %(next_value_sql)s;',
            'END'
        ]) % {
            'trigger_name': trigger_name,
            'table_name': table_name,
            'column_name': column_name,
            'next_value_sql': next_value_sql
        }

        return sequence_sql, trigger_sql

    def check_aggregate_support(self, aggregate_func):
        from django.db.models.sql.aggregates import Avg

        INVALID = ('STDDEV_SAMP', 'STDDEV_POP', 'VAR_SAMP', 'VAR_POP')
        if aggregate_func.sql_function in INVALID:
            raise NotImplementedError

        if isinstance(aggregate_func, Avg):
            aggregate_func.sql_template = '%(function)s(CAST(%(field)s as double precision))'

    def date_extract_sql(self, lookup_type, field_name):
        # Firebird uses WEEKDAY keyword.
        if lookup_type == 'week_day':
            return "EXTRACT(WEEKDAY FROM %s) + 1" % field_name
        return "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)

    def date_interval_sql(self, sql, connector, timedelta):
        """
        Implements the date interval functionality for expressions
        """
        sign = 1 if connector == '+' else -1
        if timedelta.days:
            unit = 'day'
            value = timedelta.days
        elif timedelta.seconds:
            unit = 'second'
            value = ((timedelta.days * 86400) + timedelta.seconds)
        elif timedelta.microseconds:
            unit = 'millisecond'
            value = timedelta.microseconds
        return 'DATEADD(%s %s TO %s)' % (value * sign, unit, sql)

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
            sql = "EXTRACT(year FROM %s)||'-01-01 00:00:00'" % field_name
        elif lookup_type == 'month':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-01 00:00:00'" % (field_name, field_name)
        elif lookup_type == 'day':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-'||EXTRACT(day FROM %s)||' 00:00:00'" % (field_name, field_name, field_name)
        return "CAST(%s AS TIMESTAMP)" % sql

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        """
        Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
        'second', returns the SQL that extracts a value from the given
        datetime field field_name, and a tuple of parameters.
        """
        if lookup_type == 'week_day':
            sql = "EXTRACT(WEEKDAY FROM %s) + 1" % field_name
        else:
            sql = "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)
        return sql, []

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        """
        Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
        'second', returns the SQL that truncates the given datetime field
        field_name to a datetime object with only the given specificity, and
        a tuple of parameters.
        """
        year = "EXTRACT(year FROM %s)" % field_name
        month = "EXTRACT(month FROM %s)" % field_name
        day = "EXTRACT(day FROM %s)" % field_name
        hh = "EXTRACT(hour FROM %s)" % field_name
        mm = "EXTRACT(minute FROM %s)" % field_name
        ss = "EXTRACT(second FROM %s)" % field_name
        if lookup_type == 'year':
            sql = "%s||'-01-01 00:00:00'" % year
        elif lookup_type == 'month':
            sql = "%s||'-'||%s||'-01 00:00:00'" % (year, month)
        elif lookup_type == 'day':
            sql = "%s||'-'||%s||'-'||%s||' 00:00:00'" % (year, month, day)
        elif lookup_type == 'hour':
            sql = "%s||'-'||%s||'-'||%s||' '||%s||':00:00'" % (year, month, day, hh)
        elif lookup_type == 'minute':
            sql = "%s||'-'||%s||'-'||%s||' '||%s||':'||%s||':00'" % (year, month, day, hh, mm)
        elif lookup_type == 'second':
            sql = "%s||'-'||%s||'-'||%s||' '||%s||':'||%s||':'||%s" % (year, month, day, hh, mm, ss)
        return "CAST(%s AS TIMESTAMP)" % sql, []

    def lookup_cast(self, lookup_type):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    def for_update_sql(self, nowait=False):
        """
        Returns the FOR UPDATE SQL clause to lock rows for an update operation.
        """
        # The nowait param depends on transaction setting
        #return 'FOR UPDATE WITH LOCK'
        return 'FOR UPDATE'

    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%s CONTAINING %%s' % self.quote_name(field_name)

    def last_insert_id(self, cursor, table, column):
        cursor.execute('SELECT GEN_ID(%s, 0) FROM rdb$database' % get_autoinc_sequence_name(self, table))
        return cursor.fetchone()[0]

    def max_in_list_size(self):
        """
        Returns the maximum number of items that can be passed in a single 'IN'
        list condition, or None if the backend does not impose a limit.
        Django break up the params list into an OR of manageable chunks.
        """
        return 1500

    def max_name_length(self):
        return 31

    def no_limit_value(self):
        return None

    def convert_values(self, value, field):
        if value is not None and field and field.get_internal_type() == 'DecimalField':
            value = util.typecast_decimal(field.format_number(value))
        elif value in (1, 0) and field and field.get_internal_type() in ('BooleanField', 'NullBooleanField'):
            value = bool(value)
        # Force floats to the correct type
        elif value is not None and field and field.get_internal_type() == 'FloatField':
            value = float(value)
        elif value is not None and field and (field.get_internal_type().endswith('IntegerField')
                                              or field.get_internal_type() == 'AutoField'):
            return int(value)
        return value

    def year_lookup_bounds(self, value):
        first = '%s-01-01 00:00:00'
        second = '%s-12-31 23:59:59.9999'
        return [first % value, second % value]

    def year_lookup_bounds_for_date_field(self, value):
        first = '%s-01-01'
        second = '%s-12-31'
        return [first % value, second % value]

    def quote_name(self, name):
        if not name.startswith('"') and not name.endswith('"'):
            name = '"%s"' % util.truncate_name(name, self.max_name_length())
        return name.upper()
        #return name

    def pk_default_value(self):
        return 'NULL'

    def deferrable_sql(self):
        return ''

    def return_insert_id(self):
        return "RETURNING %s", ()

    def random_function_sql(self):
        """
        Returns a SQL expression that returns a random value.
        """
        return 'RAND()'

    def savepoint_create_sql(self, sid):
        return "SAVEPOINT " + self.quote_name(sid)

    def savepoint_commint_sql(self, sid):
        return "RELEASE SAVEPOINT " + self.quote_name(sid)

    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO " + self.quote_name(sid)

    def _get_sequence_reset_sql(self, style):
        return """
        SELECT gen_id(%(sequence_name)s, coalesce(max(%(column_name)s), 0) - gen_id(%(sequence_name)s, 0) )
        FROM %(table_name)s;
        """

    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        for sequence_info in sequences:
            sequence_name = self.get_sequence_name(sequence_info['table'])
            table_name = self.quote_name(sequence_info['table'])
            column_name = self.quote_name(sequence_info['column'] or 'id')
            query = self._get_sequence_reset_sql(style) % {'sequence': sequence_name,
                                                           'table': table_name,
                                                           'column': column_name}
            sql.append(query)
        return sql

    def __sequence_reset_sql(self, style, model_list):
        """
        Attempt to make a reset sequence without create an extra store procerdure
        """
        from django.db import models

        output, procedures = [], []
        reset_value_sql = self._get_sequence_reset_sql(style)

        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    table_name = self.quote_name(model._meta.db_table)
                    column_name = self.quote_name(f.column)
                    sequence_name = self.get_sequence_name(model._meta.db_table)
                    output.append(reset_value_sql % {'sequence_name': sequence_name,
                                                     'column_name': column_name,
                                                     'table_name': table_name})
                    break
            for f in model._meta.many_to_many:
                if not f.rel.through:
                    table_name = self.quote_name(f.m2m_db_table())
                    column_name = self.quote_name(f.column)
                    sequence_name = get_autoinc_sequence_name(self, f.m2m_db_table())
                    output.append(reset_value_sql % {'sequence_name': sequence_name,
                                                     'column_name': column_name,
                                                     'table_name': table_name})
        return output

    def sequence_reset_sql(self, style, model_list):
        from django.db import models

        output, procedures = [], []
        KEYWORD = style.SQL_KEYWORD
        TABLE = style.SQL_TABLE
        FIELD = style.SQL_FIELD
        COLTYPE = style.SQL_COLTYPE
        reset_value_sql = 'ALTER SEQUENCE %(sequence_name)s RESTART WITH '
        procedure_sql = '\n'.join([
            '%s %s' % (KEYWORD('CREATE PROCEDURE'), TABLE('%(procedure_name)s')),
            KEYWORD('AS'),
            '%s %s %s;' % ( \
                KEYWORD('DECLARE VARIABLE'), FIELD('start_value'), COLTYPE('INTEGER')),
            KEYWORD('BEGIN'),
            '   %s gen_id(%s, coalesce(max(%s), 0) - gen_id(%s, 0))' % ( \
                KEYWORD('SELECT'), FIELD('%(sequence_name)s'),
                FIELD('%(column_name)s'), FIELD('%(sequence_name)s')),
            '   %s %s into %s;' % ( \
                KEYWORD('FROM'), TABLE('%(table_name)s'), FIELD(':start_value')),
            "   %s '%s' || %s || ';';" % ( \
                KEYWORD('EXECUTE STATEMENT'), reset_value_sql, FIELD(':start_value')),
            '   %s;' % KEYWORD('suspend'),
            '%s;' % KEYWORD('END')
        ])
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    table_name = self.quote_name(model._meta.db_table)
                    column_name = self.quote_name(f.column)
                    sequence_name = get_autoinc_sequence_name(self, model._meta.db_table)
                    procedure_name = get_reset_procedure_name(self, model._meta.db_table)
                    output.append(procedure_sql % locals())
                    procedures.append(procedure_name)
                    break
            for f in model._meta.many_to_many:
                if not f.rel.through:
                    table_name = self.quote_name(f.m2m_db_table())
                    column_name = self.quote_name(f.column)
                    sequence_name = get_autoinc_sequence_name(self, f.m2m_db_table())
                    procedure_name = get_reset_procedure_name(self, f.m2m_db_table())
                    output.append(procedure_sql % locals())
                    procedures.append(procedure_name)
        for procedure in procedures:
            output.append('%s %s;' % (KEYWORD('EXECUTE PROCEDURE'), TABLE(procedure)))
        for procedure in procedures:
            output.append('%s %s;' % (KEYWORD('DROP PROCEDURE'), TABLE(procedure)))

        return output

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            sql = ['%s %s %s;' %
                    (style.SQL_KEYWORD('DELETE'),
                     style.SQL_KEYWORD('FROM'),
                     style.SQL_TABLE(self.quote_name(table))
                     ) for table in tables]
            for generator_info in sequences:
                table_name = generator_info['table']
                sequence_name = self.get_sequence_name(table_name)
                query = "%s %s %s 0;" % (
                        style.SQL_KEYWORD('ALTER SEQUENCE'),
                        sequence_name,
                        style.SQL_KEYWORD('RESTART WITH')
                )
                sql.append(query)
            return sql
        else:
            return []

    def drop_sequence_sql(self, table):
        return 'DROP SEQUENCE %s' % self.get_sequence_name(table)

    def get_sequence_name(self, table_name):
        return get_autoinc_sequence_name(self, table_name)

    def get_sequence_trigger_name(self, table_name):
        return get_autoinc_trigger_name(self, table_name)

    def value_to_db_datetime(self, value):
        """
        Transform a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.
        """
        if value is None:
            return None

        # Firebird doesn't support tz-aware datetimes
        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                raise ValueError("Firebird backend does not support timezone-aware datetimes when USE_TZ is False.")

        if isinstance(value, datetime):
            value = str(value)
        if isinstance(value, six.string_types):
            #Replaces 6 digits microseconds to 4 digits allowed in Firebird
            value = value[:24]
        return six.text_type(value)

    def value_to_db_time(self, value):
        if value is None:
            return None

        # Firebird doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("Firebird backend does not support timezone-aware times.")

        return six.text_type(value)


def get_autoinc_sequence_name(ops, table):
    return ops.quote_name('%s_SQ' % util.truncate_name(table, ops.max_name_length() - 3))

def get_autoinc_trigger_name(ops, table):
    return ops.quote_name('%s_PK' % util.truncate_name(table, ops.max_name_length() - 3))

def get_reset_procedure_name(ops, table):
    return ops.quote_name('%s_RS' % util.truncate_name(table, ops.max_name_length() - 3))
