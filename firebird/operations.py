from django.db.backends import BaseDatabaseOperations, util

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "firebird.compiler"

    def __init__(self, connection):
        super(DatabaseOperations, self).__init__(connection)
        self.connection = connection

    def _get_firebird_version(self):
        """
        Access method for firebird_version property.
        firebird_version return the version number in an object list format
        Useful for ask for just a part of a version number.
        (e.g. major version is firebird_version[0])
        """
        server_version = self.connection.get_server_version()
        return [int(val) for val in server_version.split()[-1].split('.')]
    firebird_version = property(_get_firebird_version)

    def autoinc_sql(self, table, column):
        sequence_name = get_autoinc_sequence_name(self, table)
        trigger_name = get_autoinc_sequence_name(self, table)
        table_name = self.quote_name(table)
        column_name = self.quote_name(column)

        if self.firebird_version[0] < 2:
            sequence_sql = 'CREATE GENERATOR %s;' % sequence_name
            next_value_sql = 'GEN_ID(%s, 1)' % sequence_name
        else:
            sequence_sql = 'CREATE SEQUENCE %s;' % sequence_name
            next_value_sql = 'NEXT VALUE FOR %s' % sequence_name

        trigger_sql = '\n'.join([
            'CREATE TRIGGER %(trigger_name)s FOR %(table_name)s',
            'BEFORE INSERT AS',
            'BEGIN',
            '   IF(new.%(column_name)s IS NULL) THEN',
            '      new.%(column_name)s = %(next_value_sql)s;',
            'END'
        ]) % locals()

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

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
            sql = "EXTRACT(year FROM %s)||'-01-01 00:00:00'" % field_name
        elif lookup_type == 'month':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-01 00:00:00'" % (field_name, field_name)
        elif lookup_type == 'day':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-'||EXTRACT(day FROM %s)||' 00:00:00'" % (field_name, field_name, field_name)
        return "CAST(%s AS TIMESTAMP)" % sql

    def lookup_cast(self, lookup_type):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    def for_update_sql(self, nowait=False):
        """
        Returns the FOR UPDATE SQL clause to lock rows for an update operation.
        """
        # The nowait param depends on transaction setting
        return 'FOR UPDATE WITH LOCK'

    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%s CONTAINING %%s' % self.quote_name(field_name)

    def last_insert_id(self, cursor, table, column):
        cursor.execute('SELECT GEN_ID(%s, 0) FROM rdb$database' % get_autoinc_sequence_name(self, table))
        return cursor.fetchone()[0]

    def max_name_length(self):
        return 31

    def convert_values(self, value, field):
        value = super(DatabaseOperations, self).convert_values(value, field)
        if value is not None and field and field.get_internal_type() == 'DecimalField':
            value = util.typecast_decimal(field.format_number(value))
        return value

    def ___value_to_db_datetime(self, value):
        value = super(DatabaseOperations, self).value_to_db_datetime(value)
        if isinstance(value, basestring):
            #Replaces 6 digits microseconds to 4 digits allowed in Firebird
            value = value[:24]
        #return value
        print value
        return typeconv_dt.timestamp_conv_in(value)

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

    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO " + self.quote_name(sid)

    def sequence_reset_sql(self, style, model_list):
        from django.db import models

        qn = self.quote_name
        output, procedures = [], []
        KEYWORD = style.SQL_KEYWORD
        TABLE = style.SQL_TABLE
        FIELD = style.SQL_FIELD
        COLTYPE = style.SQL_COLTYPE
        if self.firebird_version[0] < 2:
            reset_value_sql = 'SET GENERATOR %(sequence_name)s TO '
        else:
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
            output.append('%s %s' % (KEYWORD('EXECUTE PROCEDURE'), TABLE(procedure)))
        for procedure in procedures:
            output.append('%s %s' % (KEYWORD('DROP PROCEDURE'), TABLE(procedure)))

        return output

    def sql_flush(self, style, tables, sequences):
        if tables:
            sql = ['%s %s %s;' % \
                    (style.SQL_KEYWORD('DELETE'),
                     style.SQL_KEYWORD('FROM'),
                     style.SQL_TABLE(self.quote_name(table))
                     ) for table in tables]
            for generator_info in sequences:
                table_name = generator_info['table']
                query = "%s %s %s 0;" % (style.SQL_KEYWORD('SET GENERATOR'),
                    self.get_generator_name(table_name), style.SQL_KEYWORD('TO'))
                sql.append(query)
            return sql
        else:
            return []

    def drop_sequence_sql(self, table):
        return 'DROP GENERATOR %s' % self.get_generator_name(table)

    def get_generator_name(self, table_name):
        return get_autoinc_sequence_name(self, table_name)

def get_autoinc_sequence_name(ops, table):
    return ops.quote_name('%s_SQ' % util.truncate_name(table, ops.max_name_length() - 3))

def get_autoinc_trigger_name(ops, table):
    return ops.quote_name('%s_PK' % util.truncate_name(table, ops.max_name_length() - 3))

def get_reset_procedure_name(ops, table):
    return ops.quote_name('%s_RS' % util.truncate_name(table, ops.max_name_length() - 3))
