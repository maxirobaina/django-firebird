import datetime

from django.utils import six
from django.utils.encoding import force_str
from django.db.models.fields import AutoField
from django.db.backends.schema import BaseDatabaseSchemaEditor
from django.db.backends.schema import logger
from django.db.models.fields.related import ManyToManyField


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_rename_table = ""  # Not supported
    sql_delete_table = "DROP TABLE %(table)s;"

    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_alter_column_type = "ALTER %(column)s TYPE %(type)s"
    sql_delete_column = "ALTER TABLE %(table)s DROP %(column)s"
    sql_rename_column = "ALTER TABLE %(table)s ALTER %(old_column)s TO %(new_column)s"

    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s)"

    def column_sql(self, model, field, include_default=False):
        """
        Takes a field and returns its column definition.
        The field must already have had set_attributes_from_name called.
        """
        # Get the column's type and use that as the basis of the SQL
        db_params = field.db_parameters(connection=self.connection)
        sql = db_params['type']
        params = []
        # Check for fields that aren't actually columns (e.g. M2M)
        if sql is None:
            return None, None
        # Work out nullability
        null = field.null
        # If we were told to include a default value, do so
        default_value = self.effective_default(field)
        if include_default and default_value is not None:
            if self.connection.features.requires_literal_defaults:
                # Some databases can't take defaults as a parameter (oracle)
                # If this is the case, the individual schema backend should
                # implement prepare_default
                sql += " DEFAULT %s" % self.prepare_default(default_value)
            else:
                sql += " DEFAULT %s"
                params += [default_value]
        # Oracle treats the empty string ('') as null, so coerce the null
        # option whenever '' is a possible value.
        if (field.empty_strings_allowed and not field.primary_key and
                self.connection.features.interprets_empty_strings_as_nulls):
            null = True
        if not null:
            sql += " NOT NULL"
        # Primary key/unique outputs
        if field.primary_key:
            sql += " PRIMARY KEY"
        elif field.unique:
            sql += " UNIQUE"
        # Optionally add the tablespace if it's an implicitly indexed column
        tablespace = field.db_tablespace or model._meta.db_tablespace
        if tablespace and self.connection.features.supports_tablespaces and field.unique:
            sql += " %s" % self.connection.ops.tablespace_sql(tablespace, inline=True)
        # Return the sql
        return sql, params

    def _column_has_default(self, params):
        sql = """
            SELECT a.RDB$DEFAULT_VALUE
            FROM RDB$RELATION_FIELDS a
            WHERE UPPER(a.RDB$FIELD_NAME) = UPPER('%(column)s')
            AND UPPER(a.RDB$RELATION_NAME) = UPPER('%(table_name)s')
        """
        value = self.execute(sql % params)
        return True if value else False

    def add_field(self, model, field):
        """
        Creates a field on a model.
        Usually involves adding a column, but may involve adding a
        table instead (for M2M fields)
        """
        # Special-case implicit M2M tables
        if isinstance(field, ManyToManyField) and field.rel.through._meta.auto_created:
            return self.create_model(field.rel.through)
        # Get the column's definition
        definition, params = self.column_sql(model, field, include_default=True)
        # It might not actually have a column behind it
        if definition is None:
            return
        # Check constraints can go on the column SQL here
        db_params = field.db_parameters(connection=self.connection)
        if db_params['check']:
            definition += " CHECK (%s)" % db_params['check']
        # Build the SQL and run it
        sql = self.sql_create_column % {
            "table": self.quote_name(model._meta.db_table),
            "column": self.quote_name(field.column),
            "definition": definition,
        }
        self.execute(sql, params)
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        if field.default is not None:
            params = {'table_name': model._meta.db_table, 'column': field.column}
            if self._column_has_default(params):
                sql = self.sql_alter_column % {
                    "table": self.quote_name(model._meta.db_table),
                    "changes": self.sql_alter_column_no_default % {
                        "column": self.quote_name(field.column),
                    }
                }
                self.execute(sql)
        # Add an index, if required
        if field.db_index and not field.unique:
            self.deferred_sql.append(
                self.sql_create_index % {
                    "name": self._create_index_name(model, [field.column], suffix=""),
                    "table": self.quote_name(model._meta.db_table),
                    "columns": self.quote_name(field.column),
                    "extra": "",
                }
            )
        # Add any FK constraints later
        if field.rel and self.connection.features.supports_foreign_keys:
            to_table = field.rel.to._meta.db_table
            to_column = field.rel.to._meta.get_field(field.rel.field_name).column
            self.deferred_sql.append(
                self.sql_create_fk % {
                    "name": self.quote_name('%s_refs_%s_%x' % (
                        field.column,
                        to_column,
                        abs(hash((model._meta.db_table, to_table)))
                    )),
                    "table": self.quote_name(model._meta.db_table),
                    "column": self.quote_name(field.column),
                    "to_table": self.quote_name(to_table),
                    "to_column": self.quote_name(to_column),
                }
            )
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()

    def remove_field(self, model, field):
        # If remove a AutoField, we need remove all related stuff
        if isinstance(field, AutoField):
            tbl = model._meta.db_table
            trg_name = self.connection.ops.get_sequence_trigger_name(tbl)
            self.execute('DROP TRIGGER %s' % trg_name)
            seq_name = self.connection.ops.get_sequence_name(tbl)
            self.execute('DROP SEQUENCE %s' % seq_name)
        
        super(DatabaseSchemaEditor, self).remove_field(model, field)

    def prepare_default(self, value):
        if isinstance(value, bool):
            return "1" if value else "0"
        s = force_str(value)
        return self.quote_value(s)

    def quote_value(self, value):
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return "'%s'" % value
        elif isinstance(value, six.string_types):
            return repr(value)
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif value is None:
            return "NULL"
        else:
            return force_str(value)

    def delete_model(self, model):
        super(DatabaseSchemaEditor, self).delete_model(model)

        # Also, drop sequence if exists
        table_name = model._meta.db_table
        sql = self.connection.ops.drop_sequence_sql(table_name)
        if sql:
            try:
                self.execute(sql)
            except:
                pass

    def sequence_exist(self, table):
        seq_name = self.connection.ops.get_sequence_name(table)
        sql = """
        SELECT RDB$GENERATOR_ID
        FROM RDB$GENERATORS
        WHERE RDB$GENERATOR_NAME = %s
        """
        value = None
        with self.connection.cursor() as cursor:
            value = cursor.execute(sql, params)
        return True if value else False

    def execute(self, sql, params=[]):
        """
        Executes the given SQL statement, with optional parameters.
        """
        print("schema:", sql)
        # Log the command we're running, then run it
        logger.debug("%s; (params %r)" % (sql, params))
        if self.collect_sql:
            self.collected_sql.append((sql % tuple(map(self.quote_value, params))) + ";")
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
            self.connection.commit()
