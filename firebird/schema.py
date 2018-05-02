import logging
import datetime

from django.utils import six
from django.utils.encoding import force_str
from django.db.models import Index
from django.db.models.fields import AutoField
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.base.schema import _related_non_m2m_objects

logger = logging.getLogger('django.db.backends.schema')


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_rename_table = "Rename table is not allowed"  # Not supported
    sql_delete_table = "DROP TABLE %(table)s;"
    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_alter_column_type = "ALTER %(column)s TYPE %(type)s"
    sql_delete_column = "ALTER TABLE %(table)s DROP %(column)s"
    sql_rename_column = "ALTER TABLE %(table)s ALTER %(old_column)s TO %(new_column)s"
    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s)"

    def _alter_column_set_null(self, table_name, column_name, is_null):
        sql = """
            UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = %(null_flag)s
            WHERE RDB$FIELD_NAME = '%(column)s'
            AND RDB$RELATION_NAME = '%(table_name)s'
        """
        null_flag = 'NULL' if is_null else '1'
        return sql % {
            'null_flag': null_flag,
            'column': column_name.upper(),
            'table_name': table_name.upper()
        }

    def _column_has_default(self, params):
        sql = """
            SELECT a.RDB$DEFAULT_VALUE
            FROM RDB$RELATION_FIELDS a
            WHERE UPPER(a.RDB$FIELD_NAME) = UPPER('%(column)s')
            AND UPPER(a.RDB$RELATION_NAME) = UPPER('%(table_name)s')
        """
        value = None
        with self.connection.cursor() as cursor:
            value = cursor.execute(sql)
        return True if value else False

    def add_field(self, model, field):
        """
        Creates a field on a model.
        Usually involves adding a column, but may involve adding a
        table instead (for M2M fields)
        """
        # Special-case implicit M2M tables
        if field.many_to_many and field.remote_field.through._meta.auto_created:
            return self.create_model(field.remote_field.through)
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
        if not self.skip_default(field) and self.effective_default(field) is not None:
            params = {'table_name': model._meta.db_table, 'column': field.column}
            # Firebird need to check if the column has default definition after change it.
            if self._column_has_default(params):
                sql = self.sql_alter_column % {
                    "table": self.quote_name(model._meta.db_table),
                    "changes": self.sql_alter_column_no_default % {
                        "column": self.quote_name(field.column),
                    }
                }
                self.execute(sql)
        # Add an index, if required
        self.deferred_sql.extend(self._field_indexes_sql(model, field))
        # Add any FK constraints later
        if field.remote_field and self.connection.features.supports_foreign_keys and field.db_constraint:
            self.deferred_sql.append(self._create_fk_sql(model, field, "_fk_%(to_table)s_%(to_column)s"))
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.commit()

    def _field_should_be_indexed(self, model, field):
        create_index = super(DatabaseSchemaEditor, self)._field_should_be_indexed(model, field)

        # No need to create an index for ForeignKey fields except if
        # db_constraint=False because the index from that constraint won't be
        # created.
        if (create_index and field.get_internal_type() == 'ForeignKey' and field.db_constraint):
            return False
        return create_index

    def _get_field_indexes(self, model, field):
        with self.connection.cursor() as cursor:
            indexes = self.connection.introspection._get_field_indexes(cursor, model._meta.db_table, field.column)
        return indexes

    def remove_field(self, model, field):
        # If remove a AutoField, we need remove all related stuff
        # if isinstance(field, AutoField):
        if field.get_internal_type() in ("AutoField", "BigAutoField"):
            tbl = model._meta.db_table
            trg_name = self.connection.ops.get_sequence_trigger_name(tbl)
            self.execute('DROP TRIGGER %s' % trg_name)
            seq_name = self.connection.ops.get_sequence_name(tbl)
            self.execute('DROP SEQUENCE %s' % seq_name)

        # If 'field' is a ForeingKey and It has defined extra indexes, delete that indexes (Github issue #70)
        for index_name in self._get_field_indexes(model, field):
            sql = self._delete_constraint_sql(self.sql_delete_index, model, index_name)
            self.execute(sql)

        super(DatabaseSchemaEditor, self).remove_field(model, field)

    def _alter_column_type_sql(self, table, old_field, new_field, new_type):
        if old_field.unique:
            # In Firebird, alter a column type with a unique constraint will fails
            # SQL Message : -607, Engine Code : 335544351
            # So, we need delete the unique constraint first, alter the column and
            # then create the constraint again.

            # delete unique constraint and generate sql to recreate later
            extra_sql = []
            model = old_field.model
            column = self.quote_name(old_field.column)
            unq_names = self._constraint_names(old_field.model, [old_field.column], unique=True)
            for name in unq_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_unique, model, name))
                params = {"table": table, "name": name, "columns": self.quote_name(column)}
                extra_sql.append((self.sql_create_unique % params, [],))

            # alter column type
            params = {"column": self.quote_name(new_field.column), "type": new_type}
            alter_sql = self.sql_alter_column_type % params
            return ((alter_sql, [],), extra_sql,)

        return super(DatabaseSchemaEditor, self)._alter_column_type_sql(table, old_field, new_field, new_type)

    def _alter_field(self, model, old_field, new_field, old_type, new_type,
                     old_db_params, new_db_params, strict=False):
        """Actually perform a "physical" (non-ManyToMany) field update."""

        # Drop any FK constraints, we'll remake them later
        fks_dropped = set()
        if old_field.remote_field and old_field.db_constraint:
            fk_names = self._constraint_names(model, [old_field.column], foreign_key=True)
            if strict and len(fk_names) != 1:
                raise ValueError("Found wrong number (%s) of foreign key constraints for %s.%s" % (
                    len(fk_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for fk_name in fk_names:
                fks_dropped.add((old_field.column,))
                self.execute(self._delete_constraint_sql(self.sql_delete_fk, model, fk_name))
        # Has unique been removed?
        if old_field.unique and (not new_field.unique or (not old_field.primary_key and new_field.primary_key)):
            # Find the unique constraint for this field
            constraint_names = self._constraint_names(model, [old_field.column], unique=True)
            if strict and len(constraint_names) != 1:
                raise ValueError("Found wrong number (%s) of unique constraints for %s.%s" % (
                    len(constraint_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_unique, model, constraint_name))
        # Drop incoming FK constraints if the field is a primary key or unique,
        # which might be a to_field target, and things are going to change.
        drop_foreign_keys = (
            (
                (old_field.primary_key and new_field.primary_key) or
                (old_field.unique and new_field.unique)
            ) and old_type != new_type
        )
        if drop_foreign_keys:
            # '_meta.related_field' also contains M2M reverse fields, these
            # will be filtered out
            for _old_rel, new_rel in _related_non_m2m_objects(old_field, new_field):
                rel_fk_names = self._constraint_names(
                    new_rel.related_model, [new_rel.field.column], foreign_key=True
                )
                for fk_name in rel_fk_names:
                    self.execute(self._delete_constraint_sql(self.sql_delete_fk, new_rel.related_model, fk_name))
        # Removed an index? (no strict check, as multiple indexes are possible)
        # Remove indexes if db_index switched to False or a unique constraint
        # will now be used in lieu of an index. The following lines from the
        # truth table show all True cases; the rest are False:
        #
        # old_field.db_index | old_field.unique | new_field.db_index | new_field.unique
        # ------------------------------------------------------------------------------
        # True               | False            | False              | False
        # True               | False            | False              | True
        # True               | False            | True               | True
        if old_field.db_index and not old_field.unique and (not new_field.db_index or new_field.unique):
            # Find the index for this field
            meta_index_names = {index.name for index in model._meta.indexes}
            # Retrieve only BTREE indexes since this is what's created with
            # db_index=True.
            index_names = self._constraint_names(model, [old_field.column], index=True, type_=Index.suffix)
            for index_name in index_names:
                if index_name in meta_index_names:
                    # The only way to check if an index was created with
                    # db_index=True or with Index(['field'], name='foo')
                    # is to look at its name (refs #28053).
                    continue
                self.execute(self._delete_constraint_sql(self.sql_delete_index, model, index_name))
        # Change check constraints?
        if self.connection.features.supports_column_check_constraints:
            if old_db_params['check'] != new_db_params['check'] and old_db_params['check']:
                constraint_names = self._constraint_names(model, [old_field.column], check=True)
                if strict and len(constraint_names) != 1:
                    raise ValueError("Found wrong number (%s) of check constraints for %s.%s" % (
                        len(constraint_names),
                        model._meta.db_table,
                        old_field.column,
                    ))
                for constraint_name in constraint_names:
                    self.execute(self._delete_constraint_sql(self.sql_delete_check, model, constraint_name))
        # Have they renamed the column?
        if old_field.column != new_field.column:
            self.execute(self._rename_field_sql(model._meta.db_table, old_field, new_field, new_type))
        # Next, start accumulating actions to do
        actions = []
        null_actions = []
        post_actions = []
        # Type change?
        if old_type != new_type:
            fragment, other_actions = self._alter_column_type_sql(
                model._meta.db_table, old_field, new_field, new_type
            )
            actions.append(fragment)
            post_actions.extend(other_actions)
        # When changing a column NULL constraint to NOT NULL with a given
        # default value, we need to perform 4 steps:
        #  1. Add a default for new incoming writes
        #  2. Update existing NULL rows with new default
        #  3. Replace NULL constraint with NOT NULL
        #  4. Drop the default again.
        # Default change?
        old_default = self.effective_default(old_field)
        new_default = self.effective_default(new_field)
        needs_database_default = (
            old_default != new_default and
            new_default is not None and
            not self.skip_default(new_field)
        )
        if needs_database_default:
            if self.connection.features.requires_literal_defaults:
                # Some databases can't take defaults as a parameter (oracle)
                # If this is the case, the individual schema backend should
                # implement prepare_default
                actions.append((
                    self.sql_alter_column_default % {
                        "column": self.quote_name(new_field.column),
                        "type": new_type,
                        "default": self.prepare_default(new_default),
                    },
                    [],
                ))
            else:
                actions.append((
                    self.sql_alter_column_default % {
                        "column": self.quote_name(new_field.column),
                        "type": new_type,
                        "default": "%s",
                    },
                    [new_default],
                ))
        # Nullability change?
        if old_field.null != new_field.null:
            sql_null = self._alter_column_set_null(
                model._meta.db_table,
                new_field.column,
                new_field.null
            )
            post_actions.append((sql_null, [],))

        # Only if we have a default and there is a change from NULL to NOT NULL
        four_way_default_alteration = (
            new_field.has_default() and
            (old_field.null and not new_field.null)
        )
        if actions or null_actions:
            if not four_way_default_alteration:
                # If we don't have to do a 4-way default alteration we can
                # directly run a (NOT) NULL alteration
                actions = actions + null_actions
            # Combine actions together if we can (e.g. postgres)
            if self.connection.features.supports_combined_alters and actions:
                sql, params = tuple(zip(*actions))
                actions = [(", ".join(sql), sum(params, []))]
            # Apply those actions
            for sql, params in actions:
                self.execute(
                    self.sql_alter_column % {
                        "table": self.quote_name(model._meta.db_table),
                        "changes": sql,
                    },
                    params,
                )
            if four_way_default_alteration:
                # Update existing rows with default value
                self.execute(
                    self.sql_update_with_default % {
                        "table": self.quote_name(model._meta.db_table),
                        "column": self.quote_name(new_field.column),
                        "default": "%s",
                    },
                    [new_default],
                )
                # Since we didn't run a NOT NULL change before we need to do it
                # now
                for sql, params in null_actions:
                    self.execute(
                        self.sql_alter_column % {
                            "table": self.quote_name(model._meta.db_table),
                            "changes": sql,
                        },
                        params,
                    )
        if post_actions:
            for sql, params in post_actions:
                self.execute(sql, params)
        # Added a unique?
        if (not old_field.unique and new_field.unique) or (
            old_field.primary_key and not new_field.primary_key and new_field.unique
        ):
            self.execute(self._create_unique_sql(model, [new_field.column]))
        # Added an index? Add an index if db_index switched to True or a unique
        # constraint will no longer be used in lieu of an index. The following
        # lines from the truth table show all True cases; the rest are False:
        #
        # old_field.db_index | old_field.unique | new_field.db_index | new_field.unique
        # ------------------------------------------------------------------------------
        # False              | False            | True               | False
        # False              | True             | True               | False
        # True               | True             | True               | False
        if (not old_field.db_index or old_field.unique) and new_field.db_index and not new_field.unique:
            # If the new field is a foreign key not index is necessary because Firebird create it implicitly
            # This behavior is related to Github issue #70
            # original -->  self.execute(self._create_index_sql(model, [new_field]))
            self.deferred_sql.extend(self._field_indexes_sql(model, new_field))
        # Type alteration on primary key? Then we need to alter the column
        # referring to us.
        rels_to_update = []
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            rels_to_update.extend(_related_non_m2m_objects(old_field, new_field))
        # Changed to become primary key?
        # Note that we don't detect unsetting of a PK, as we assume another field
        # will always come along and replace it.
        if not old_field.primary_key and new_field.primary_key:
            # First, drop the old PK
            constraint_names = self._constraint_names(model, primary_key=True)
            if strict and len(constraint_names) != 1:
                raise ValueError("Found wrong number (%s) of PK constraints for %s" % (
                    len(constraint_names),
                    model._meta.db_table,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_pk, model, constraint_name))
            # Make the new one
            self.execute(
                self.sql_create_pk % {
                    "table": self.quote_name(model._meta.db_table),
                    "name": self.quote_name(self._create_index_name(model, [new_field.column], suffix="_pk")),
                    "columns": self.quote_name(new_field.column),
                }
            )
            # Update all referencing columns
            rels_to_update.extend(_related_non_m2m_objects(old_field, new_field))
        # Handle our type alters on the other end of rels from the PK stuff above
        for old_rel, new_rel in rels_to_update:
            rel_db_params = new_rel.field.db_parameters(connection=self.connection)
            rel_type = rel_db_params['type']
            fragment, other_actions = self._alter_column_type_sql(
                new_rel.related_model._meta.db_table, old_rel.field, new_rel.field, rel_type
            )
            self.execute(
                self.sql_alter_column % {
                    "table": self.quote_name(new_rel.related_model._meta.db_table),
                    "changes": fragment[0],
                },
                fragment[1],
            )
            for sql, params in other_actions:
                self.execute(sql, params)
        # Does it have a foreign key?
        if (new_field.remote_field and
                (fks_dropped or not old_field.remote_field or not old_field.db_constraint) and
                new_field.db_constraint):
            self.execute(self._create_fk_sql(model, new_field, "_fk_%(to_table)s_%(to_column)s"))
        # Rebuild FKs that pointed to us if we previously had to drop them
        if drop_foreign_keys:
            for rel in new_field.model._meta.related_objects:
                if _is_relevant_relation(rel, new_field) and rel.field.db_constraint:
                    self.execute(self._create_fk_sql(rel.related_model, rel.field, "_fk"))
        # Does it have check constraints we need to add?
        if old_db_params['check'] != new_db_params['check'] and new_db_params['check']:
            self.execute(
                self.sql_create_check % {
                    "table": self.quote_name(model._meta.db_table),
                    "name": self.quote_name(self._create_index_name(model, [new_field.column], suffix="_check")),
                    "column": self.quote_name(new_field.column),
                    "check": new_db_params['check'],
                }
            )
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        if needs_database_default:
            sql = self.sql_alter_column % {
                "table": self.quote_name(model._meta.db_table),
                "changes": self.sql_alter_column_no_default % {
                    "column": self.quote_name(new_field.column),
                    "type": new_type,
                }
            }
            self.execute(sql)
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.commit()

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
            except Exception as e:
                logger.info(str(e))
                pass

    def sequence_exist(self, table):
        seq_name = self.connection.ops.get_sequence_name(table)
        sql = """
        SELECT RDB$GENERATOR_ID
        FROM RDB$GENERATORS
        WHERE RDB$GENERATOR_NAME = %s
        """ % seq_name
        value = None
        with self.connection.cursor() as cursor:
            value = cursor.execute(sql)
        return True if value else False

    def execute(self, sql, params=[]):
        """
        Executes the given SQL statement, with optional parameters.
        """
        # print("schema:", sql)
        super(DatabaseSchemaEditor, self).execute(sql, params)
