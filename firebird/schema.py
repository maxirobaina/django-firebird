import logging
import datetime

from django.db.backends.ddl_references import Statement, Table, IndexName, IndexColumns, TableColumns
from django.utils import six
from django.utils.encoding import force_str
from django.db.models import Index
from django.db.models.fields import AutoField, CharField
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.base.schema import _related_non_m2m_objects, _is_relevant_relation

from fdb import TransactionContext

logger = logging.getLogger('django.db.backends.schema')


class FirebirdColumns(TableColumns):
    """Hold a reference to one or many columns."""

    def __init__(self, table, columns, quote_name, col_suffixes=()):
        self.quote_name = quote_name
        self.col_suffixes = col_suffixes
        super().__init__(table, columns)

    def __str__(self):
        def col_str(column, idx):
            return self.quote_name(column)

        return ', '.join(col_str(column, idx) for idx, column in enumerate(self.columns))


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_rename_table = "Rename table is not allowed"  # Not supported
    sql_delete_table = "DROP TABLE %(table)s;"
    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_alter_column_type = "ALTER %(column)s TYPE %(type)s"
    sql_delete_column = "ALTER TABLE %(table)s DROP %(column)s"
    sql_rename_column = "ALTER TABLE %(table)s ALTER %(old_column)s TO %(new_column)s"
    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s)"

    # Important!!!
    # If an index is created or a unique on a large VARCHAR field, the expression with hash function is used
    sql_create_hash_index = "CREATE INDEX %(name)s ON %(table)s computed by(hash(%(columns)s))"
    sql_create_unique_hash_index = "CREATE UNIQUE INDEX %(name)s ON %(table)s computed by(hash(%(columns)s))"

    def _alter_column_set_null(self, table_name, column_name, is_null):
        engine_ver = str(self.connection.connection.engine_version).split('.')
        if engine_ver and len(engine_ver) > 0 and int(engine_ver[0]) >= 3:
            sql = """
                ALTER TABLE \"%(table_name)s\" 
                ALTER \"%(column)s\" 
                %(null_flag)s NOT NULL
            """
            null_flag = 'DROP' if is_null else 'SET'
        else:
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
        """ % {
            'column': params['column'],
            'table_name': params['table_name']
        }

        res = None
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchone()
        return True if res else False

    def add_index(self, model, index):
        """
        Add an index on a model.

        Args:
            model (Model): Model of table
            index (Index): Index for creation

        .. important::

           Firebird does not support creating indexes to big VARCHAR fields,
           so an expression with hash function is used when creation error.
           https://firebirdsql.org/refdocs/langrefupd20-create-index.html#langrefupd20-creatind-keylength

        Note:
           If there is an error when create index, we try to create index with hash.
        """
        create_statement = None
        try:
            create_statement = index.create_sql(model, self)
            self.execute(create_statement, params=None)
        except Exception as e:
            # If the creation of the index failed with
            # the error 'key size too big for index',
            # then create an index with hash expression
            create_statement.template = self.sql_create_hash_index
            self.execute(create_statement, params=None)

    def alter_unique_together(self, model, old_unique_together, new_unique_together):
        """
        Deal with a model changing its unique_together. The input
        unique_togethers must be doubly-nested, not the single-nested
        ["foo", "bar"] format.
        """
        olds = {tuple(fields) for fields in old_unique_together}
        news = {tuple(fields) for fields in new_unique_together}
        # Deleted uniques
        for fields in olds.difference(news):
            try:
                self._delete_composed_index(model, fields, {'unique': True}, self.sql_delete_unique)
            except Exception as e:
                with self.connection.cursor() as cursor:
                    constraints = self.connection.introspection.get_constraints(cursor, model._meta.db_table)
                result = None
                expression = ('(hash(%s))' % ' || '.join(self.quote_name(column) for column in fields)).lower()
                for name, infodict in constraints.items():
                    if infodict['expression_source'] == expression:
                        if infodict['unique'] and infodict['index']:
                            result = name
                            break
                self.execute(self._delete_constraint_sql(self.sql_delete_index, model, result))
        # Created uniques
        for fields in news.difference(olds):
            columns = [model._meta.get_field(field).column for field in fields]
            create_statement = self._create_unique_sql(model, columns)
            self.create_unique(create_statement)

    def create_unique(self, create_statement):
        """
        Creates an unique constraint by executing a statement

        Args:
            create_statement (Statement): Statement to execute

        .. important::

           Firebird does not support creating uniques to big VARCHAR fields,
           so an expression with hash function is used when creation error.
           https://firebirdsql.org/refdocs/langrefupd20-create-index.html#langrefupd20-creatind-keylength

        Note:
           If there is an error when create unique, we try to create unique with hash.
        """
        try:
            self.execute(create_statement)
        except Exception as e:
            # If the creation of the unique failed with
            # the error 'key size too big for index',
            # then create an unique index with hash expression
            cols = ' || '.join(self.quote_name(column) for column in create_statement.parts['columns'].columns)
            create_statement.template = self.sql_create_unique_hash_index
            create_statement.parts['columns'] = cols
            self.execute(create_statement, params=None)

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Rename the table a model points to."""
        if (old_db_table == new_db_table or
            (self.connection.features.ignores_table_name_case and
                old_db_table.lower() == new_db_table.lower())):
            return
        # Not supported yet
        return

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
        # The Firebird does not support direct type change to BLOB,
        # therefore this action is divided into 4 stages.
        # Change through temp column.
        alter_blob_actions = []
        if new_type == self.connection.data_types['TextField'] or new_type == self.connection.data_types['BinaryField']:
            alter_blob_actions.append(
                (self.sql_create_column % {
                    "table": self.quote_name(table),
                    "column": self.quote_name("mirgate_temp_" + new_field.column),
                    "definition": new_type,
                }, [])
            )
            alter_blob_actions.append(
                (self.sql_update_with_default % {
                    "table": self.quote_name(table),
                    "column": self.quote_name("mirgate_temp_" + new_field.column),
                    "default": self.quote_name(old_field.column),
                }, [])
            )
            alter_blob_actions.append(
                (self.sql_delete_column % {
                    "table": self.quote_name(table),
                    "column": self.quote_name(old_field.column),
                }, [])
            )
            alter_blob_actions.append(
                (self.sql_rename_column % {
                    "table": self.quote_name(table),
                    "old_column": self.quote_name("mirgate_temp_" + new_field.column),
                    "new_column": self.quote_name(new_field.column),
                }, [])
            )

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

            if new_type != self.connection.data_types['TextField'] or new_type == self.connection.data_types[
                'BinaryField']:
                # alter column type
                params = {"column": self.quote_name(new_field.column), "type": new_type}
                alter_sql = self.sql_alter_column_type % params
                return ((alter_sql, [],), extra_sql,)
        if new_type != self.connection.data_types['TextField'] and new_type != self.connection.data_types[
            'BinaryField']:
            return super(DatabaseSchemaEditor, self)._alter_column_type_sql(table, old_field, new_field, new_type)
        else:
            return ((alter_blob_actions), [],)

    def _alter_column_blob_type_sql(self, table, old_field, new_field, new_type):
        # The Firebird does not support direct type change from BLOB,
        # therefore this action is divided into 4 stages.
        # Change through temp column.
        alter_blob_actions = [(self.sql_create_column % {
            "table": self.quote_name(table),
            "column": self.quote_name("mirgate_temp_" + new_field.column),
            "definition": new_type,
        }, []), (self.sql_update_with_default % {
            "table": self.quote_name(table),
            "column": self.quote_name("mirgate_temp_" + new_field.column),
            "default": self.quote_name(old_field.column),
        }, []), (self.sql_delete_column % {
            "table": self.quote_name(table),
            "column": self.quote_name(old_field.column),
        }, []), (self.sql_rename_column % {
            "table": self.quote_name(table),
            "old_column": self.quote_name("mirgate_temp_" + new_field.column),
            "new_column": self.quote_name(new_field.column),
        }, [])]
        return ((alter_blob_actions), [],)

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
        fragment = None
        # Type change?
        if old_type != new_type:
            fragment, other_actions = self._alter_column_type_sql(
                model._meta.db_table, old_field, new_field, new_type
            )
            # If old type is blob, then we have to make new field in 4 steps
            if old_type == self.connection.data_types['TextField'] \
                    or old_type == self.connection.data_types['BinaryField']:
                fragment, other_actions = self._alter_column_blob_type_sql(
                    model._meta.db_table, old_field, new_field, new_type
                )

            # If new type is blob or old type was blob, then fragment contains 4 action
            if isinstance(fragment, list):
                [actions.append(el) for el in fragment]
            else:
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
                # The Firebird does not support direct type change to/from BLOB.
                # Need to execute 4 action in addition to alter.
                if isinstance(fragment, list):
                    self.execute(sql, params)
                else:
                    self.execute(
                        self.sql_alter_column % {
                            "table": self.quote_name(model._meta.db_table),
                            "changes": sql,
                        },
                        params,
                    )
            if four_way_default_alteration:
                # Update existing rows with default value

                # Some databases can't take defaults as a parameter (oracle)
                # If this is the case, the individual schema backend should
                # implement prepare_default
                if self.connection.features.requires_literal_defaults:
                    self.execute(
                        self.sql_update_with_default % {
                            "table": self.quote_name(model._meta.db_table),
                            "column": self.quote_name(new_field.column),
                            "default": self.prepare_default(new_default),
                        },
                        [],
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
            # self.execute(self._create_unique_sql(model, [new_field.column]))
            self.create_unique(self._create_unique_sql(model, [new_field.column]))
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

    def _create_index_sql(self, model, fields, *, name=None, suffix='', using='',
                          db_tablespace=None, col_suffixes=(), sql=None, opclasses=(),
                          condition=None):
        """
        Return the SQL statement to create the index for one or several fields.
        `sql` can be specified if the syntax differs from the standard (GIS
        indexes, ...).
        """
        tablespace_sql = self._get_index_tablespace_sql(model, fields, db_tablespace=db_tablespace)
        columns = [field.column for field in fields]
        sql_create_index = sql or self.sql_create_index
        table = model._meta.db_table

        def create_index_name(*args, **kwargs):
            nonlocal name
            if name is None:
                name = self._create_index_name(*args, **kwargs)
            return self.quote_name(name)

        return Statement(
            sql_create_index,
            table=Table(table, self.quote_name),
            name=IndexName(table, columns, suffix, create_index_name),
            using=using,
            columns=self._index_columns(table, columns, col_suffixes, opclasses),
            extra=tablespace_sql,
            condition=(' WHERE ' + condition) if condition else '',
        )

    def _index_columns(self, table, columns, col_suffixes, opclasses):
        if opclasses:
            return IndexColumns(table, columns, self.quote_name, col_suffixes=col_suffixes, opclasses=opclasses)
        return FirebirdColumns(table, columns, self.quote_name, col_suffixes)

    def prepare_default(self, value):
        # If the major server version is less than 3 then use `smallint` for the boolean field
        if isinstance(value, bool) and int(self.connection.ops.firebird_version[3]) < 3:
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

    # Actions

    def create_model(self, model):
        """
        Create a table and any accompanying indexes or unique constraints for
        the given `model`.
        """
        # Create column SQL, add FK deferreds if needed
        column_sqls = []
        params = []
        for field in model._meta.local_fields:
            if isinstance(field, CharField) and field.max_length > 32765:
                field.max_length = 8191 # max for 4-byte character sets
            # SQL
            definition, extra_params = self.column_sql(model, field)
            if definition is None:
                continue
            # Check constraints can go on the column SQL here
            db_params = field.db_parameters(connection=self.connection)
            if db_params['check']:
                definition += " " + self.sql_check_constraint % db_params
            # Autoincrement SQL (for backends with inline variant)
            col_type_suffix = field.db_type_suffix(connection=self.connection)
            if col_type_suffix:
                definition += " %s" % col_type_suffix
            params.extend(extra_params)
            # FK
            if field.remote_field and field.db_constraint:
                to_table = field.remote_field.model._meta.db_table
                to_column = field.remote_field.model._meta.get_field(field.remote_field.field_name).column
                if self.sql_create_inline_fk:
                    definition += " " + self.sql_create_inline_fk % {
                        "to_table": self.quote_name(to_table),
                        "to_column": self.quote_name(to_column),
                    }
                elif self.connection.features.supports_foreign_keys:
                    self.deferred_sql.append(self._create_fk_sql(model, field, "_fk_%(to_table)s_%(to_column)s"))
            # Add the SQL to our big list
            column_sqls.append("%s %s" % (
                self.quote_name(field.column),
                definition,
            ))
            # Autoincrement SQL (for backends with post table definition variant)
            if field.get_internal_type() in ("AutoField", "BigAutoField"):
                autoinc_sql = self.connection.ops.autoinc_sql(model._meta.db_table, field.column)
                if autoinc_sql:
                    self.deferred_sql.extend(autoinc_sql)

        constraints = [constraint.constraint_sql(model, self) for constraint in model._meta.constraints]
        # Make the table
        sql = self.sql_create_table % {
            "table": self.quote_name(model._meta.db_table),
            "definition": ", ".join(constraint for constraint in (*column_sqls, *constraints) if constraint),
        }
        if model._meta.db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(model._meta.db_tablespace)
            if tablespace_sql:
                sql += ' ' + tablespace_sql
        # Prevent using [] as params, in the case a literal '%' is used in the definition
        self.execute(sql, params or None)

        # Add any unique_togethers (always deferred, as some fields might be
        # created afterwards, like geometry fields with some backends)
        for fields in model._meta.unique_together:
            columns = [model._meta.get_field(field).column for field in fields]
            # self.deferred_sql.append(self._create_unique_sql(model, columns))
            self.create_unique(self._create_unique_sql(model, columns))

        # Add any field index and index_together's (deferred as SQLite _remake_table needs it)
        self._model_indexes_sql(model)

        # Make M2M tables
        for field in model._meta.local_many_to_many:
            if field.remote_field.through._meta.auto_created:
                self.create_model(field.remote_field.through)

    def _model_indexes_sql(self, model):
        """
        Return a list of all index SQL statements (field indexes,
        index_together, Meta.indexes) for the specified model.
        """
        if not model._meta.managed or model._meta.proxy or model._meta.swapped:
            return []
        output = []
        for field in model._meta.local_fields:
            self._field_indexes_sql(model, field)

        for field_names in model._meta.index_together:
            fields = [model._meta.get_field(field) for field in field_names]
            create_statement = self._create_index_sql(model, fields, suffix="_idx")
            self.create_index(create_statement)

        for index in model._meta.indexes:
            self.add_index(self, model, index)
        return output

    def _field_indexes_sql(self, model, field):
        """
        Return a list of all index SQL statements for the specified field.
        """
        output = []
        if self._field_should_be_indexed(model, field):
            create_statement = self._create_index_sql(model, [field])
            self.create_index(create_statement)
        return output

    def create_index(self, statement):
        try:
            self.execute(statement, params=None)
        except Exception as e:
            # If the creation of the index failed with
            # the error 'key size too big for index',
            # then create an index with hash expression
            statement.template = self.sql_create_hash_index
            self.execute(statement, params=None)

    def delete_model(self, model):
        super(DatabaseSchemaEditor, self).delete_model(model)

        # Also, drop sequence if exists
        table_name = model._meta.db_table
        if not self.sequence_exist(table_name):
            return

        sql = self.connection.ops.drop_sequence_sql(table_name)
        if sql:
            try:
                self.execute(sql)
            except Exception as e:
                logger.info(str(e))
                pass

    def sequence_exist(self, table):
        seq_name = str(self.connection.ops.get_sequence_name(table)).replace("\"", "\'")
        sql = """
        SELECT RDB$GENERATOR_ID
        FROM RDB$GENERATORS
        WHERE RDB$GENERATOR_NAME = %s
        """ % seq_name
        value = None
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            value = cursor.fetchone()
        return True if value else False

    def execute(self, sql, params=[]):
        """
        Executes the given SQL statement, with optional parameters.

        Args:
            sql (str): SQL query to execute
            params (list): list of parameters

        .. important::

            If DDL and DML statements are executed in one transaction,
            then DML statements will not see the changes of the DDL statements.
            It is necessary to commit all previous statements so that
            follow statements can see changes previous ones.

        Note:
           This requires that `DatabaseFeatures.autocommits_when_autocommit_is_off` feature is True
        """
        # print("schema:", sql)

        if self.connection.features.autocommits_when_autocommit_is_off:
            for tr in self.connection.connection.transactions:
                if tr.active:
                    tr.commit()
            # TransactionContext automatically commit statement
            with TransactionContext(self.connection.connection.trans()) as tr:
                try:
                    cur = tr.cursor()
                    cur.execute(str(sql), params)
                except Exception as e:
                    raise e
        else:
            super(DatabaseSchemaEditor, self).execute(sql, params)