import datetime
import warnings

from django.utils import six
from django.utils.encoding import force_str
from django.utils.deprecation import RemovedInDjango21Warning
from django.db.models.indexes import Index
from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo, TableInfo,
)


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        7: 'SmallIntegerField',
        8: 'IntegerField',
        10: 'FloatField',
        12: 'DateField',
        13: 'TimeField',
        14: 'CharField',
        16: 'BigIntegerField',
        27: 'FloatField',
        35: 'DateTimeField',
        37: 'CharField',
        40: 'TextField',
        260: 'BinaryField',
        261: 'TextField',
        # A NUMERIC/DECIMAL data type is stored as a SMALLINT, INTEGER or BIGINT
        # in Firebird, thus the value of RDB$FIELD_TYPE is reported. So we need
        # two additional helper data types for that to distinguish between real
        # Integer data types and NUMERIC/DECIMAL
        161: 'DecimalField',  # NUMERIC => RDB$FIELD_SUB_TYPE = 1
        162: 'DecimalField',  # DECIMAL => RDB$FIELD_SUB_TYPE = 2
        # Also, the scale value of a NUMERIC/DECIMAL fields is stored as negative
        # number in the Firebird system tables, thus we have to multiply with -1.
        # The SELECT statement in the function get_table_description takes care
        # of all of that.
    }

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

    def table_name_converter(self, name):
        return name.lower()

    def get_table_list(self, cursor):
        "Returns a list of table names in the current database."
        cursor.execute("""
            select
                lower(trim(rdb$relation_name)),
                case when RDB$VIEW_BLR IS NULL then 't' else 'v' end as rel_type
            from rdb$relations
            where rdb$system_flag=0
            order by 1 """)
        # return [r[0].strip().lower() for r in cursor.fetchall()]
        return [TableInfo(row[0], row[1]) for row in cursor.fetchall()]

    def get_table_description(self, cursor, table_name):
        """
        Returns a description of the table, with the DB-API cursor.description interface.
        Must return a 'FieldInfo' struct 'name type_code display_size internal_size precision scale null_ok'
        """
        tbl_name = "'%s'" % table_name.upper()
        cursor.execute("""
            select
              lower(trim(rf.rdb$field_name))
              , case
                  when (f.rdb$field_type in (7,8,16)) and (f.rdb$field_sub_type > 0) then
                    160 + f.rdb$field_sub_type
                  when (f.rdb$field_type = 261) then
                    260 + f.rdb$field_sub_type
                  else
                    f.rdb$field_type
                end as type_code

              , case
                  when (f.rdb$field_type in (14,37)) then
                    f.rdb$character_length
                  else
                    f.rdb$field_length
                end as field_length
              , f.rdb$field_precision
              , f.rdb$field_scale * -1
              , rf.rdb$null_flag
              , rf.rdb$default_source
            from
              rdb$relation_fields rf join rdb$fields f on (rf.rdb$field_source = f.rdb$field_name)
            where
              upper(rf.rdb$relation_name) = %s
            order by
              rf.rdb$field_position
            """ % (tbl_name,))
        items = []
        for r in cursor.fetchall():
            # name type_code display_size internal_size precision scale null_ok + default
            items.append(FieldInfo(r[0], r[1], r[2], r[2] or 0, r[3], r[4], not (r[5] == 1), r[6]))
        return items

    def get_key_columns(self, cursor, table_name):
        """
        Backends can override this to return a list of (column_name, referenced_table_name,
        referenced_column_name) for all key columns in given table.
        """
        tbl_name = "'%s'" % table_name.upper()
        key_columns = []
        cursor.execute("""
            select
                lower(s.rdb$field_name) as column_name,
                lower(i2.rdb$relation_name) as referenced_table_name,
                lower(s2.rdb$field_name) as referenced_column_name
            from rdb$index_segments s
            left join rdb$indices i on i.rdb$index_name = s.rdb$index_name
            left join rdb$relation_constraints rc on rc.rdb$index_name = s.rdb$index_name
            left join rdb$ref_constraints refc on rc.rdb$constraint_name = refc.rdb$constraint_name
            left join rdb$relation_constraints rc2 on rc2.rdb$constraint_name = refc.rdb$const_name_uq
            left join rdb$indices i2 on i2.rdb$index_name = rc2.rdb$index_name
            left join rdb$index_segments s2 on i2.rdb$index_name = s2.rdb$index_name
            WHERE RC.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
            and upper(i.rdb$relation_name) = %s """ % (tbl_name,))

        for r in cursor.fetchall():
            key_columns.append((r[0].strip(), r[1].strip(), r[2].strip()))
        return key_columns

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        constraints = self.get_key_columns(cursor, table_name)
        relations = {}
        for my_fieldname, other_table, other_field in constraints:
            relations[my_fieldname] = (other_field, other_table)
        return relations

    def get_indexes(self, cursor, table_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index/constraint}
        """

        warnings.warn(
            "get_indexes() is deprecated in favor of get_constraints().",
            RemovedInDjango21Warning, stacklevel=2
        )

        # This query retrieves each field name and index type on the given table.
        tbl_name = "'%s'" % table_name.upper()
        cursor.execute("""
        SELECT
          LOWER(s.RDB$FIELD_NAME) AS field_name,

          LOWER(case
            when rc.RDB$CONSTRAINT_TYPE is not null then rc.RDB$CONSTRAINT_TYPE
            else 'INDEX'
          end) AS constraint_type

        FROM RDB$INDEX_SEGMENTS s
        LEFT JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        WHERE i.RDB$RELATION_NAME = %s
        AND i.RDB$SEGMENT_COUNT = 1
        ORDER BY s.RDB$FIELD_POSITION
        """ % (tbl_name,))
        indexes = {}
        for fn, ct in cursor.fetchall():
            field_name = fn.strip()
            constraint_type = ct.strip()
            if field_name not in indexes:
                indexes[field_name] = {'primary_key': False, 'unique': False}
            # It's possible to have the unique and PK constraints in separate indexes.
            if constraint_type == 'primary key':
                indexes[field_name]['primary_key'] = True
            if constraint_type == 'unique':
                indexes[field_name]['unique'] = True
        return indexes

    def get_constraints(self, cursor, table_name):
        """
        Retrieves any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Returns a dict mapping constraint names to their attributes,
        where attributes is a dict with keys:
         * columns: List of columns this covers
         * primary_key: True if primary key, False otherwise
         * unique: True if this is a unique constraint, False otherwise
         * foreign_key: (table, column) of target, or None
         * check: True if check constraint, False otherwise
         * index: True if index, False otherwise.
         * orders: The order (ASC/DESC) defined for the columns of indexes
         * type: The type of the index (btree, hash, etc.)

        Some backends may return special constraint names that don't exist
        if they don't name constraints of a certain type (e.g. SQLite)
        """
        tbl_name = "'%s'" % table_name.upper()
        constraints = {}

        cursor.execute("""
        SELECT
          case
            when rc.RDB$CONSTRAINT_NAME is not null then rc.RDB$CONSTRAINT_NAME
            else i.RDB$INDEX_NAME
          end as constraint_name,

          case
            when rc.RDB$CONSTRAINT_TYPE is not null then rc.RDB$CONSTRAINT_TYPE
            else 'INDEX'
          end AS constraint_type,

          s.RDB$FIELD_NAME AS field_name,
          i2.RDB$RELATION_NAME AS references_table,
          s2.RDB$FIELD_NAME AS references_field,
          i.RDB$UNIQUE_FLAG,
          i.RDB$INDEX_TYPE
        FROM RDB$INDEX_SEGMENTS s
        LEFT JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        LEFT JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS rc2 ON rc2.RDB$CONSTRAINT_NAME = refc.RDB$CONST_NAME_UQ
        LEFT JOIN RDB$INDICES i2 ON i2.RDB$INDEX_NAME = rc2.RDB$INDEX_NAME
        LEFT JOIN RDB$INDEX_SEGMENTS s2 ON i2.RDB$INDEX_NAME = s2.RDB$INDEX_NAME
        WHERE i.RDB$RELATION_NAME = %s
        ORDER BY s.RDB$FIELD_POSITION
        """ % (tbl_name,))
        for constraint_name, constraint_type, column, other_table, other_column, unique, order in cursor.fetchall():
            primary_key = False
            foreign_key = None
            check = False
            index = False
            order = 'DESC' if order else 'ASC'
            constraint = constraint_name.strip()
            constraint_type = constraint_type.strip()
            column = column.strip().lower()
            if other_table:
                other_table = other_table.strip().lower()
            if other_column:
                other_column = other_column.strip().lower()

            if constraint_type == 'PRIMARY KEY':
                primary_key = True
            elif constraint_type == 'UNIQUE':
                unique = True
            elif constraint_type == 'FOREIGN KEY':
                foreign_key = (other_table, other_column,)
                index = True
            elif constraint_type == 'INDEX':
                index = True

            if constraint not in constraints:
                constraints[constraint] = {
                    "columns": [],
                    "orders": [],
                    "primary_key": primary_key,
                    "unique": unique,
                    "foreign_key": foreign_key,
                    "check": check,
                    "index": index,
                    "type": Index.suffix
                }
            # Record the details
            constraints[constraint]['columns'].append(column)
            constraints[constraint]['orders'].append(order)

        return constraints

    def _get_field_indexes(self, cursor, table_name, field_name):
        """
          Return a list of index names that are not created automatically (ie: Foreign Key)
        """
        table = "'%s'" % table_name.upper()
        field = "'%s'" % field_name.upper()
        cursor.execute("""
            select s.rdb$index_name
            from rdb$index_segments s
            left join rdb$indices i on i.rdb$index_name = s.rdb$index_name
            left join rdb$relation_constraints rc on rc.rdb$index_name = s.rdb$index_name
            where i.rdb$relation_name = %s
            and s.rdb$field_name = %s
            and rc.rdb$constraint_type is null
            order by s.rdb$field_position """ % (table, field,))

        return [index_name[0].strip() for index_name in cursor.fetchall()]

    def _name_to_index(self, cursor, table_name):
        """Return a dictionary of {field_name: field_index} for the given table.
           Indexes are 0-based.
        """
        return dict([(d[0], i) for i, d in enumerate(self.get_table_description(cursor, table_name))])
