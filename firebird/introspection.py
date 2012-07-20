from django.db.backends import BaseDatabaseIntrospection

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        7: 'SmallIntegerField',
        8: 'IntegerField',
        10: 'FloatField',
        12: 'DateField',
        13: 'TimeField',
        14: 'CharField',
        16: 'IntegerField',
        27: 'FloatField',
        35: 'DateTimeField',
        37: 'CharField',
        40: 'TextField',
        261: 'TextField',
        # A NUMERIC/DECIMAL data type is stored as a SMALLINT, INTEGER or BIGINT
        # in Firebird, thus the value of RDB$FIELD_TYPE is reported. So we need
        # two additional helper data types for that to distinguish between real
        # Integer data types and NUMERIC/DECIMAL
        161: 'DecimalField', # NUMERIC => RDB$FIELD_SUB_TYPE = 1
        162: 'DecimalField', # DECIMAL => RDB$FIELD_SUB_TYPE = 2
        # Also, the scale value of a NUMERIC/DECIMAL fields is stored as negative
        # number in the Firebird system tables, thus we have to multiply with -1.
        # The SELECT statement in the function get_table_description takes care
        # of all of that.
    }

    def get_table_list(self, cursor):
        "Returns a list of table names in the current database."
        cursor.execute("""select rdb$relation_name from rdb$relations
            where rdb$system_flag=0 and rdb$view_source is null
            order by rdb$relation_name""")
        return [r[0].strip().lower() for r in cursor.fetchall()]
    
    def table_name_converter(self, name):
        return name.lower()

    def get_table_description(self, cursor, table_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        tbl_name = "'%s'" % table_name
        cursor.execute("""
            select
              rf.rdb$field_name
              , case
                  when (f.rdb$field_type in (7,8,16)) and (f.rdb$field_sub_type > 0) then
                    160 + f.rdb$field_sub_type
                  else
                    f.rdb$field_type end
              , f.rdb$field_length
              , f.rdb$field_precision
              , f.rdb$field_scale * -1
              , rf.rdb$null_flag
            from
              rdb$relation_fields rf join rdb$fields f on (rf.rdb$field_source = f.rdb$field_name)
            where
              rf.rdb$relation_name = %s
            order by
              rf.rdb$field_position
            """ % (tbl_name,))
        return [(r[0].strip(), r[1], r[2], r[2] or 0, r[3], r[4], not (r[5] == 1)) for r in cursor.fetchall()]
        
    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        tbl_name = "'%s'" % table_name
        cursor.execute("""
            select
              rf1.rdb$field_position
              , rf2.rdb$field_position
              , rf2.rdb$relation_name
            from
              rdb$relation_constraints rc1
              join rdb$indices i1 on (rc1.rdb$index_name = i1.rdb$index_name)
              join rdb$index_segments is1 on (i1.rdb$index_name = is1.rdb$index_name)
              join rdb$relation_fields rf1 on (rc1.rdb$relation_name = rf1.rdb$relation_name and is1.rdb$field_name = rf1.rdb$field_name)
              join rdb$relation_constraints rc2 on (rc2.rdb$constraint_name = i1.rdb$foreign_key)
              join rdb$index_segments is2 on (rc2.rdb$index_name = is2.rdb$index_name)
              join rdb$relation_fields rf2 on (rc2.rdb$relation_name = rf2.rdb$relation_name and is2.rdb$field_name = rf2.rdb$field_name)
            where
              rf1.rdb$relation_name = %s
              and rc1.rdb$constraint_type = 'FOREIGN KEY'
            order by
              rf1.rdb$field_position""" % (tbl_name,))

        relations = {}
        for r in cursor.fetchall():
            relations[r[0]] = (r[1], r[2].strip())
        return relations

    def get_indexes(self, cursor, table_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index/constraint}
        """

        # This query retrieves each field name and index type on the given table.
        tbl_name = "'%s'" % table_name
        cursor.execute("""
            SELECT
              seg2.rdb$field_name
              , case
                  when exists (
                    select
                      1
                    from
                      rdb$relation_constraints con
                    where
                      con.rdb$constraint_type = 'PRIMARY KEY'
                      and con.rdb$index_name = i.rdb$index_name
                  ) then
                    'PRIMARY KEY'
                  else
                    'UNIQUE'
              end
            FROM
              rdb$indices i
              JOIN rdb$index_segments seg2 on seg2.rdb$index_name = i.rdb$index_name
            WHERE
              i.rdb$relation_name = %s
              and i.rdb$unique_flag = 1""" % (tbl_name,))
        indexes = {}
        for r in cursor.fetchall():
            indexes[r[0].strip()] = {
                'primary_key': (r[1].strip() == 'PRIMARY KEY'),
                'unique': (r[1].strip() == 'UNIQUE')
            }
        return indexes
