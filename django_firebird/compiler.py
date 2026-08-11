from django.db.models.sql import compiler

# The izip_longest was renamed to zip_longest in py3
try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest


class SQLCompiler(compiler.SQLCompiler):
    def resolve_columns(self, row, fields=()):
        # We need to convert values from database to correct django field representation.
        # For instance, if we defined a BooleanField field, django-firebird do create a
        # smallint field into DB. When retrieving this field value, it's converted to
        # BooleanField again.
        index_start = len(self.query.extra_select)
        values = []
        for value, field in zip_longest(row[index_start:], fields):
            v = self.query.convert_values(value, field, connection=self.connection)
            values.append(v)
        return row[:index_start] + tuple(values)

    # Row limiting uses the standard OFFSET/FETCH syntax provided by
    # DatabaseOperations.limit_offset_sql(), and untyped Value parameters are
    # inlined by Value.as_firebird (see expressions.py), so no custom as_sql()
    # is needed.


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
