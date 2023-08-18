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

    def as_sql(self, with_limits=True, with_col_aliases=False):
        sql, params = super(SQLCompiler, self).as_sql(with_limits=False, with_col_aliases=with_col_aliases)

        if with_limits:
            limits = []
            if self.query.high_mark is not None:
                limits.append('FIRST %d' % (self.query.high_mark - self.query.low_mark))
            if self.query.low_mark:
                if self.query.high_mark is None:
                    val = self.connection.ops.no_limit_value()
                    if val:
                        limits.append('FIRST %d' % val)
                limits.append('SKIP %d' % self.query.low_mark)
            sql = 'SELECT %s %s' % (' '.join(limits), sql[6:].strip())
        return sql, params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
