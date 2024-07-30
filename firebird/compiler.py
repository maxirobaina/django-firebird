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

        combinator = self.query.combinator
        extra_select, order_by, group_by = self.pre_sql_setup(
            with_col_aliases=with_col_aliases or bool(combinator),
        )
        idx = 1
        sub_str = '%s'
        for value, (s_sql, s_params), alias in self.select + extra_select:
            from django.db.models import Value
            if s_sql == sub_str and isinstance(value, Value):
                cast_length = len(''.join([str(item) for item in s_params]))
                sql = self.nth_repl(sql, sub_str, 'CAST(%s AS VARCHAR(%s))' % ('%s', cast_length), idx)
            idx += 1
            if alias:
                s_sql = "%s AS %s" % (
                    s_sql,
                    self.connection.ops.quote_name(alias),
                )
                a = s_sql

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

    def nth_repl(self, s, old, new, n):
        find = s.find(old)
        # If find is not -1 we have found at least one match for the substring
        idx = find != -1
        # loop until we find the nth, or we find no match
        while find != -1 and idx != n:
            # find + 1 means we start searching from after the last match
            find = s.find(old, find + 1)
            idx += 1
        # If idx is equal to n we found nth match so replace
        if idx == n and idx <= len(s.split(old)) - 1:
            return s[:find] + new + s[find + len(old):]
        return s


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
