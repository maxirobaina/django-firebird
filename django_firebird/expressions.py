import datetime
import decimal

from django.db.models import Case, When
from django.db.models.expressions import RawSQL, Value, Expression
from django.db.models.functions import Cast, Length, RPad, Repeat, Substr, ConcatPair
from django.utils.encoding import force_str


def _fraction(value):
    # Firebird accepts at most four fractional-second digits in literals
    # (its timestamps have 100 microsecond resolution).
    return '.%s' % ('%06d' % value.microsecond)[:4] if value.microsecond else ''


def quote_value(value):
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP '%s%s'" % (value.strftime('%Y-%m-%d %H:%M:%S'), _fraction(value))
    elif isinstance(value, datetime.date):
        return "DATE '%s'" % value
    elif isinstance(value, datetime.time):
        return "TIME '%s%s'" % (value.strftime('%H:%M:%S'), _fraction(value))
    elif isinstance(value, str):
        # A plain quoted literal: it must stay valid in any context, including
        # DDL DEFAULT clauses, which reject CAST() and expressions.
        return "'%s'" % value.replace("'", "''")
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif value is None:
        return "NULL"
    else:
        return force_str(value)


def firebird_fix_value_expr(self, compiler, connection):
    """
    Firebird fails to resolve the type of query parameters that appear in
    expressions without any column operand (e.g. comparing two literals),
    raising "Data type unknown" (SQL error -804). Render plain literal values
    inline instead of binding them as parameters.
    https://stackoverflow.com/questions/37348807/data-type-unknown-in-case-expression-with-only-parameters-as-values
    """
    sql, params = self.as_sql(compiler, connection)
    if params and all(
        isinstance(p, (str, bool, int, float, decimal.Decimal,
                       datetime.date, datetime.time, datetime.datetime))
        or p is None
        for p in params
    ):
        # Temporal values reach this point already adapted to strings; use the
        # output field to render them as typed literals, since e.g. DATEADD
        # does not accept an untyped string where a timestamp is expected.
        try:
            internal_type = self.output_field.get_internal_type()
        except Exception:
            internal_type = None
        literal_prefix = {'DateTimeField': 'TIMESTAMP',
                          'DateField': 'DATE',
                          'TimeField': 'TIME'}.get(internal_type)

        def quote(p):
            if literal_prefix and isinstance(p, str):
                return "%s '%s'" % (literal_prefix, p)
            return quote_value(p)

        return sql % tuple(quote(p) for p in params), []
    return sql, params


def firebird_length(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, function='CHAR_LENGTH', **extra_context)


def firebird_substring(self, compiler, connection, function=None, template=None, arg_joiner=None, **extra_context):
    connection.ops.check_expression_support(self)
    sql_parts = []
    params = []
    for arg in self.source_expressions:
        arg_sql, arg_params = compiler.compile(arg)
        sql_parts.append(arg_sql)
        params.extend(arg_params)
    data = {**self.extra, **extra_context}
    # Use the first supplied value in this order: the parameter to this
    # method, a value supplied in __init__()'s **extra (the value in
    # `data`), or the value defined on the class.
    if function is not None:
        data['function'] = function
    else:
        data.setdefault('function', self.function)
    template = template or data.get('template', self.template)
    arg_joiner = arg_joiner or data.get('arg_joiner', self.arg_joiner)
    if len(sql_parts) == 2:
        data['expressions'] = data['field'] = sql_parts[0] + ' from ' + sql_parts[1]
    else:
        data['expressions'] = data['field'] = sql_parts[0] + ' from ' + sql_parts[1] + ' for ' + sql_parts[2]
    template = template % data
    template = template % tuple(params)
    return template, []


def firebird_concat(self, compiler, connection, **extra_context):
    return self.as_sql(compiler, connection, template='%(expressions)s', arg_joiner=' || ', **extra_context)


def firebird_cast(self, compiler, connection, **extra_context):
    """
    Firebird cannot CAST a BOOLEAN to a numeric type. Render casts of
    conditional expressions to integer types as CASE WHEN ... THEN 1 ELSE 0.
    """
    source = self.source_expressions[0]
    target = self.output_field.get_internal_type()
    if getattr(source, 'conditional', False) and target in (
        'IntegerField', 'BigIntegerField', 'SmallIntegerField',
        'PositiveIntegerField', 'PositiveBigIntegerField', 'PositiveSmallIntegerField',
    ):
        source_sql, params = compiler.compile(source)
        return 'CASE WHEN %s THEN 1 ELSE 0 END' % source_sql, params
    return self.as_sql(compiler, connection, **extra_context)


setattr(Length, 'as_firebird', firebird_length)
setattr(Substr, 'as_firebird', firebird_substring)
setattr(ConcatPair, 'as_firebird', firebird_concat)
def firebird_case(self, compiler, connection, **extra_context):
    """
    String literals are typed CHAR(n) in Firebird, so a CASE over literals of
    different lengths gets blank-padded to the longest one. Trim the result,
    but only when every branch is a string literal, so real column values with
    meaningful trailing spaces are never touched.
    """
    sql, params = self.as_sql(compiler, connection, **extra_context)
    results = [case.result for case in self.get_source_expressions() if isinstance(case, When)]
    results.append(self.default)
    if all(r is None or (isinstance(r, Value) and isinstance(r.value, str)) for r in results):
        sql = 'TRIM(TRAILING FROM %s)' % sql
    return sql, params


def firebird_repeat(self, compiler, connection, **extra_context):
    # Firebird has no REPEAT(); emulate it with RPAD like on Oracle.
    expression, number = self.source_expressions
    length = None if number is None else Length(expression) * number
    rpad = RPad(expression, length, expression)
    return rpad.as_sql(compiler, connection, **extra_context)


setattr(Value, 'as_firebird', firebird_fix_value_expr)
setattr(Cast, 'as_firebird', firebird_cast)
setattr(Case, 'as_firebird', firebird_case)
setattr(Repeat, 'as_firebird', firebird_repeat)
