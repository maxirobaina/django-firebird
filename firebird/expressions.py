import datetime

from django.db.models.expressions import RawSQL, Value, Expression
from django.db.models.functions import Length, Substr, ConcatPair
from django.utils.encoding import force_str


def quote_value(value):
    if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
        return "'%s'" % value
    elif isinstance(value, str):
        return "'%s'" % value
    elif isinstance(value, bool):
        return "1" if value else "0"
    elif value is None:
        return "NULL"
    else:
        return force_str(value)


def firebird_fix_value_expr(self, compiler, connection):
    """
    Firebird fails to resolve some params replacement
    https://stackoverflow.com/questions/37348807/data-type-unknown-in-case-expression-with-only-parameters-as-values

    See a workaround at:
    https://groups.google.com/d/msg/django-developers/UcO_ha1APPk/n3E3JzFvBwAJ
    """
    sql, params = self.as_sql(compiler, connection)

    if len(params) > 0:
        print("params:", params, type(params[0]))
    else:
        print("params:", params, type(params))

    _params = tuple(quote_value(p) for p in params)
    if _params:
        return sql % _params, []
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


setattr(Length, 'as_firebird', firebird_length)
setattr(Substr, 'as_firebird', firebird_substring)
setattr(ConcatPair, 'as_firebird', firebird_concat)
