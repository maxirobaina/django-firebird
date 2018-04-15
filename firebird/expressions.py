import datetime

from django.utils import six
from django.db.models.expressions import RawSQL, Value
from django.utils.encoding import force_str


def quote_value(value):
    if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
        return "'%s'" % value
    elif isinstance(value, six.string_types):
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
    """
    sql, params = self.as_sql(compiler, connection)
    prm = tuple(quote_value(p) for p in params)
    return sql % prm, []


RawSQL.as_firebird = firebird_fix_value_expr
Value.as_firebird = firebird_fix_value_expr
