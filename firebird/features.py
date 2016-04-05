from django.utils.functional import cached_property
from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    allows_group_by_pk = False  # if the backend can group by just by PK
    supports_forward_references = False
    has_bulk_insert = False
    can_return_id_from_insert = True
    has_select_for_update = True
    has_select_for_update_nowait = False
    supports_forward_references = False
    supports_tablespaces = False
    supports_long_model_names = False
    supports_timezones = False
    has_zoneinfo_database = False
    uses_savepoints = True
    supports_paramstyle_pyformat = False
    # connection_persists_old_columns = True
    can_rollback_ddl = True
    requires_literal_defaults = True
    has_case_insensitive_like = False

    # In firebird, check constraint are table based, no column based
    supports_column_check_constraints = False

    can_introspect_boolean_field = False
    can_introspect_small_integer_field = True

    # If NULL is implied on columns without needing to be explicitly specified
    implied_column_null = True

    uppercases_column_names = True

    @cached_property
    def supports_transactions(self):
        return True
