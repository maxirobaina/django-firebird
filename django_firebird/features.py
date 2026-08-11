from django.utils.functional import cached_property
from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    allows_group_by_pk = False  # if the backend can group by just by PK
    supports_forward_references = False
    has_bulk_insert = True
    can_return_columns_from_insert = True
    has_select_for_update = True
    has_select_for_update_nowait = False
    has_select_for_update_skip_locked = True
    has_select_for_update_of = True
    supports_tablespaces = False
    supports_long_model_names = False
    supports_timezones = False
    has_zoneinfo_database = False
    uses_savepoints = True
    supports_paramstyle_pyformat = False
    connection_persists_old_columns = False
    can_rollback_ddl = True
    requires_literal_defaults = True
    has_case_insensitive_like = False
    supports_index_column_ordering = False

    # Is there a true datatype for uuid?
    has_native_uuid_field = False

    # Is there a true datatype for timedeltas?
    has_native_duration_field = False

    # In firebird, check constraint are table based, no column based
    supports_column_check_constraints = False

    can_introspect_foreign_keys = True
    can_introspect_small_integer_field = True

    # If NULL is implied on columns without needing to be explicitly specified
    implied_column_null = True

    uppercases_column_names = True

    # Does the __regex lookup support backreferencing and grouping?
    supports_regex_backreferencing = False

    # Does the database driver supports same type temporal data subtraction
    # by returning the type used to store duration field?
    supports_temporal_subtraction = False

    supports_microsecond_precision = False

    can_introspect_null = True

    supports_partial_indexes = False
    supports_expression_indexes = False

    allows_multiple_constraints_on_same_fields = False

    supports_json_field = False

    supports_ignore_conflicts = False

    # Commit every statement, that other transactions see changes.
    autocommit_when_autocommit_is_off = True

    max_query_params = 255

    @cached_property
    def supports_transactions(self):
        return True

    @cached_property
    def introspected_field_types(self):
        # Auto fields are implemented with sequences and triggers, positive
        # integer fields with check constraints, and durations as bigint, so
        # they all introspect as their plain column types.
        return {
            **super().introspected_field_types,
            'AutoField': 'IntegerField',
            'BigAutoField': 'BigIntegerField',
            'SmallAutoField': 'SmallIntegerField',
            'PositiveBigIntegerField': 'BigIntegerField',
            'PositiveIntegerField': 'IntegerField',
            'PositiveSmallIntegerField': 'SmallIntegerField',
            'DurationField': 'BigIntegerField',
            'GenericIPAddressField': 'CharField',
        }
