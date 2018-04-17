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
    connection_persists_old_columns = False
    can_rollback_ddl = True
    requires_literal_defaults = True
    has_case_insensitive_like = False

    # Is there a true datatype for uuid?
    has_native_uuid_field = False

    # Is there a true datatype for timedeltas?
    has_native_duration_field = False

    # In firebird, check constraint are table based, no column based
    supports_column_check_constraints = False

    can_introspect_foreign_keys = True
    can_introspect_boolean_field = False
    can_introspect_small_integer_field = True

    # If NULL is implied on columns without needing to be explicitly specified
    implied_column_null = True

    uppercases_column_names = True

    # Does the __regex lookup support backreferencing and grouping?
    supports_regex_backreferencing = False

    # Does the database driver supports same type temporal data subtraction
    # by returning the type used to store duration field?
    supports_temporal_subtraction = False

    @cached_property
    def supports_transactions(self):
        return True

    def introspected_boolean_field_type(self, field=None, created_separately=False):
        """
        What is the type returned when the backend introspects a BooleanField?
        The optional arguments may be used to give further details of the field to be
        introspected; in particular, they are provided by Django's test suite:
        field -- the field definition
        created_separately -- True if the field was added via a SchemaEditor's AddField,
                              False if the field was created with the model

        Note that return value from this function is compared by tests against actual
        introspection results; it should provide expectations, not run an introspection
        itself.
        """

        return 'SmallIntegerField'
