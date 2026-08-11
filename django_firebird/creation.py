import sys
import firebird.driver as Database

from django.db.backends.base.creation import BaseDatabaseCreation

TEST_MODE = 0


class DatabaseCreation(BaseDatabaseCreation):

    def sql_for_inline_foreign_key_references(self, model, field, known_models, style):
        # Always pending
        return [], TEST_MODE < 2

    def sql_for_pending_references(self, model, style, pending_references):
        if TEST_MODE < 2:
            final_output = super(DatabaseCreation, self).sql_for_pending_references(model, style, pending_references)
            return ['%s ON DELETE CASCADE;' % s[:-1] for s in final_output]
        return []

    def sql_remove_table_constraints(self, model, references_to_delete, style):
        if TEST_MODE < 2:
            return super(DatabaseCreation, self).sql_remove_table_constraints(model, references_to_delete, style)
        return []

    def _check_active_connection(self, verbosity):
        if self.connection:
            if verbosity >= 1:
                print("Closing active connection")
            self.connection.close()

    def _build_dsn(self, database, default_host=None):
        # firebird-driver has no host/port arguments; the server location
        # must be embedded in the DSN as host[/port]:database.
        settings_dict = self.connection.settings_dict
        host = settings_dict.get('HOST') or default_host
        port = settings_dict.get('PORT')
        if not host:
            return database
        if port:
            return '%s/%s:%s' % (host, port, database)
        return '%s:%s' % (host, database)

    def _get_connection_params(self, **overrides):
        settings_dict = self.connection.settings_dict
        conn_params = {'charset': 'UTF8'}
        database = overrides.pop('database', settings_dict['NAME'])
        conn_params['database'] = self._build_dsn(database)
        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        if 'ROLE' in settings_dict:
            conn_params['role'] = settings_dict['ROLE']
        conn_params.update(settings_dict['OPTIONS'])
        conn_params.update(overrides)
        return conn_params

    def _get_creation_params(self, **overrides):
        settings_dict = self.connection.settings_dict
        params = {}
        if settings_dict.get('USER'):
            params['user'] = settings_dict['USER']
        if settings_dict.get('PASSWORD'):
            params['password'] = settings_dict['PASSWORD']
        params['charset'] = settings_dict.get('CHARSET') or 'UTF8'
        params['page_size'] = 8192

        test_settings = settings_dict.get('TEST')
        if test_settings:
            params['charset'] = test_settings.get('CHARSET') or params['charset']
            params['page_size'] = test_settings.get('PAGE_SIZE') or params['page_size']
        params.update(overrides)
        return params

    def _create_database(self, test_database_name, verbosity):
        self._check_active_connection(verbosity)
        params = self._get_creation_params(database=test_database_name)
        # Database creation attributes are taken from the driver configuration,
        # not from create_database() arguments.
        db_defaults = Database.driver_config.db_defaults
        db_defaults.page_size.value = params['page_size']
        db_defaults.db_charset.value = params['charset']
        connection = Database.create_database(
            self._build_dsn(params['database'], default_host='localhost'),
            user=params.get('user'),
            password=params.get('password'),
            charset=params['charset'],
        )
        connection.execute_immediate("CREATE EXCEPTION teste '';")
        connection.commit()
        connection.close()

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """"
        Internal implementation - creates the test db tables.
        """
        test_database_name = self._get_test_db_name()

        try:
            self._create_database(test_database_name, verbosity)
            if verbosity >= 1:
                print("Database %s created..." % test_database_name)
        except Exception as e:
            # if we want to keep the db, then no need to do any of the below,
            # just return and skip it all.
            if keepdb:
                return test_database_name

            sys.stderr.write("Got an error creating the test database: %s\n" % e)
            if not autoclobber:
                confirm = input("Type 'yes' if you would like to try deleting the test database '%s', or 'no' to cancel: " % test_database_name)
            if autoclobber or confirm == 'yes':
                try:
                    if verbosity >= 1:
                        print("Destroying old test database...")
                    self._destroy_test_db(test_database_name, verbosity)
                    if verbosity >= 1:
                        print("Creating test database...")
                    self._create_database(test_database_name, verbosity)
                    if verbosity >= 1:
                        print("Database %s created..." % test_database_name)
                except Exception as e:
                    sys.stderr.write("Got an error recreating the test database: %s\n" % e)
                    sys.exit(2)
            else:
                print("Tests cancelled.")
                sys.exit(1)

        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        """
        Internal implementation - remove the test db tables.
        """
        self._check_active_connection(verbosity)
        connection = Database.connect(**self._get_connection_params(database=test_database_name))
        connection.drop_database()
        connection.close()
