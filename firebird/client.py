import os
import sys
import subprocess

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = None

    def __init__(self, *args, **kwargs):
        if os.name == 'nt':
            self.executable_name = 'isql'
        else:
            self.executable_name = 'isql-fb'
        super(DatabaseClient, self).__init__(*args, **kwargs)

    def _get_args(self):
        args = [self.executable_name]
        params = self.connection.get_connection_params()
        args.append(params['dsn'])
        if params['user']:
            args += ["-u", params['user']]
        if params['password']:
            args += ["-p", params['password']]
        if 'role' in params:
            args += ["-r", params['role']]
        return args
    args = property(_get_args)

    def runshell(self):
        if os.name == 'nt':
            sys.exit(os.system(" ".join(self.args)))
        else:
            subprocess.check_call(self.args)
