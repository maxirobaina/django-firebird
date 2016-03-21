import os
import sys

from django.db.backends import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = 'isql'

    def _get_args(self):
        args = [self.executable_name]
        settings_dict = self.connection.settings_dict
        if settings_dict['HOST']:
            args.append(settings_dict['HOST'] + ':' + settings_dict['NAME'])
        else:
            args.append(settings_dict['NAME'])
        if settings_dict['USER']:
            args += ["-u", settings_dict['USER']]
        if settings_dict['PASSWORD']:
            args += ["-p", settings_dict['PASSWORD']]
        if 'ROLE' in settings_dict:
            args += ["-r", settings_dict['ROLE']]
        return args
    args = property(_get_args)

    def runshell(self):
        if os.name == 'nt':
            sys.exit(os.system(" ".join(self.args)))
        else:
            self.executable_name = 'isql-fb'
            os.execvp(self.executable_name, self.args)
