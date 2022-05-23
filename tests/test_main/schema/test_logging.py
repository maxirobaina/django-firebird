from django.db import connection
from django.test import TestCase


class SchemaLoggerTests(TestCase):

    def test_extra_args(self):
        editor = connection.schema_editor(collect_sql=True)
        sql = 'SELECT * FROM rdb$database d WHERE d.rdb$relation_id in (?, ?)'
        params = [142, 1337]
        with self.assertLogs('django.db.backends.schema', 'DEBUG') as cm:
            editor.execute(sql, params)
        self.assertEqual(cm.records[0].sql, sql)
        self.assertEqual(cm.records[0].params, params)
        self.assertEqual(
            cm.records[0].getMessage(),
            'SELECT * FROM rdb$database d WHERE d.rdb$relation_id in (?, ?); (params [142, 1337])',
        )
