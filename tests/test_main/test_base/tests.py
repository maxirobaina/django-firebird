#-*- utf-8 -*-

from django.test import TestCase
from django.db import connection

from .models import BigS

class FirebirdTest(TestCase):
    def setUp(self):
        pass

    def test_server_version(self):
        version = connection.server_version
        self.assertNotEqual(version, '')

    def test_firebird_version(self):
        version = connection.ops.firebird_version
        self.assertNotEqual(version, [])


class DatabaseOperationsTest(TestCase):
    def setUp(self):
        self.ops = connection.ops

    def test_get_sequence_name(self):
        sq_name = self.ops.get_sequence_name('TEST')
        self.assertEqual(sq_name, '"TEST_SQ"')

    def test_drop_sequence_sql(self):
        sql = self.ops.drop_sequence_sql('TEST')
        self.assertEqual(sql, 'DROP SEQUENCE "TEST_SQ"')

    def test_date_extract_sql(self):
        sql = self.ops.date_extract_sql('week_day', 'DATE_FIELD')
        value = "EXTRACT(WEEKDAY FROM DATE_FIELD) + 1"
        self.assertEqual(sql, value)

        sql = self.ops.date_extract_sql('year', 'DATE_FIELD')
        value = "EXTRACT(YEAR FROM DATE_FIELD)"
        self.assertEqual(sql, value)

        sql = self.ops.date_extract_sql('month', 'DATE_FIELD')
        value = "EXTRACT(MONTH FROM DATE_FIELD)"
        self.assertEqual(sql, value)

        sql = self.ops.date_extract_sql('day', 'DATE_FIELD')
        value = "EXTRACT(DAY FROM DATE_FIELD)"
        self.assertEqual(sql, value)

    def test_datetime_trunc_sql(self):
        sql, params = self.ops.datetime_trunc_sql('year', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-01-01 00:00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql, params = self.ops.datetime_trunc_sql('month', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-01 00:00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql, params = self.ops.datetime_trunc_sql('day', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' 00:00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql, params = self.ops.datetime_trunc_sql('hour', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' '||EXTRACT(hour FROM DATE_FIELD)||':00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql, params = self.ops.datetime_trunc_sql('minute', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' '||EXTRACT(hour FROM DATE_FIELD)||':'||EXTRACT(minute FROM DATE_FIELD)||':00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql, params = self.ops.datetime_trunc_sql('second', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' '||EXTRACT(hour FROM DATE_FIELD)||':'||EXTRACT(minute FROM DATE_FIELD)||':'||EXTRACT(second FROM DATE_FIELD) AS TIMESTAMP)"
        self.assertEqual(sql, value)


class SlugFieldTests(TestCase):
    def test_slugfield_max_length(self):
        """
        Make sure SlugField honors max_length (#9706)
        """
        bs = BigS.objects.create(s = 'slug'*50)
        bs = BigS.objects.get(pk=bs.pk)
        self.assertEqual(bs.s, 'slug'*50)
