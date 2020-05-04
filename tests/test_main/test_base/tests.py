# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from django.conf import settings
from django.db import connection, DatabaseError
from django.db.models import F, DateField, DateTimeField, IntegerField, TimeField, CASCADE
from django.db.models.fields.related import ForeignKey
from django.db.models.functions import (
    Extract, ExtractDay, ExtractHour, ExtractMinute, ExtractMonth,
    ExtractSecond, ExtractWeek, ExtractWeekDay, ExtractYear, Trunc, TruncDate,
    TruncDay, TruncHour, TruncMinute, TruncMonth, TruncSecond, TruncTime,
    TruncYear,
)
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone


from .models import BigS, FieldsTest, Foo, Bar, DTModel


def microsecond_support(value):
    return value if connection.features.supports_microsecond_precision else value.replace(microsecond=0)


def truncate_to(value, kind, tzinfo=None):
    # Convert to target timezone before truncation
    if tzinfo is not None:
        value = value.astimezone(tzinfo)

    def truncate(value, kind):
        if kind == 'second':
            return value.replace(microsecond=0)
        if kind == 'minute':
            return value.replace(second=0, microsecond=0)
        if kind == 'hour':
            return value.replace(minute=0, second=0, microsecond=0)
        if kind == 'day':
            if isinstance(value, datetime):
                return value.replace(hour=0, minute=0, second=0, microsecond=0)
            return value
        if kind == 'month':
            if isinstance(value, datetime):
                return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            return value.replace(day=1)
        # otherwise, truncate to year
        if isinstance(value, datetime):
            return value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return value.replace(month=1, day=1)

    value = truncate(value, kind)
    if tzinfo is not None:
        # If there was a daylight saving transition, then reset the timezone.
        value = timezone.make_aware(value.replace(tzinfo=None), tzinfo)
    return value


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
        sql = self.ops.datetime_trunc_sql('year', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-01-01 00:00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql = self.ops.datetime_trunc_sql('month', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-01 00:00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql = self.ops.datetime_trunc_sql('day', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' 00:00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql = self.ops.datetime_trunc_sql('hour', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' '||EXTRACT(hour FROM DATE_FIELD)||':00:00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql = self.ops.datetime_trunc_sql('minute', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' '||EXTRACT(hour FROM DATE_FIELD)||':'||EXTRACT(minute FROM DATE_FIELD)||':00' AS TIMESTAMP)"
        self.assertEqual(sql, value)

        sql = self.ops.datetime_trunc_sql('second', 'DATE_FIELD', None)
        value = "CAST(EXTRACT(year FROM DATE_FIELD)||'-'||EXTRACT(month FROM DATE_FIELD)||'-'||EXTRACT(day FROM DATE_FIELD)||' '||EXTRACT(hour FROM DATE_FIELD)||':'||EXTRACT(minute FROM DATE_FIELD)||':'||TRUNC(EXTRACT(second FROM DATE_FIELD)) AS TIMESTAMP)"
        self.assertEqual(sql, value)

    def test_time_trunc_sql(self):
        sql = self.ops.time_trunc_sql('hour', 'TIME_FIELD')
        out = "CAST(EXTRACT(hour FROM TIME_FIELD) || ':00:00' AS TIME)"
        self.assertEqual(sql, out)

        sql = self.ops.time_trunc_sql('minute', 'TIME_FIELD')
        out = "CAST(EXTRACT(hour FROM TIME_FIELD) || ':' || EXTRACT(minute FROM TIME_FIELD) || ':00' AS TIME)"
        self.assertEqual(sql, out)

        sql = self.ops.time_trunc_sql('second', 'TIME_FIELD')
        out = "CAST(EXTRACT(hour FROM TIME_FIELD) || ':' || EXTRACT(minute FROM TIME_FIELD) || ':' || TRUNC(EXTRACT(second FROM TIME_FIELD)) AS TIME)"
        self.assertEqual(sql, out)


class DatabaseSchemaTests(TransactionTestCase):
    def test_no_index_for_foreignkey(self):
        """
        FirebirdSQL already creates indexes automatically for foreign keys. (#70).
        """
        index_sql = connection.schema_editor()._model_indexes_sql(Bar)
        self.assertEqual(index_sql, [])

    def test_fk_index_creation(self):
        new_field = ForeignKey(Foo, on_delete=CASCADE)
        new_field.set_attributes_from_name(None)
        with connection.schema_editor() as editor:
            editor.add_field(
                Bar,
                new_field
            )
            # Just return indexes others that not automaically created by Fk
            indexes = editor._get_field_indexes(Bar, new_field)
        self.assertEqual(indexes, [])

    def test_fk_remove_issue70(self):
        with connection.schema_editor() as editor:
            editor.remove_field(
                Bar,
                Bar._meta.get_field("a")
            )
        self.assertRaises(DatabaseError)


class SlugFieldTests(TestCase):
    def test_slugfield_max_length(self):
        """
        Make sure SlugField honors max_length (#9706)
        """
        bs = BigS.objects.create(s='slug' * 50)
        bs = BigS.objects.get(pk=bs.pk)
        self.assertEqual(bs.s, 'slug' * 50)


class DateFieldTests(TestCase):
    def tests_date_interval(self):
        obj = FieldsTest()
        obj.pub_date = datetime.now()
        obj.mod_date = obj.pub_date + timedelta(days=3)
        obj.save()

        objs = FieldsTest.objects.filter(mod_date__gte=F('pub_date') + timedelta(days=3)).all()
        self.assertEqual(len(objs), 1)


@override_settings(USE_TZ=False)
class DateFunctionTests(TestCase):

    def create_model(self, start_datetime, end_datetime):
        return DTModel.objects.create(
            name=start_datetime.isoformat(),
            start_datetime=start_datetime, end_datetime=end_datetime,
            start_date=start_datetime.date(), end_date=end_datetime.date(),
            start_time=start_datetime.time(), end_time=end_datetime.time(),
            duration=(end_datetime - start_datetime),
        )

    def test_trunc_func(self):
        start_datetime = microsecond_support(datetime(2015, 6, 15, 14, 30, 50, 321))
        end_datetime = microsecond_support(datetime(2016, 6, 15, 14, 10, 50, 123))
        if settings.USE_TZ:
            start_datetime = timezone.make_aware(start_datetime, is_dst=False)
            end_datetime = timezone.make_aware(end_datetime, is_dst=False)
        self.create_model(start_datetime, end_datetime)
        self.create_model(end_datetime, start_datetime)

        msg = 'output_field must be either DateField, TimeField, or DateTimeField'
        with self.assertRaisesMessage(ValueError, msg):
            list(DTModel.objects.annotate(truncated=Trunc('start_datetime', 'year', output_field=IntegerField())))

        with self.assertRaisesMessage(AssertionError, "'name' isn't a DateField, TimeField, or DateTimeField."):
            list(DTModel.objects.annotate(truncated=Trunc('name', 'year', output_field=DateTimeField())))

        with self.assertRaisesMessage(ValueError, "Cannot truncate DateField 'start_date' to DateTimeField"):
            list(DTModel.objects.annotate(truncated=Trunc('start_date', 'second')))

        with self.assertRaisesMessage(ValueError, "Cannot truncate TimeField 'start_time' to DateTimeField"):
            list(DTModel.objects.annotate(truncated=Trunc('start_time', 'month')))

        with self.assertRaisesMessage(ValueError, "Cannot truncate DateField 'start_date' to DateTimeField"):
            list(DTModel.objects.annotate(truncated=Trunc('start_date', 'month', output_field=DateTimeField())))

        with self.assertRaisesMessage(ValueError, "Cannot truncate TimeField 'start_time' to DateTimeField"):
            list(DTModel.objects.annotate(truncated=Trunc('start_time', 'second', output_field=DateTimeField())))

        def test_datetime_kind(kind):
            self.assertQuerysetEqual(
                DTModel.objects.annotate(
                    truncated=Trunc('start_datetime', kind, output_field=DateTimeField())
                ).order_by('start_datetime'),
                [
                    (truncate_to(start_datetime, kind)),
                    (truncate_to(end_datetime, kind))
                ],
                lambda m: (m.truncated)
            )

        def test_date_kind(kind):
            self.assertQuerysetEqual(
                DTModel.objects.annotate(
                    truncated=Trunc('start_date', kind, output_field=DateField())
                ).order_by('start_datetime'),
                [
                    (truncate_to(start_datetime.date(), kind)),
                    (truncate_to(end_datetime.date(), kind))
                ],
                lambda m: (m.truncated)
            )

        def test_time_kind(kind):
            self.assertQuerysetEqual(
                DTModel.objects.annotate(
                    truncated=Trunc('start_time', kind, output_field=TimeField())
                ).order_by('start_datetime'),
                [
                    (truncate_to(start_datetime.time(), kind)),
                    (truncate_to(end_datetime.time(), kind))
                ],
                lambda m: (m.truncated)
            )

        test_date_kind('year')
        test_date_kind('month')
        test_date_kind('day')
        test_time_kind('hour')
        test_time_kind('minute')
        test_time_kind('second')
        test_datetime_kind('year')
        test_datetime_kind('month')
        test_datetime_kind('day')
        test_datetime_kind('hour')
        test_datetime_kind('minute')
        test_datetime_kind('second')

        qs = DTModel.objects.filter(start_datetime__date=Trunc('start_datetime', 'day', output_field=DateField()))
        self.assertEqual(qs.count(), 2)

    def test_trunc_time_func(self):
        start_datetime = microsecond_support(datetime(2015, 6, 15, 14, 30, 50, 321000))
        end_datetime = microsecond_support(datetime(2016, 6, 15, 14, 10, 50, 123000))
        if settings.USE_TZ:
            start_datetime = timezone.make_aware(start_datetime, is_dst=False)
            end_datetime = timezone.make_aware(end_datetime, is_dst=False)
        self.create_model(start_datetime, end_datetime)
        self.create_model(end_datetime, start_datetime)
        self.assertQuerysetEqual(
            DTModel.objects.annotate(extracted=TruncTime('start_datetime')).order_by('start_datetime'),
            [
                (start_datetime.time()),
                (end_datetime.time()),
            ],
            lambda m: (m.extracted)
        )
        self.assertEqual(DTModel.objects.filter(start_datetime__time=TruncTime('start_datetime')).count(), 2)

        with self.assertRaisesMessage(ValueError, "Cannot truncate DateField 'start_date' to TimeField"):
            list(DTModel.objects.annotate(truncated=TruncTime('start_date')))

        with self.assertRaisesMessage(ValueError, "Cannot truncate DateField 'start_date' to TimeField"):
            list(DTModel.objects.annotate(truncated=TruncTime('start_date', output_field=DateField())))
