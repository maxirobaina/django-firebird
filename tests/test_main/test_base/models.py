# -*- utf-8 -*-

from django.db import models
from django.utils.encoding import python_2_unicode_compatible


class FieldsTest(models.Model):
    pub_date = models.DateTimeField()
    mod_date = models.DateTimeField()


class BigS(models.Model):
    s = models.SlugField(max_length=255)


class Foo(models.Model):
    a = models.CharField(max_length=10)
    d = models.DecimalField(max_digits=5, decimal_places=3)


class Bar(models.Model):
    b = models.CharField(max_length=10)
    a = models.ForeignKey(Foo, related_name='bars', on_delete=models.CASCADE)


@python_2_unicode_compatible
class DTModel(models.Model):
    name = models.CharField(max_length=32)
    start_datetime = models.DateTimeField(null=True, blank=True)
    end_datetime = models.DateTimeField(null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    start_time = models.TimeField(null=True, blank=True)
    end_time = models.TimeField(null=True, blank=True)
    duration = models.DurationField(null=True, blank=True)

    def __str__(self):
        return 'DTModel({0})'.format(self.name)
