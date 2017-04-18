# -*- utf-8 -*-

from django.db import models


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
    a = models.ForeignKey(Foo, related_name=b'bars')
