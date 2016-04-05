# -*- utf-8 -*-

from django.db import models


class FieldsTest(models.Model):
    pub_date = models.DateTimeField()
    mod_date = models.DateTimeField()


class BigS(models.Model):
    s = models.SlugField(max_length=255)
