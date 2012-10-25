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


class SlugFieldTests(TestCase):
    def test_slugfield_max_length(self):
        """
        Make sure SlugField honors max_length (#9706)
        """
        bs = BigS.objects.create(s = 'slug'*50)
        bs = BigS.objects.get(pk=bs.pk)
        self.assertEqual(bs.s, 'slug'*50)
