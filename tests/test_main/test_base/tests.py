#-*- utf-8 -*-

from django.test import TestCase
from django.db import connection


class FirebirdTest(TestCase):
    def setUp(self):
        pass

    def test_server_version(self):
        version = connection.server_version
        self.assertNotEqual(version, '')

    def test_firebird_version(self):
        version = connection.ops.firebird_version
        self.assertNotEqual(version, [])
