# -*- coding: utf-8 -*-
import six

from django.core.exceptions import ValidationError
from django.test import TestCase
from .models import DataModel


# Know issues:
# test_set_and_retrieve (model_fields.test_binaryfield.BinaryFieldTests)
#  File "/usr/lib/python2.7/ctypes/__init__.py", line 65, in create_string_buffer
#    raise TypeError(init)
#  TypeError: F�


class BinaryFieldTests(TestCase):
    binary_data = b'\x00\x46\xFE'

    def test_set_and_retrieve(self):
        data_set = (self.binary_data, six.memoryview(self.binary_data))
        for bdata in data_set:
            dm = DataModel(data=bdata)
            dm.save()
            dm = DataModel.objects.get(pk=dm.pk)
            self.assertEqual(bytes(dm.data), bytes(bdata))
            # Resave (=update)
            dm.save()
            dm = DataModel.objects.get(pk=dm.pk)
            self.assertEqual(bytes(dm.data), bytes(bdata))
            # Test default value
            self.assertEqual(bytes(dm.short_data), b'\x08')

    def test_max_length(self):
        dm = DataModel(short_data=self.binary_data * 4)
        with self.assertRaises(ValidationError):
            dm.full_clean()
