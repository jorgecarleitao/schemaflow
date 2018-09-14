import unittest

from schemaflow.types import List, Tuple, infer_schema


class TestListTuple(unittest.TestCase):

    def test_instance_check(self):
        instance = [1.0, 2.0]
        array_type = List(float)
        self.assertEqual(array_type.check_schema(instance), [])

        instance = [1.0, 'a']
        array_type = List(float)
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        instance = (1.0, 2.0)
        array_type = List(float)
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        instance = (1.0, 2.0)
        array_type = Tuple(float)
        self.assertEqual(array_type.check_schema(instance), [])

    def test_type_check(self):
        array_type = List(float)
        self.assertEqual(array_type.check_schema(List(float)), [])

        array_type = List(float)
        self.assertEqual(len(array_type.check_schema(List(object))), 1)

        array_type = List(float)
        self.assertEqual(len(array_type.check_schema(Tuple(float))), 1)

        array_type = Tuple(float)
        self.assertEqual(array_type.check_schema(Tuple(float)), [])

    def test_infer(self):
        instance = [1.0, 2.0]

        schema = infer_schema({'a': instance})
        self.assertEqual(schema, {'a': List(float)})
