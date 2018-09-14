import unittest
import numpy as np

from schemaflow.types import Array, infer_schema
from schemaflow import exceptions


class TestArray(unittest.TestCase):

    def test_instance_check(self):
        # 2D with N features
        instance = np.array([[1.0], [2.0]])
        array_type = Array(float, shape=(None, None))
        self.assertEqual(array_type.check_schema(instance), [])

        # 2D with 1 feature
        instance = np.array([[1.0], [2.0]])
        array_type = Array(float, shape=(None, 1))
        self.assertEqual(array_type.check_schema(instance), [])

        # 2D with 2 features != 2D with 1 feature
        instance = np.array([[1.0], [2.0]])
        array_type = Array(float, shape=(None, 2))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        # 1D != 2D
        instance = np.array([[1.0], [2.0]])
        array_type = Array(float, shape=(None,))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        # 1D == 1D
        instance = np.array([1.0, 2.0])
        array_type = Array(float, shape=(None,))
        self.assertEqual(array_type.check_schema(instance), [])

        instance = np.float64(2)
        array_type = Array(float, shape=(None,))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        instance = np.array([object, 2.0])
        array_type = Array(float, shape=(None,))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

    def test_type_check(self):
        # 2D with N features
        array_type = Array(float, shape=(None, None))
        self.assertEqual(array_type.check_schema(array_type), [])

        # 2D with 1 feature
        instance = Array(float, shape=(None, 1))
        self.assertEqual(array_type.check_schema(instance), [])

        # 2D with 2 features != 2D with 1 feature
        instance = Array(float, shape=(1, 1))
        array_type = Array(float, shape=(None, 2))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        # 1D != 2D
        instance = Array(float, shape=(1, 1))
        array_type = Array(float, shape=(None,))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        # 1D == 1D
        array_type = Array(float, shape=(None,))
        self.assertEqual(array_type.check_schema(array_type), [])

        instance = np.float64
        array_type = Array(float, shape=(None,))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        instance = np.array([object, 2.0]).dtype
        array_type = Array(float, shape=(None,))
        self.assertEqual(len(array_type.check_schema(instance)), 1)

        instance = Array(float, shape=(None, 1))
        array_type = Array(np.float64, shape=(1,))
        with self.assertRaises(exceptions.WrongShape) as e:
            array_type.check_schema(instance, True)
        self.assertIn('Wrong shape', str(e.exception))

    def test_infer(self):
        instance = np.array([[1.0], [2.0]])

        schema = infer_schema({'a': instance})
        self.assertEqual(schema, {'a': Array(np.float64, (2, 1))})
