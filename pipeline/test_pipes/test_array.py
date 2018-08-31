import unittest
import numpy as np

from pipeline.types import Array


class TestArray(unittest.TestCase):

    def test_type_check(self):
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
