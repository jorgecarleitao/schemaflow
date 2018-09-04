import unittest

import pandas as pd

from pipeline.types import PandasDataFrame


class TestPandasDataFrame(unittest.TestCase):

    def test_instance_check(self):
        # ok
        type = PandasDataFrame(schema={'a': pd.np.float64, 'b': pd.np.dtype('O')})
        instance = pd.DataFrame(data={'a': [1.0, 1.0], 'b': ['s', 's']})
        self.assertEqual(type.check_schema(instance), [])

        # extra column is ok
        instance = pd.DataFrame(data={'a': [1.0, 1.0], 'b': ['s', 's'], 'c': [1, 1]})
        self.assertEqual(type.check_schema(instance), [])

        # missing column
        instance = pd.DataFrame(data={'a': [1.0, 1.0]})
        self.assertEqual(len(type.check_schema(instance)), 1)

        # wrong column type
        instance = pd.DataFrame(data={'a': [1.0, 1.0], 'b': [1.0, 1.0]})
        self.assertEqual(len(type.check_schema(instance)), 1)

        # wrong datum type
        instance = 1
        self.assertEqual(len(type.check_schema(instance)), 1)

    def test_type_check(self):
        # ok
        type = PandasDataFrame(schema={'a': pd.np.float64, 'b': pd.np.dtype('O')})
        self.assertEqual(type.check_schema(type), [])

        # missing column
        instance = pd.DataFrame(data={'a': [1.0, 1.0]})
        self.assertEqual(len(type.check_schema(PandasDataFrame(schema={'a': pd.np.float64}))), 1)

        # wrong column type
        self.assertEqual(len(type.check_schema(PandasDataFrame(schema={'a': pd.np.float64, 'b': pd.np.float64}))), 1)

        # wrong datum type
        self.assertEqual(len(type.check_schema(int)), 1)
