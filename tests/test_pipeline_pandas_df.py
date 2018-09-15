import unittest

import numpy as np
import pandas as pd

from schemaflow.pipeline import Pipeline
from schemaflow.pipe import Pipe
from schemaflow import types, ops


class Pipe1(Pipe):
    transform_requires = {
        'x': types.PandasDataFrame(schema={'a': np.float64, 'b': np.float64}),
    }

    transform_modifies = {
        'x': ops.ModifyDataFrame({'a * b': ops.Set(np.float64)})
    }

    def transform(self, data: dict):
        data['x']['a * b'] = data['x']['a'] * data['x']['b']
        return data


class Pipe2(Pipe):
    transform_requires = {
        'x': types.PandasDataFrame(schema={'a': np.float64, 'b': np.float64}),
    }

    transform_modifies = {
        'x': ops.ModifyDataFrame({'a': ops.Drop()})
    }

    def transform(self, data: dict):
        data['x'] = data['x'].drop('a', axis=1)
        return data


class TestPipeline(unittest.TestCase):

    def test_set(self):
        p = Pipeline([Pipe1()])

        result = p.transform({'x': pd.DataFrame({'a': [2.0], 'b': [2.0]})})
        self.assertEqual(result['x'].loc[:, 'a * b'].values, [4.0])

        self.assertEqual(p.transform_modifies, Pipe1.transform_modifies)

    def test_drop(self):
        p = Pipeline([Pipe2()])

        result = p.transform({'x': pd.DataFrame({'a': [2.0], 'b': [2.0]})})
        self.assertEqual(len(result['x'].columns), 1)

        self.assertEqual(p.transform_modifies, Pipe2.transform_modifies)

    def test_combine(self):
        p = Pipeline([Pipe1(), Pipe2()])

        result = p.transform({'x': pd.DataFrame({'a': [2.0], 'b': [2.0]})})
        self.assertEqual(len(result['x'].columns), 2)

        self.assertEqual(p.transform_modifies,
                         {'x': [Pipe1.transform_modifies['x'], Pipe2.transform_modifies['x']]})

        schema = p.transform_schema({'x': types.PandasDataFrame({'a': np.float64, 'b': np.float64})})

        self.assertEqual(schema['x'], types.PandasDataFrame({'b': np.float64,
                                                             'a * b': np.float64}))
