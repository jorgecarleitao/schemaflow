import unittest

from schemaflow.pipeline import Pipeline
from schemaflow.pipe import Pipe
from schemaflow import types


class Pipe1(Pipe):
    transform_data = {
        'x': types.List(str),
    }

    transform_modifies = {
        'x': types.List(float),
    }

    def transform(self, data: dict):
        data['x'] = [float(x_i) for x_i in data['x']]
        return data


class Pipe2(Pipe):
    transform_data = {
        'x': types.List(float),
    }

    fit_data = {
        'x': types.List(float),
    }

    fitted_parameters = {'mean': float, 'var': float}

    transform_modifies = {
        'x': types.List(float),
    }

    def fit(self, data: dict, parameters: dict=None):
        self['mean'] = sum(data['x']) / len(data['x'])
        self['var'] = sum([x_i**2 for x_i in data['x']]) / len(data['x']) - self['mean']**2

    def transform(self, data: dict):
        data['x'] = [(x_i - self['mean'])/self['var']**0.5 for x_i in data['x']]
        return data


class TestPipeline(unittest.TestCase):

    def test_basic(self):
        p = Pipeline([Pipe1(), Pipe2()])

        self.assertEqual(p.check_fit({'x': ['1']}), [])
        self.assertEqual(p.check_transform({'x': ['1']}), [])
        self.assertEqual(p.fitted_parameters, {'0': {}, '1': {'mean': float, 'var': float}})
        self.assertEqual(p.transform_data, {'x': types.List(str)})

        p.fit({'x': ['1', '2', '3']})
        result = p.transform({'x': ['1', '2', '3']})

        # std([1,2,3]) == 0.816496580927726
        self.assertEqual(result['x'], [-1.2247448713915887, 0.0, 1.2247448713915887])
