import unittest
import collections

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

    fit_parameters = {'unused': float}

    transform_modifies = {
        'x': types.List(float),
    }

    def fit(self, data: dict, parameters: dict=None):
        self['mean'] = sum(data['x']) / len(data['x'])
        self['var'] = sum([x_i**2 for x_i in data['x']]) / len(data['x']) - self['mean']**2

    def transform(self, data: dict):
        data['x'] = [(x_i - self['mean'])/self['var']**0.5 for x_i in data['x']]
        return data


class Pipe3(Pipe):
    transform_data = {
        'x1': types.List(str),
        'x': types.List(float),
    }


class Pipe4(Pipe):
    fit_data = {
        'x1': types.List(float),
    }

    fitted_parameters = {
        'mean': float
    }

    transform_data = {
        'x': types.List(float),
    }


class TestPipeline(unittest.TestCase):

    def test_basic(self):
        p = Pipeline([Pipe1(), Pipe2()])

        self.assertEqual(p.check_fit({'x': ['1']}, {'1': {'unused': 1.0}}), [])
        self.assertEqual(p.check_transform({'x': ['1']}), [])
        self.assertEqual(p.fitted_parameters, {'0': {}, '1': {'mean': float, 'var': float}})
        self.assertEqual(p.transform_data, {'x': types.List(str)})
        self.assertEqual(p.fit_data, {'x': types.List(str)})

        p.fit({'x': ['1', '2', '3']}, {'1': {'unused': 1.0}})
        result = p.transform({'x': ['1', '2', '3']})

        # std([1,2,3]) == 0.816496580927726
        self.assertEqual(result['x'], [-1.2247448713915887, 0.0, 1.2247448713915887])

    def test_custom_parameters(self):
        p = Pipeline([('1', Pipe1()), ('2', Pipe2())])

        p.fit({'x': ['1', '2', '3']}, {'2': {'unused': 1.0}})
        result = p.transform({'x': ['1', '2', '3']})

        # std([1,2,3]) == 0.816496580927726
        self.assertEqual(result['x'], [-1.2247448713915887, 0.0, 1.2247448713915887])

    def test_custom_init(self):
        pipes = collections.OrderedDict([('1', Pipe1()), ('2', Pipe2())])
        p = Pipeline(pipes)

        self.assertEqual(p.pipes, pipes)

        with self.assertRaises(TypeError):
            Pipeline([('1', 1)])

        with self.assertRaises(TypeError):
            Pipeline(Pipe1())

    def test_two_transform_data(self):
        # P1 needs 'x', P2 needs 'x1'
        p = Pipeline([Pipe1(), Pipe3(), Pipe2()])

        self.assertEqual(p.transform_data, {'x': types.List(str), 'x1': types.List(str)})

        self.assertEqual(p.fit_data, {'x': types.List(str), 'x1': types.List(str)})

    def test_two_fit_data(self):
        # P4 fit-needs 'x1', P2 fit-needs 'x' (float) => fit_data needs both on its first type-occurrence
        p = Pipeline([Pipe1(), Pipe4(), Pipe2()])

        self.assertEqual(p.fit_data, {'x': types.List(str), 'x1': types.List(float)})

        self.assertEqual(p.transform_data, {'x': types.List(str)})
