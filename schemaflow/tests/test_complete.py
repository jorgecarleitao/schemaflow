import unittest

import numpy as np

from schemaflow import pipe, types, exceptions


class Pipe(pipe.Pipe):
    requirements = {'sklearn'}

    # variables required by fit (supervised learning)
    fit_data = {
        # (arbitrary items, arbitrary features)
        'x': types.Array(np.float64, shape=(None, None)),
        'y': types.List(float)
    }

    transform_data = {
        'x': types.List(float)
    }

    # parameter passed to fit()
    fit_parameters = {
        'alpha': float
    }

    # parameter assigned in fit()
    fitted_parameters = {
        'model': object
    }

    # type and key of transform
    transform_modifies = {
        'model': object
    }

    def fit(self, data, parameters=None):
        import sklearn.linear_model
        self['model'] = sklearn.linear_model.Lasso(parameters['alpha'])

        self['model'].fit(data['x'], data['y'])

    def transform(self, data):
        data['model'] = self['model']
        return data


class TestPipe(unittest.TestCase):

    def test_basic(self):
        p = Pipe()

        p.fit({'x': [[1.0], [2.0]], 'y': [1.0, 1.0]}, {'alpha': 0.1})
        data = p.transform({'df1': [[1.0], [2.0]]})

        self.assertEqual(data['model'].__class__.__name__, 'Lasso')

    def test_check_fit(self):
        p = Pipe()

        errors = p.check_fit({'x': np.array([[1.0], [2.0]]), 'z': [1.0, 1.0]}, {'alpha': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongData)

        errors = p.check_fit({'x': 1, 'y': []}, {'alpha': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongType)

        errors = p.check_fit({'x': np.array([[1.0], [2.0]]), 'y': [1.0, 1.0]}, {'alph': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongParameter)

    def test_check_transform(self):
        p = Pipe()

        errors = p.check_transform({'x': []})
        self.assertEqual(len(errors), 0)
