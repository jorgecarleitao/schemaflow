import unittest
import logging

import numpy as np

from schemaflow import pipe, types, exceptions


class MockLoggingHandler(logging.Handler):
    """Mock logging handler to check for expected logs."""
    # see https://stackoverflow.com/a/1049375/931303

    def __init__(self, *args, **kwargs):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())


class Pipe(pipe.Pipe):
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

    def setUp(self):
        logger = logging.getLogger()
        logger.level = logging.DEBUG
        self._handler = MockLoggingHandler()
        logger.addHandler(self._handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._handler)

    def test_fit_transform(self):
        p = Pipe()

        p.fit({'x': [[1.0], [2.0]], 'y': [1.0, 1.0]}, {'alpha': 0.1})
        data = p.transform({'df1': [[1.0], [2.0]]})

        self.assertEqual(data['model'].__class__.__name__, 'Lasso')

    def test_check_fit_parameters(self):
        p = Pipe()
        good_data = {'x': np.array([[1.0], [2.0]]), 'y': [1.0, 1.0]}

        errors = p.check_fit(good_data, {'alph': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongParameter)

        # use default parameter
        errors = p.check_fit(good_data)
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongParameter)

        with self.assertRaises(exceptions.WrongParameter) as e:
            p.check_fit(good_data, {'alph': 0.1}, raise_=True)
        self.assertIn('in fit', str(e.exception))

        errors = p.check_fit(good_data, {'alpha': 'a'})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongType)

        with self.assertRaises(exceptions.WrongType) as e:
            p.check_fit(good_data, {'alpha': 'a'}, raise_=True)
        self.assertIn('in parameter \'alpha\' of fit', str(e.exception))

    def test_check_fit(self):
        p = Pipe()

        bad_data_schema = {'x': np.array([[1.0], [2.0]]), 'z': [1.0, 1.0]}
        bad_data_type = {'x': 1, 'y': [1.0, 1.0]}
        good_data = {'x': np.array([[1.0], [2.0]]), 'y': [1.0, 1.0]}

        errors = p.check_fit(good_data, {'alpha': 0.1})
        self.assertEqual(len(errors), 0)

        errors = p.check_fit(bad_data_schema, {'alpha': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongSchema)

        with self.assertRaises(exceptions.WrongSchema) as e:
            p.check_fit(bad_data_schema, {'alpha': 0.1}, raise_=True)
        self.assertIn('in fit', str(e.exception))

        errors = p.check_fit(bad_data_type, {'alpha': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongType)

    def test_check_transform(self):
        p = Pipe()

        bad_data_schema = {'x1': np.array([[1.0], [2.0]])}
        bad_data_type = {'x': 1}
        good_data = {'x': [1.0]}

        errors = p.check_transform(good_data)
        self.assertEqual(len(errors), 0)

        errors = p.check_transform(bad_data_schema)
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongSchema)

        with self.assertRaises(exceptions.WrongSchema) as e:
            p.check_transform(bad_data_schema, raise_=True)
        self.assertIn('in transform', str(e.exception))

        errors = p.check_transform(bad_data_type)
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), exceptions.WrongType)

        with self.assertRaises(exceptions.WrongType) as e:
            p.check_transform(bad_data_type, raise_=True)
        self.assertIn('in argument \'x\' of transform', str(e.exception))

    def test_logged(self):
        p = Pipe()

        good_fit_data = {'x': np.array([[1.0, 2.0], [2.0, 1.0]]), 'y': [1.0, 2.0]}
        good_transform_data = {'x': [1.0]}

        p.logged_fit(good_fit_data, {'alpha': 0.1})
        self.assertEqual(self._handler.messages['error'], [])
        self.assertEqual(len(self._handler.messages['info']), 2)

        p.logged_transform(good_transform_data)

        self.assertEqual(self._handler.messages['error'], [])
        self.assertEqual(len(self._handler.messages['info']), 4)

        p = Pipe()
        with self.assertRaises(exceptions.NotFittedError) as e:
            p['model']
        self.assertIn('model', str(e.exception))

    def test_transform_schema(self):
        p = Pipe()

        self.assertEqual(p.transform_schema({'x': types.List(float)}), {
            'x': types.List(float),
            'model': types._LiteralType(object)})

        with self.assertRaises(exceptions.WrongSchema) as e:
            p.transform_schema({'y': types.List(float)})
        self.assertIn('in transform', str(e.exception))
