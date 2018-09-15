import unittest
import logging
import collections

from schemaflow.pipeline import Pipeline
from schemaflow.pipe import Pipe
from schemaflow import types
from schemaflow import exceptions


class Pipe1(Pipe):
    requirements = {'a1'}

    transform_requires = {
        'x': types.List(str),
    }

    transform_modifies = {
        'x': types.List(float),
    }

    def transform(self, data: dict):
        data['x'] = [float(x_i) for x_i in data['x']]
        return data


class Pipe2(Pipe):
    requirements = {'a2'}

    transform_requires = {
        'x': types.List(float),
    }

    fit_requires = {
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
    transform_requires = {
        'x1': types.List(str),
        'x': types.List(float),
    }


class Pipe4(Pipe):
    fit_requires = {
        'x1': types.List(float),
    }

    fitted_parameters = {
        'mean': float
    }

    transform_requires = {
        'x': types.List(float),
    }


class PipeWrongTransform(Pipe):
    """
    Claims to convert to float, but converts to int
    """
    requirements = {'a1'}

    transform_requires = {
        'x': types.List(str),
    }

    transform_modifies = {
        'x': types.List(float),
    }

    def transform(self, data: dict):
        data['x'] = [int(x_i) for x_i in data['x']]
        return data


class TestPipeline(unittest.TestCase):

    def test_check_fit(self):
        p = Pipeline([Pipe1(), Pipe2()])

        # ok
        self.assertEqual(p.check_fit({'x': ['1']}, {'1': {'unused': 1.0}}), [])
        # ok with raise
        self.assertEqual(p.check_fit({'x': ['1']}, {'1': {'unused': 1.0}}, True), [])

        # not ok
        self.assertEqual(len(p.check_fit({'x': 1}, {'1': {'unused': 1.0}})), 1)

        # not ok with raise
        with self.assertRaises(exceptions.WrongType):
            p.check_fit({'x': [1]}, {'1': {'unused': 1.0}}, True)

        p = Pipeline([Pipe2()])
        with self.assertRaises(exceptions.WrongType):
            p.check_fit({'x': ['1']}, {'1': {'unused': 1.0}}, True)

    def test_check_transform(self):
        p = Pipeline([Pipe1(), Pipe2()])

        # ok
        self.assertEqual(p.check_transform({'x': ['1']}), [])
        # ok with raise
        self.assertEqual(p.check_transform({'x': ['1']}, True), [])

        # not ok
        self.assertEqual(len(p.check_transform({'x': 1})), 1)

        # not ok with raise
        with self.assertRaises(exceptions.WrongType):
            p.check_transform({'x': [1]}, True)

    def test_requirements(self):
        p = Pipeline([Pipe1(), Pipe2()])
        self.assertEqual(p.requirements, {'a1', 'a2'})

        self.assertEqual(len(p.check_requirements), 2)

    def test_basic(self):
        p = Pipeline([Pipe1(), Pipe2()])

        self.assertEqual(p.fitted_parameters, {'0': {}, '1': {'mean': float, 'var': float}})
        self.assertEqual(p.transform_requires, {'x': types.List(str)})
        self.assertEqual(p.fit_requires, {'x': types.List(str)})

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

        self.assertEqual(p.transform_requires, {'x': types.List(str), 'x1': types.List(str)})

        self.assertEqual(p.fit_requires, {'x': types.List(str), 'x1': types.List(str)})

        self.assertEqual(p.transform_modifies, {'x': types.List(float)})

    def test_transform_schema(self):
        # P1 needs 'x', P2 needs 'x1'
        p = Pipeline([Pipe1(), Pipe3(), Pipe2()])

        # 'x1' is passed along without modification
        self.assertEqual(p.transform_schema({'x': types.List(str), 'x1': types.List(str)}),
                         {'x': types.List(float), 'x1': types.List(str)})

        with self.assertRaises(exceptions.WrongSchema) as e:
            p.transform_schema({'y': types.List(str)})
        self.assertIn('in transform of pipe \'0\' of Pipeline', str(e.exception))

        with self.assertRaises(exceptions.WrongSchema) as e:
            p.transform_schema({'x': types.List(str)})
        self.assertIn('in transform of pipe \'1\' of Pipeline', str(e.exception))

    def test_two_fit_schema(self):
        # P4 fit-needs 'x1', P2 fit-needs 'x' (float) => fit_requires needs both on its first type-occurrence
        p = Pipeline([Pipe1(), Pipe4(), Pipe2()])

        self.assertEqual(p.transform_requires, {'x': types.List(str)})

        self.assertEqual(p.fit_requires, {'x': types.List(str), 'x1': types.List(float)})

        self.assertEqual(p.transform_modifies, {'x': types.List(float)})


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

    def reset(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())


class TestPipelineLogging(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger('schemaflow')
        self._handler = MockLoggingHandler()
        logger.addHandler(self._handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._handler)

    def test_logged(self):
        p = Pipeline([Pipe1(), Pipe2()])

        p.logged_fit({'x': ['1', '2', '3']}, {'1': {'unused': 1.0}})
        self.assertEqual(self._handler.messages['error'], [])
        self.assertEqual(len(self._handler.messages['info']), 8)

        result = p.logged_transform({'x': ['1', '2', '3']})
        self.assertEqual(self._handler.messages['error'], [])
        self.assertEqual(len(self._handler.messages['info']), 8 + 4)

        # std([1,2,3]) == 0.816496580927726
        self.assertEqual(result['x'], [-1.2247448713915887, 0.0, 1.2247448713915887])

    def test_logged_transform(self):
        p = Pipeline([Pipe1(), Pipe2()])
        p.fit({'x': ['1', '2', '3']}, {'1': {'unused': 1.0}})
        p.logged_transform({'x': ['1', '2', '3']})
        self.assertEqual(self._handler.messages['error'], [])
        self.assertEqual(len(self._handler.messages['info']), 4)

        self._handler.reset()

        p.logged_transform({'x': [1, 2, 3]})
        self.assertEqual(len(self._handler.messages['error']), 1)
        self.assertEqual(len(self._handler.messages['info']), 4)

        self._handler.reset()

        p = Pipeline([PipeWrongTransform(), Pipe2()])
        p.fit({'x': ['1', '2', '3']}, {'1': {'unused': 1.0}})
        p.logged_transform({'x': ['1', '2', '3']})
        # 2 errors: 1 transform_modify and 1 for wrong input to second pipe
        self.assertEqual(len(self._handler.messages['error']), 2)
        self.assertEqual(self._handler.messages['error'][0],
                         "Wrong type in result 'x' of modified data from transform in 0:\nRequired type: List(float)\nPassed type:   List(int)")
        self.assertEqual(self._handler.messages['error'][1],
                         "Wrong type in argument 'x' of transform in 1:\nRequired type: List(float)\nPassed type:   List(int)")
        self.assertEqual(len(self._handler.messages['info']), 4)
