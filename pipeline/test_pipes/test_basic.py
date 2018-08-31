import unittest

from pipeline import pipe, types


class Pipe(pipe.BasePipe):

    fit_placeholders = {
        'x': types.List(float),
        'y': types.List(float)
    }

    placeholders = {
        'x': types.List(float)
    }

    # parameter assigned in fit()
    fit_parameters = {
        'alpha': float
    }

    # parameter assigned in fit()
    fitted_parameters = {
        'coefficients': types.List(float)
    }

    # a parameter passed to fit()
    alpha = pipe.Parameter(float)


    coefficients_ = pipe.FittedParameter(types.List(float))

    # a variable used in transform
    df1 = pipe.Placeholder(types.List(float))

    # the output type of transform
    output = pipe.Output(types.List(float))

    # variables required by fit (supervised learning)
    x = pipe.FitPlaceholder(types.List(float))
    y = pipe.FitPlaceholder(types.List(float))

    def fit(self, data, parameters=None):
        self.coefficients_ = [1.0, 1.0, parameters['alpha']]

    def transform(self, data):
        data['output'] = self.coefficients_
        return data


class TestPipe(unittest.TestCase):

    def test_basic(self):
        p = Pipe()

        p.fit({'x': [1.0, 2.0], 'y': [1.0, 1.0]}, {'alpha': 0.1})

        self.assertEqual(p.transform({'x': [1.0, 2.0]}),
                         {'x': [1.0, 2.0], 'output': [1.0, 1.0, 0.1]})

        errors = p.check_fit({'x': [], 'z': []}, {'alpha': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), pipe.WrongData)

        errors = p.check_fit({'x': 1, 'y': []}, {'alpha': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), pipe.WrongDataType)

        errors = p.check_fit({'x': [], 'y': []}, {'alph': 0.1})
        self.assertEqual(len(errors), 1)
        self.assertEqual(type(errors[0]), pipe.WrongParameter)

        errors = p.check_transform({'x': []})
        self.assertEqual(len(errors), 0)
