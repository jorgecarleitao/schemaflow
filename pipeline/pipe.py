import collections
import logging

import pipeline.types
import pipeline.exceptions as _exceptions


logger = logging.getLogger(__name__)


class Column:
    def __init__(self, type):
        if not isinstance(type, pipeline.types.Type):
            type = pipeline.types.LiteralType(type)
        self._type = type

    @property
    def type(self):
        return self._type

    def is_valid_type(self, instance):
        return self.type.check_schema(instance)


class Parameter(Column):
    pass


class FittedParameter(Column):
    pass


class Placeholder(Column):
    pass


class Output(Column):
    pass


class FitPlaceholder(Column):
    pass


class BasePipe:
    requirements = {}

    # data required in fit
    fit_placeholders = {}

    # data used in transform
    placeholders = {}

    # parameter passed to fit()
    fit_parameters = {}

    # parameter assigned in fit()
    fitted_parameters = {}

    # type and key of transform
    result = {}

    def __init__(self):
        self.states = {}

    def __setitem__(self, key, value):
        self.states.__setitem__(key, value)

    def __getitem__(self, key):
        return self.states.__getitem__(key)

    @staticmethod
    def _get_type(type):
        if not isinstance(type, pipeline.types.Type):
            type = pipeline.types.LiteralType(type)
        return type

    def check_fit(self, data, parameters):
        exceptions = []

        data_keys = set(data.keys())
        required_keys = set(self.fit_placeholders.keys())
        if len(required_keys - data_keys):
            exceptions.append(
                _exceptions.WrongData('Missing arguments in fit:'
                                      '\nRequired arguments: %s\nPassed arguments:   %s' % (required_keys, data_keys))
            )

        for key in self.fit_placeholders:
            expected_type = self._get_type(self.fit_placeholders[key])
            if key in data:
                new_exceptions = expected_type.check_schema(data[key])
                for exception in new_exceptions:
                    exception.location = 'argument \'%s\' in fit' % key
                exceptions += new_exceptions

        keys1 = set(parameters.keys())
        keys2 = set(self.fit_parameters.keys())
        if keys1 != keys2:
            exceptions.append(
                _exceptions.WrongParameter('Unexpected or missing parameter in fit:'
                                           '\nExpected parameter: %s\nPassed parameter:   %s' % (keys2, keys1))
            )

        for key in self.fit_parameters:
            expected_type = self._get_type(self.fit_parameters[key])
            if key in parameters:
                new_exceptions = expected_type.check_schema(parameters[key])
                for exception in new_exceptions:
                    exception.location = 'parameter \'%s\' in fit' % key
                exceptions += new_exceptions

        return exceptions

    def check_transform(self, data):
        exceptions = []

        data_keys = set(data.keys())
        required_keys = set(self.placeholders.keys())
        if len(required_keys - data_keys):
            exceptions.append(
                _exceptions.WrongData('Missing arguments in transform:'
                                      '\nRequired arguments: %s\nPassed arguments:   %s' % (required_keys, data_keys))
            )

        for key in self.placeholders:
            expected_type = self._get_type(self.placeholders[key])
            if key in data:
                new_exceptions = expected_type.check_schema(data[key])
                for exception in new_exceptions:
                    exception.location = 'argument \'%s\' in transform' % key
        return exceptions

    def apply_transform_schema(self, data):
        for key, value in self.result.items():
            data[key] = value.type
        return data

    def fit(self, data, parameters=None):
        raise NotImplementedError

    def transform(self, data):
        raise NotImplementedError


class Pipeline:
    def __init__(self, pipes):
        if isinstance(pipes, collections.OrderedDict):
            self.pipes = pipes
            return
        elif not isinstance(pipes, list):
            raise TypeError('Pipes must a list or OrderedDict')

        self.pipes = collections.OrderedDict()
        for i, item in enumerate(pipes):
            if isinstance(item, tuple):
                assert len(item) == 2 and isinstance(item[1], BasePipe) and \
                       isinstance(item[0], str) and '/' not in item[0]
                self.pipes[item[0]] = item[1]
            elif isinstance(item, BasePipe):
                self.pipes[str(i)] = item
            else:
                raise TypeError('Items must be pipes or a tuple with `(str, Pipe)`')

    def check_transform(self, data=None):
        if data is None:
            data = {}

        errors = []
        for key, pipe in self.pipes.items():
            errors += pipe.check_transform(data)
            data = pipe.apply_transform_schema(data)
        return errors

    def check_fit(self, data=None, parameters=None):
        if data is None:
            data = {}
        if parameters is None:
            parameters = {}

        errors = []
        for key, pipe in self.pipes.items():
            if key in parameters:
                errors += pipe.check_fit(data, parameters[key])
            else:
                errors += pipe.check_fit(data, parameters)
            data = pipe.apply_transform_schema(data)
        return errors

    @property
    def requirements(self):
        requirements = set()
        for pipe in self.pipes:
            requirements.union(pipe.requirements)
        return requirements

    def transform(self, data: dict):
        for key, pipe in self.pipes.items():
            data = pipe.transform(data)
        return data

    def fit(self, data: dict, parameters=None):
        """
        Fits all pipes in sequence.
        """
        if parameters is None:
            parameters = {}
        for key, pipe in self.pipes.items():
            if key in parameters:
                pipe.fit(data, parameters[key])
            else:
                pipe.fit(data)
            data = pipe.transform(data)