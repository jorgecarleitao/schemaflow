import collections
import logging

import pipeline.types


logger = logging.getLogger(__name__)


class WrongData(Exception):
    pass


class WrongParameter(Exception):
    pass


class WrongDataType(WrongData):
    pass


class Column:
    def __init__(self, type):
        if not isinstance(type, pipeline.types.Type):
            type = pipeline.types.LiteralType(type)
        self._type = type

    @property
    def type(self):
        return self._type

    def is_valid_type(self, instance):
        return self.type.is_valid_type(instance)


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



class Meta:
    def __init__(self, placeholders, output, fit_placeholders, parameters, fitted_parameters, requirements):
        self.placeholders = placeholders
        self.output = output
        self.fit_placeholders = fit_placeholders
        self.parameters = parameters
        self.fitted_parameters = fitted_parameters
        self.requirements = requirements

    def check_fit_arguments(self, data, parameters):
        exceptions = []

        data_keys = set(data.keys())
        required_keys = set(self.fit_placeholders.keys())
        if len(required_keys - data_keys):
            exceptions.append(
                WrongData('Missing arguments in fit:'
                          '\nRequired arguments: %s\nPassed arguments:   %s' % (required_keys, data_keys))
            )

        for key in self.fit_placeholders:
            if key in data and not self.fit_placeholders[key].is_valid_type(data[key]):
                exceptions.append(
                    WrongDataType('Wrong type of argument \'%s\' in fit:'
                                  '\nExpected type: %s\nPassed type:   %s' % (
                        key, self.fit_placeholders[key].type, type(data[key])))
                )

        keys1 = set(parameters.keys())
        keys2 = set(self.parameters.keys())
        if keys1 != keys2:
            exceptions.append(
                WrongParameter('Unexpected or missing parameter in fit:'
                               '\nExpected parameter: %s\nPassed parameter:   %s' % (keys2, keys1))
            )
        for key in self.parameters:
            if key in parameters and not self.parameters[key].is_valid_type(parameters[key]):
                exceptions.append(
                    WrongDataType('Unexpected type of parameter \'%s\' in fit:'
                                  '\nExpected type: %s\nPassed type:   %s' % (
                        key, self.parameters[key].type, type(parameters[key])))
                )

        return exceptions

    def check_transform_arguments(self, data):
        exceptions = []

        data_keys = set(data.keys())
        required_keys = set(self.placeholders.keys())
        if len(required_keys - data_keys):
            exceptions.append(
                WrongData('Missing arguments in transform:'
                          '\nRequired arguments: %s\nPassed arguments:   %s' % (required_keys, data_keys))
            )

        for key in self.placeholders:
            if key in data and not self.placeholders[key].is_valid_type(data[key]):
                exceptions.append(
                    WrongDataType('Unexpected type of argument \'%s\' in transform:'
                                  '\nExpected type: %s\nPassed type:   %s' % (
                                      key, self.placeholders[key].type, type(data[key])))
                )
        return exceptions


class MetaPipe(type):
    def __new__(mcs, name, bases, attrs):
        parameters = {}
        fit_placeholders = {}
        fitted_parameters = {}
        placeholders = {}
        output = {}
        requirements = set()

        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, FitPlaceholder):
                fit_placeholders[attr_name] = attr_value
            elif isinstance(attr_value, FittedParameter):
                fitted_parameters[attr_name] = attr_value
            elif isinstance(attr_value, Parameter):
                parameters[attr_name] = attr_value
            elif isinstance(attr_value, Placeholder):
                placeholders[attr_name] = attr_value
            elif isinstance(attr_value, Output):
                output[attr_name] = attr_value
            elif attr_name == 'requirements':
                requirements = attr_value

        attrs.update({'_meta': Meta(placeholders, output, fit_placeholders,
                                    parameters, fitted_parameters, requirements)})
        return super(MetaPipe, mcs).__new__(mcs, name, bases, attrs)


class BasePipe(metaclass=MetaPipe):
    requirements = {}

    def check_transform(self, data):
        return self._meta.check_transform_arguments(data)

    def apply_transform_schema(self, data):
        for key, value in self._meta.output.items():
            data[key] = value.type
        return data

    def check_fit(self, data, parameters=None):
        if parameters is None:
            parameters = {}
        return self._meta.check_fit_arguments(data, parameters)

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
