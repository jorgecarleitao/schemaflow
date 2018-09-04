import importlib.util

import pipeline.types
import pipeline.exceptions as _exceptions


class Pipe:
    """
    The base class of all pipes. This class represents a stateful data transformation.

    Data in this context consists of a Python dictionary whose each value is a type with some representation of data,
    either in-memory (e.g. ``float``, ``pandas.DataFrame``) of remote (e.g. ``pyspark.sql.DataFrame``, ``sqlalchemy``).

    A :class:`Pipe` is defined by:

    - a function :meth:`transform` that:

        - uses the keys :attr:`transform_modifies` from ``data``
        - uses the :attr:`state`
        - modifies the keys in :attr:`transform_modifies` in ``data``

    - a function :meth:`fit` that:

        - uses (training) keys :attr:`fit_data` from ``data``
        - uses (passed) :attr:`fit_parameters`
        - modifies the keys :attr:`fitted_parameters` in :attr:`state`

    - a set of :attr:`requirements` (a set of package names, e.g. ``{'pandas'}``) of the transformation

    All :attr:`transform_modifies` and :attr:`fit_data` have a :class:`~pipeline.types.Type` that can
    be used to check that the Pipe's input is consistent, with

        - :meth:`check_fit`
        - :meth:`check_transform`

    The existence of the requirements can be checked using

        - :meth:`check_requirements`

    The rational is that you can run ``check_*`` with access only to the data's schema.
    This is specially important when the pipeline is an expensive operation.
    """
    #: set of packages required by the Pipe.
    requirements = set()

    #: the data schema required in :meth:`~fit`;
    #: a dictionary ``str``: :class:`~pipeline.types.Type`.
    fit_data = {}

    #: the data schema required in :meth:`~transform`;
    #: a dictionary ``str``: :class:`~pipeline.type.Type`.
    transform_data = {}

    #: parameters' schema passed to :meth:`~fit`
    fit_parameters = {}

    #: schema of the parameters assigned in :meth:`~fit`
    fitted_parameters = {}

    #: type and key of :meth:`~transform`
    transform_modifies = {}

    def __init__(self):
        self.state = {}  #: A dictionary with the states of the Pipe. Use [] operator to access and modify it.

    def __setitem__(self, key, value):
        self.state.__setitem__(key, value)

    def __getitem__(self, key):
        return self.state.__getitem__(key)

    @staticmethod
    def _get_type(type):
        if not isinstance(type, pipeline.types.Type):
            type = pipeline.types._LiteralType(type)
        return type

    def check_requirements(self):
        """
        Checks for requirements.

        :return: a list of exceptions with missing requirements
        """
        requirements = self.requirements
        for type in self.fit_data.values():
            requirements.add(type)
        for type in self.transform_data.values():
            requirements.add(type)
        for type in self.fit_parameters.values():
            requirements.add(type)
        for type in self.fitted_parameters.values():
            requirements.add(type)

        exceptions = []
        for requirement in requirements:
            if importlib.util.find_spec(requirement) is None:
                exceptions.append(_exceptions.MissingRequirement(requirement))
        return exceptions

    def check_fit(self, data: dict, parameters: dict=None):
        """
        Checks that a given data has a valid schema to be used in :meth:`fit`.

        :param data:
        :param parameters:
        :return: a list of (subclasses of) :class:`~pipeline.exceptions.PipelineError` with all missing arguments.
        """
        if parameters is None:
            parameters = {}

        exceptions = []

        data_keys = set(data.keys())
        required_keys = set(self.fit_data.keys())
        if len(required_keys - data_keys):
            exceptions.append(
                _exceptions.WrongData('Missing arguments in fit:'
                                      '\nRequired arguments: %s\nPassed arguments:   %s' % (required_keys, data_keys))
            )

        for key in self.fit_data:
            expected_type = self._get_type(self.fit_data[key])
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
        """
        Checks that a given data has a valid schema to be used in :meth:`transform`.

        :param data:
        :return: a list of (subclasses of) :class:`pipeline.exceptions.PipelineError` with all missing arguments.
        """
        exceptions = []

        data_keys = set(data.keys())
        required_keys = set(self.transform_data.keys())
        if len(required_keys - data_keys):
            exceptions.append(
                _exceptions.WrongData('Missing arguments in transform:'
                                      '\nRequired arguments: %s\nPassed arguments:   %s' % (required_keys, data_keys))
            )

        for key in self.transform_data:
            expected_type = self._get_type(self.transform_data[key])
            if key in data:
                new_exceptions = expected_type.check_schema(data[key])
                for exception in new_exceptions:
                    exception.location = 'argument \'%s\' in transform' % key
        return exceptions

    def apply_transform_schema(self, data):
        for key, value in self.transform_modifies.items():
            if not isinstance(value, pipeline.types.Type):
                value = pipeline.types._LiteralType(value)
            data[key] = value
        return data

    def fit(self, data: dict, parameters: dict=None):
        """
        Modifies the instance's :attr:`state`.

        :param data:
        :param parameters:
        :return: None
        """
        pass

    def transform(self, data: dict):
        """
        Modifies the data keys identified in :attr:`transform_modifies`.

        :param data:
        :return: the modified data
        """
        return data
