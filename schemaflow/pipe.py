import logging
import importlib.util

import schemaflow.types
import schemaflow.ops
import schemaflow.exceptions as _exceptions


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _check_schema_keys(schema: dict, expected: dict, error_location: list, raise_: bool=False):
    schema_keys = set(schema.keys())
    required_keys = set(expected.keys())

    exceptions = []
    if len(required_keys - schema_keys):
        exception = _exceptions.WrongSchema(required_keys, schema_keys, error_location)
        if raise_:
            raise exception
        exceptions.append(exception)
    return exceptions


def _check_schema_types(schema: dict, expected: dict, error_location: str, raise_: bool=False):
    exceptions = []
    for key, expected_type in expected.items():
        if key in schema:
            try:
                new_exceptions = expected_type.check_schema(schema[key], raise_)
            except _exceptions.SchemaFlowError as e:
                e.locations.append(error_location % key)
                raise e
            for exception in new_exceptions:
                exception.locations.append(error_location % key)
            exceptions += new_exceptions
    return exceptions


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

    All :attr:`transform_modifies` and :attr:`fit_data` have a :class:`~schemaflow.types.Type` that can
    be used to check that the Pipe's input is consistent, with

        - :meth:`check_fit`
        - :meth:`check_transform`

    The existence of the requirements can be checked using

        - :meth:`check_requirements`

    The rational is that you can run ``check_*`` with access only to the data's schema.
    This is specially important when the schemaflow is an expensive operation.
    """
    #: set of packages required by the Pipe.
    requirements = set()

    #: the data schema required in :meth:`~fit`;
    #: a dictionary ``str``: :class:`~schemaflow.types.Type`.
    fit_data = {}

    #: the data schema required in :meth:`~transform`;
    #: a dictionary ``str``: :class:`~schemaflow.type.Type`.
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
        if key not in self.state:
            raise _exceptions.NotFittedError(self, key)
        return self.state.__getitem__(key)

    @staticmethod
    def _get_type(type):
        if not isinstance(type, schemaflow.types.Type):
            type = schemaflow.types._LiteralType(type)
        return type

    @property
    def check_requirements(self):
        """
        Checks for requirements.

        :return: a list of exceptions with missing requirements
        """
        requirements = self.requirements

        all_types = list(self.fit_data.values()) + list(self.transform_data.values()) + \
                    list(self.fit_parameters.values()) + list(self.fitted_parameters.values())

        for value_type in all_types:
            if isinstance(all_types, schemaflow.types.Type):
                requirements = requirements.union(value_type.requirements)

        exceptions = []
        for requirement in requirements:
            if importlib.util.find_spec(requirement) is None:
                exceptions.append(_exceptions.MissingRequirement(requirement))
        return exceptions

    def _check_transform_modifies(self, schema: dict):
        exceptions = _check_schema_keys(schema, self.transform_modifies, ['in modified data from transform'])

        expected_schema = {key: self._get_type(value) for key, value in self.transform_modifies.items()}
        exceptions += _check_schema_types(schema, expected_schema, 'in argument \'%s\' of modified data from transform')

        return exceptions

    def check_fit(self, data: dict, parameters: dict=None, raise_: bool=False):
        """
        Checks that a given data has a valid schema to be used in :meth:`fit`.

        :param data: a dictionary with either ``(str, Type)`` or ``(str, instance)``
        :param parameters: a dictionary with either ``(str, Type)`` or ``(str, instance)``
        :param raise_: whether it should raise the first found exception or list them all (default: list them)
        :return: a list of (subclasses of) :class:`~pipeline.exceptions.PipelineError` with all failed checks.
        """
        if parameters is None:
            parameters = {}

        exceptions = _check_schema_keys(data, self.fit_data, ['in fit'], raise_)

        expected_schema = {key: self._get_type(value) for key, value in self.fit_data.items()}
        exceptions += _check_schema_types(data, expected_schema, 'in argument \'%s\' of fit', raise_)

        # check that parameters are correct
        parameters_keys = set(parameters.keys())
        expected_parameters = set(self.fit_parameters.keys())
        if parameters_keys != expected_parameters:
            exception = _exceptions.WrongParameter(
                expected_parameters,
                parameters_keys, ['in fit'])
            if raise_:
                raise exception
            exceptions.append(exception)

        expected_schema = {key: self._get_type(value) for key, value in self.fit_parameters.items()}
        exceptions += _check_schema_types(parameters, expected_schema, 'in parameter \'%s\' of fit', raise_)

        return exceptions

    def check_transform(self, data: dict, raise_: bool=False):
        """
        Checks that a given data has a valid schema to be used in :meth:`transform`.

        :param data: a dictionary with either ``(str, Type)`` or ``(str, instance)``
        :param raise_: whether it should raise the first found exception or list them all (default: list them)
        :return: a list of (subclasses of) :class:`schemaflow.exceptions.SchemaFlowError` with all missing arguments.
        """
        exceptions = _check_schema_keys(data, self.transform_data, ['in transform'], raise_)

        expected_schema = {key: self._get_type(value) for key, value in self.transform_data.items()}
        exceptions += _check_schema_types(data, expected_schema, 'in argument \'%s\' of transform', raise_)
        return exceptions

    def _transform_schema(self, schema: dict):
        for key, value in self.transform_modifies.items():
            if isinstance(value, schemaflow.ops.Operation):
                schema = value.transform(key, schema)
            else:
                schema[key] = value
        return schema

    def transform_schema(self, schema: dict):
        """
        Transforms the ``schema`` into the new schema based on :attr:`~transform_modifies`.

        :param schema: a dictionary of pairs ``str`` :class:`~schemaflow.types.Type`.
        :return: the new schema.
        """
        self.check_transform(schema, True)
        return self._transform_schema(schema)

    def fit(self, data: dict, parameters: dict=None):
        """
        Modifies the instance's :attr:`state`.

        :param data: a dictionary of pairs ``(str, object)``.
        :param parameters: a dictionary of pairs ``(str, object)``.
        :return: ``None``
        """

    def transform(self, data: dict):
        """
        Modifies the data keys identified in :attr:`transform_modifies`.

        :param data: a dictionary of pairs ``(str, object)``.
        :return: the modified data
        """
        return data

    def logged_transform(self, data: dict):
        """
        Transforms the ``schema`` into the new schema based on :attr:`~transform_modifies`. Logs the intermediary
        steps using ``logging``.

        :param data: a dictionary of pairs ``str`` :class:`~schemaflow.types.Type`.
        :return: the new schema.
        """
        schema = dict((key, type(value)) for key, value in data.items())
        logger.info('In %s.transform(%s)' % (self.__class__.__name__, schema))

        for error in self.check_transform(data):
            logger.error(str(error))

        new_data = self.transform(data)

        for error in self._check_transform_modifies(new_data):
            logger.error(str(error))

        new_data_schema = dict((key, type(value)) for key, value in new_data.items())
        logger.info('Ended %s.transform(%s) with %s' % (self.__class__.__name__, schema, new_data_schema))
        return new_data

    def logged_fit(self, data: dict, parameters: dict = None):
        """
        Modifies the instance's :attr:`state`. Logs the intermediary steps using ``logging``.

        :param data: a dictionary of pairs ``(str, object)``.
        :param parameters: a dictionary of pairs ``(str, object)``.
        :return: ``None``
        """
        data_schema = dict((key, type(value)) for key, value in data.items())
        logger.info('In %s.fit(%s)' % (self.__class__.__name__, data_schema))
        self.fit(data, parameters)
        state_schema = dict((key, type(value)) for key, value in self.state.items())
        logger.info('Ended %s.fit(%s) with %s' % (self.__class__.__name__, data_schema, state_schema))
