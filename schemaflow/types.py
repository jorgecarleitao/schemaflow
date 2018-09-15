import datetime
import importlib.util

import schemaflow.exceptions as _exceptions


def _requirement_fulfilled(requirement: str):
    """
    Returns whether a requirement is fulfilled.

    :return: bool
    """
    return importlib.util.find_spec(requirement) is not None


def _all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _all_subclasses(c)])


def infer_schema(data: dict):
    subclasses = list(subclass for subclass in _all_subclasses(Type)
                      if subclass.requirements_fulfilled and not subclass.__name__.startswith('_'))

    schema = {}
    for key, value in data.items():
        for subclass in subclasses:
            if isinstance(value, subclass.base_type()):
                schema[key] = subclass.infer(value)
                break
        else:
            schema[key] = type(value)
    return schema


def _get_type(instance_type):
    if not isinstance(instance_type, Type):
        instance_type = _LiteralType(instance_type)
    return instance_type


class Type:
    """
    The base type of all types. Used to declare new types to be used in :class:`schemaflow.pipe.Pipe`.

    The class attribute :attr:`requirements` (a set of strings) is used to define if using this type has
    package requirements (e.g. `numpy`).
    """
    requirements = {}  #: set of packages required for this type to be usable.

    @classmethod
    def base_type(cls):
        """
        A class property that returns the underlying type of this Type.
        :return:
        """
        raise NotImplementedError

    @classmethod
    def infer(cls, instance):
        raise NotImplementedError

    @classmethod
    def requirements_fulfilled(cls):
        """
        Returns whether this Type has its requirements fulfilled.

        :return: bool
        """
        return all(_requirement_fulfilled(requirement) for requirement in cls.requirements)

    def _check_as_instance(self, instance: object, raise_: bool):
        raise NotImplementedError

    def _check_as_type(self, instance, raise_: bool):
        if type(instance) != type(self):
            exception = _exceptions.WrongType(instance, self)
            if raise_:
                raise exception
            return [exception]
        return []

    def check_schema(self, instance: object, raise_: bool=False):
        """
        Checks that the instance has the correct type and schema (composite types).

        :param instance: a datum in either its representation form or on its schema form.
        :param raise_:
        :return: a list of exceptions
        """
        if isinstance(instance, Type):
            return self._check_as_type(instance, raise_)
        else:
            return self._check_as_instance(instance, raise_)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class _LiteralType(Type):
    """
    A :class:`Type` that wraps literal types (e.g. ``float``). Used internally only.
    """
    def __init__(self, base_type):
        assert not isinstance(base_type, Type)
        assert not isinstance(base_type, (list, tuple))
        self._base_type = base_type

    def __repr__(self):
        return 'L(%s)' % self._base_type

    @property
    def base_type(self):
        return self._base_type

    def _check_as_instance(self, instance: object, raise_: bool):
        if not isinstance(instance, self._base_type):
            exception = _exceptions.WrongType(self._base_type, type(instance))
            if raise_:
                raise exception
            return [exception]
        return []


class _DataFrame(Type):
    """
    Abstract schemaflow representation of a DataFrame. See subclasses for Pandas and PySpark.
    """
    def __init__(self, schema: dict):
        """
        :param schema: dictionary of `(column_name, type)`.
        """
        self.schema = schema.copy()
        for column, base_type in self.schema.items():
            if not isinstance(base_type, Type):
                self.schema[column] = _LiteralType(base_type)

    def __getitem__(self, key):
        return self.schema[key]

    def __setitem__(self, key, value):
        self.schema[key] = value

    def __delitem__(self, key):
        del self.schema[key]

    @classmethod
    def infer(cls, instance):
        assert isinstance(instance, cls.base_type())
        return cls(schema=cls._get_schema(instance))

    @staticmethod
    def _get_schema(instance):
        """
        Return the DataFrame's schema from an instance
        """
        raise NotImplementedError

    def _check_schema(self, schema, raise_: bool):
        exceptions = []
        for column in self.schema:
            if column not in schema:
                exception = _exceptions.WrongSchema(column, set(schema.keys()))
                if raise_:
                    raise exception
                exceptions.append(exception)
            else:
                column_type = schema[column].base_type
                expected_type = self.schema[column].base_type
                if expected_type != column_type:
                    exception = _exceptions.WrongType(
                        expected_type, column_type, 'column \'%s\'' % column)
                    if raise_:
                        raise exception
                    exceptions.append(exception)
        return exceptions

    def _check_as_type(self, instance, raise_: bool):
        exceptions = super()._check_as_type(instance, raise_)
        if not exceptions:
            exceptions += self._check_schema(instance.schema, raise_)
        return exceptions

    def _check_as_instance(self, instance: object, raise_: bool):
        if not isinstance(instance, self.base_type()):
            exception = _exceptions.WrongType(self.base_type(), type(instance))
            if raise_:
                raise exception
            return [exception]
        return self._check_schema(self._get_schema(instance), raise_)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.schema)


class PySparkDataFrame(_DataFrame):
    """
    Representation of a pyspark.sql.DataFrame. Requires ``pyspark``.
    """
    requirements = {'pyspark'}

    @classmethod
    def base_type(cls):
        import pyspark.sql
        return pyspark.sql.DataFrame

    @staticmethod
    def _get_schema(instance):
        import pyspark.sql.types
        import numpy
        mapping = {
            pyspark.sql.types.LongType: _LiteralType(int),
            pyspark.sql.types.DoubleType: _LiteralType(float),
            pyspark.sql.types.BooleanType: _LiteralType(bool),
            pyspark.sql.types.StringType: _LiteralType(numpy.dtype('O')),
            pyspark.sql.types.DateType: _LiteralType(datetime.date),
            pyspark.sql.types.TimestampType: _LiteralType(datetime.datetime),
        }
        return dict((f.name, mapping[type(f.dataType)]) for f in instance.schema.fields)


class PandasDataFrame(_DataFrame):
    """
    Representation of a pandas.DataFrame. Requires ``pandas``.
    """
    requirements = {'pandas'}

    @classmethod
    def base_type(cls):
        import pandas
        return pandas.DataFrame

    @staticmethod
    def _get_schema(instance):
        return dict((column, _get_type(column_dtype)) for column, column_dtype in instance.dtypes.items())


class _Container(Type):

    @classmethod
    def base_type(cls):
        return list

    def __init__(self, items_type):
        if not isinstance(items_type, Type):
            items_type = _LiteralType(items_type)
        self._items_type = items_type

    @classmethod
    def infer(cls, instance):
        assert isinstance(instance, cls.base_type())

        inferred_items_type = object
        if len(instance):
            inferred_items_type = type(instance[0])
            if any(type(item) != inferred_items_type for item in instance):
                inferred_items_type = object

        return cls(inferred_items_type)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._items_type.base_type.__name__)

    def _check_as_type(self, instance, raise_: bool):
        exceptions = super()._check_as_type(instance, raise_)
        if not exceptions and self._items_type.base_type != instance._items_type.base_type:
            exception = _exceptions.WrongType(self, instance)
            if raise_:
                raise exception
            exceptions += [exception]
        return exceptions

    def _check_as_instance(self, instance: object, raise_: bool):
        if not isinstance(instance, self.base_type()):
            exception = _exceptions.WrongType(self.base_type(), type(instance))
            if raise_:
                raise exception
            return [exception]

        exceptions = []
        if not exceptions:
            for i, item in enumerate(instance):
                exceptions += self._items_type.check_schema(item, raise_)
        return exceptions


class List(_Container):
    pass


class Tuple(_Container):
    @classmethod
    def base_type(cls):
        return tuple


class Array(_Container):
    """
    Representation of a numpy.array. Requires ``numpy``.
    """
    requirements = {'numpy'}

    def __init__(self, items_type: type, shape=None):
        import numpy
        assert isinstance(items_type, numpy.dtype) or issubclass(items_type, (numpy.generic, float, int, bool))
        if items_type == float:
            items_type = numpy.float64
        super().__init__(_LiteralType(items_type))
        assert isinstance(shape, (type(None), tuple))
        self.shape = shape

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self._items_type.base_type, self.shape)

    @classmethod
    def infer(cls, instance):
        assert isinstance(instance, cls.base_type())

        return cls(instance.dtype, instance.shape)

    def _check_as_type(self, instance, raise_: bool):
        exceptions = super()._check_as_type(instance, raise_)
        if not exceptions:
            if not self._is_valid_shape(instance.shape):
                exception = _exceptions.WrongShape(self.shape, instance.shape)
                if raise_:
                    raise exception
                exceptions.append(exception)
        return exceptions

    def _check_as_instance(self, instance: object, raise_: bool):
        if not isinstance(instance, self.base_type()):
            exception = _exceptions.WrongType(self.base_type(), type(instance))
            if raise_:
                raise exception
            return [exception]

        exceptions = []
        assert hasattr(instance, 'dtype')
        if instance.dtype == self._items_type.base_type and hasattr(instance, 'shape'):
            if not self._is_valid_shape(instance.shape):
                exception = _exceptions.WrongShape(self.shape, instance.shape)
                if raise_:
                    raise exception
                exceptions.append(exception)
        else:
            exception = _exceptions.WrongType(self._items_type.base_type, instance.dtype)
            if raise_:
                raise exception
            exceptions.append(exception)
        return exceptions

    def _is_valid_shape(self, shape):
        if self.shape is not None:
            if len(shape) != len(self.shape):
                return False
            for required_d, d in zip(self.shape, shape):
                if required_d is not None and d != required_d:
                    return False
        return True

    @classmethod
    def base_type(cls):
        import numpy
        return numpy.ndarray
