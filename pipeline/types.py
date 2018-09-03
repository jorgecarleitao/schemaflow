import pipeline.exceptions as _exceptions


class Type:
    """
    The base type of all types. Used to declare new types to be used in :class:`pipeline.pipe.Pipe`.

    The class attribute :attr:`requirements` (a set of strings) is used to define if using this type has
    package requirements (e.g. `numpy`).
    """
    requirements = {}  #: set of packages required for this type to be usable.

    def check_schema(self, instance: object):
        """
        Checks that the instance has the correct type or schema (composite types).

        :param instance:
        :return: a list of exceptions
        """
        raise NotImplementedError


class _LiteralType(Type):
    """
    A :class:`Type` that wraps literal types (e.g. ``float``). Used internally only.
    """
    def __init__(self, base_type):
        assert not isinstance(base_type, Type)
        self._base_type = base_type

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._base_type.__name__)

    @property
    def type(self):
        return self._base_type

    def check_schema(self, instance):
        if not isinstance(instance, self._base_type):
            return [_exceptions.WrongType(self._base_type, type(instance))]
        return []


class _DataFrame(Type):
    """
    Abstract pipeline representation of a DataFrame. See subclasses for Pandas and PySpark.
    """
    def __init__(self, schema: dict):
        """
        :param schema: dictionary of `(column_name, type)`.
        """
        self._schema = schema.copy()
        for column, base_type in self._schema.items():
            if not isinstance(base_type, Type):
                self._schema[column] = _LiteralType(base_type)

    @staticmethod
    def _own_type():
        """
        Returns the DataFrame's type
        """
        raise NotImplementedError

    @staticmethod
    def _get_schema(instance):
        """
        Return the DataFrame's schema from an instance
        """
        raise NotImplementedError

    def check_schema(self, instance):
        expected_type = self._own_type()

        if not isinstance(instance, expected_type):
            return [_exceptions.WrongType(expected_type, type(instance))]

        exceptions = []
        schema = self._get_schema(instance)
        for column in self._schema:
            if column not in schema:
                exceptions.append(_exceptions.WrongSchema(column, set(schema.keys())))
            else:
                column_type = schema[column]
                expected_type = self._schema[column].type
                if expected_type != column_type:
                    exceptions.append(_exceptions.WrongType(
                        expected_type, column_type, 'column \'%s\'' % column))

        return exceptions


class PySparkDataFrame(_DataFrame):
    """
    Representation of a pyspark.sql.DataFrame. Requires ``pyspark``.
    """
    requirements = {'pyspark'}

    @staticmethod
    def _own_type():
        import pyspark.sql
        return pyspark.sql.DataFrame

    @staticmethod
    def _get_schema(instance):
        import pyspark.sql.types
        import numpy
        mapping = {
            pyspark.sql.types.LongType: int,
            pyspark.sql.types.DoubleType: float,
            pyspark.sql.types.StringType: numpy.dtype('O')
        }
        return dict((f.name, mapping[type(f.dataType)]) for f in instance.schema.fields)


class PandasDataFrame(_DataFrame):
    """
    Representation of a pandas.DataFrame. Requires ``pandas``.
    """
    requirements = {'pandas'}

    @staticmethod
    def _own_type():
        import pandas
        return pandas.DataFrame

    @staticmethod
    def _get_schema(instance):
        return dict((column, column_dtype.type) for column, column_dtype in instance.dtypes.items())


class _Container(Type):
    _own_type = _LiteralType(list)

    def __init__(self, items_type):
        if not isinstance(items_type, Type):
            items_type = _LiteralType(items_type)
        self._items_type = items_type

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._items_type.type.__name__)

    def check_schema(self, instance):
        exceptions = self._own_type.check_schema(instance)
        if not exceptions:
            for i, item in enumerate(instance):
                exceptions += self._items_type.check_schema(item)
        return exceptions


class List(_Container):
    _own_type = _LiteralType(list)


class Tuple(_Container):
    _own_type = _LiteralType(tuple)


class Array(_Container):
    """
    Representation of a numpy.array. Requires ``numpy``.
    """
    requirements = {'numpy'}

    def __init__(self, items_type: type, shape=None):
        import numpy
        assert issubclass(items_type, (numpy.generic, float, int, bool))
        if items_type == float:
            items_type = numpy.float64
        super().__init__(_LiteralType(items_type))
        assert isinstance(shape, (type(None), tuple))
        self._shape = shape

    def check_schema(self, instance):
        exceptions = self._own_type.check_schema(instance)
        if not exceptions:
            assert hasattr(instance, 'dtype')
            if instance.dtype == self._items_type.type and hasattr(instance, 'shape'):
                if not self._is_valid_shape(instance.shape):
                    exceptions.append(_exceptions.WrongShape(self._shape, instance.shape))
            else:
                exceptions.append(_exceptions.WrongType(self._items_type.type, instance.dtype))
        return exceptions

    def _is_valid_shape(self, shape):
        if shape is not None:
            if len(shape) != len(self._shape):
                return False
            for required_d, d in zip(self._shape, shape):
                if required_d is not None and d != required_d:
                    return False
        return True

    @property
    def _own_type(self):
        import numpy
        return _LiteralType(numpy.ndarray)
