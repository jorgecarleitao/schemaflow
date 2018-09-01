import pipeline.exceptions as _exceptions


class Type:
    def check_schema(self, instance: object):
        """
        :param instance:
        :return: a list of exceptions
        """
        raise NotImplementedError


class LiteralType(Type):
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


class SparkDataFrame(Type):
    def check_schema(self, instance):
        import pyspark.sql
        return isinstance(instance, pyspark.sql.DataFrame)


class PandasDataFrame(Type):
    def __init__(self, schema: dict):
        self._schema = schema.copy()
        for column, base_type in self._schema.items():
            if not isinstance(base_type, Type):
                self._schema[column] = LiteralType(base_type)

    def check_schema(self, instance):
        import pandas
        if not isinstance(instance, pandas.DataFrame):
            return [_exceptions.WrongType(pandas.DataFrame, type(instance))]

        exceptions = []
        columns = set(instance.columns)
        for column in self._schema:
            if column not in columns:
                exceptions.append(_exceptions.WrongData(column, columns))
            elif not pandas.np.issubdtype(instance.dtypes[column], self._schema[column].type):
                exceptions.append(_exceptions.WrongType(self._schema[column].type, instance.dtypes[column]))

        return exceptions


class _Container(Type):
    _own_type = LiteralType(list)

    def __init__(self, items_type):
        if not isinstance(items_type, Type):
            items_type = LiteralType(items_type)
        self._items_type = items_type

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._items_type.type.__name__)

    @staticmethod
    def _check_type(item, expected_type):
        if hasattr(expected_type, 'check_schema'):
            return expected_type.check_schema(item)
        elif isinstance(item, expected_type):
            return [_exceptions.WrongType(expected_type, type(item))]
        return []

    def check_schema(self, instance):
        exceptions = self._own_type.check_schema(instance)
        if not exceptions:
            for i, item in enumerate(instance):
                if hasattr(self._items_type, 'check_schema'):
                    exceptions += self._items_type.check_schema(item)
                elif isinstance(item, self._items_type):
                    exceptions.append(_exceptions.WrongType(self._items_type, type(item)))
        return exceptions


class List(_Container):
    _own_type = LiteralType(list)


class Array(_Container):
    requirements = {'numpy'}

    def __init__(self, items_type: type, shape=None):
        import numpy
        assert issubclass(items_type, (numpy.generic, float, int, bool))
        super().__init__(LiteralType(items_type))
        assert isinstance(shape, (type(None), tuple))
        self._shape = shape

    def check_schema(self, instance):
        exceptions = self._own_type.check_schema(instance)
        if hasattr(instance, 'dtype'):
            if instance.dtype == self._items_type.type and hasattr(instance, 'shape'):
                if not self._is_valid_shape(instance.shape):
                    exceptions.append(_exceptions.WrongShape(self._shape, instance.shape))
            else:
                exceptions.append(_exceptions.WrongType(self._items_type.type, instance.dtype))
        else:
            exceptions.append(_exceptions.WrongType(self._items_type.type, type(instance)))
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
        return LiteralType(numpy.ndarray)


class Tuple(_Container):
    _own_type = LiteralType(tuple)
