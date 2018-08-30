class Type:
    def is_valid_type(self, instance):
        raise NotImplementedError

    @staticmethod
    def _check_type(item, expected_type):
        if hasattr(expected_type, 'is_valid_type'):
            return expected_type.is_valid_type(item)
        else:
            return isinstance(item, expected_type)


class LiteralType(Type):
    def __init__(self, type):
        self._type = type

    @property
    def type(self):
        return self._type

    def is_valid_type(self, instance):
        return isinstance(instance, self._type)


class SparkDataFrame(Type):
    def is_valid_type(self, instance):
        import pyspark.sql
        return isinstance(instance, pyspark.sql.DataFrame)


class PandasDataFrame(Type):
    def is_valid_type(self, instance):
        import pandas as pd
        return isinstance(instance, pd.DataFrame)


class Container(Type):
    _own_type = None

    def __init__(self, items_type):
        if not isinstance(items_type, Type):
            items_type = LiteralType(items_type)
        self._items_type = items_type

    def is_valid_type(self, instance):
        return self._own_type.is_valid_type(instance) and \
               all(self._check_type(item, self._items_type) for item in instance)


class List(Container):
    _own_type = LiteralType(list)


class Array(Container):
    requirements = {'numpy'}

    def __init__(self, items_type, shape=None):
        import numpy
        assert issubclass(items_type, (numpy.generic, float, int, bool))
        super().__init__(items_type)
        assert isinstance(shape, (type(None), tuple))
        self._shape = shape

    def is_valid_type(self, instance):
        return self._own_type.is_valid_type(instance) and \
               instance.dtype == self._items_type.type and \
               self._is_valid_shape(instance.shape)

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


class Tuple(Container):
    _own_type = LiteralType(tuple)
