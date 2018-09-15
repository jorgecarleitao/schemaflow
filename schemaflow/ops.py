import schemaflow.types


class Operation:
    """
    Declares a generic operation to the schema, used in :attr:`~schemaflow.pipe.Pipe.transform_modifies`.
    """
    def transform(self, key, schema):
        return schema


class Set(Operation):
    """
    Declares an operation that sets a new schema key.

    Not using an operation to define
    """
    def __init__(self, value_type):
        if not isinstance(value_type, schemaflow.types._LiteralType):
            value_type = schemaflow.types._LiteralType(value_type)
        self.value_type = value_type

    def transform(self, key, schema):
        schema[key] = self.value_type
        return schema

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.value_type)


class Drop(Operation):
    """
    Declares an operation that drops an existing schema key.
    """
    def transform(self, key, schema):
        return {k: value for k, value in schema.items() if k != key}

    def __repr__(self):
        return '%s()' % self.__class__.__name__


class ModifyDataFrame(Operation):
    """
    Declares an operation that modifies a schema from a DataFrame.
    """
    def __init__(self, ops):
        self.ops = ops

    def transform(self, key, schema):
        instance = schema.get(key, schemaflow.types._DataFrame({}))

        for column, op in self.ops.items():
            instance.schema = op.transform(column, instance.schema)
        return schema

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.ops)
