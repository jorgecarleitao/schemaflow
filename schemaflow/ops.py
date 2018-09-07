import schemaflow.types


class Operation:
    """
    Declares a generic operation to the schema
    """
    def transform(self, key, schema):
        return schema


class Set(Operation):
    """
    Declares an operation that sets a new schema key.
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
        del schema[key]
        return schema

    def __repr__(self):
        return '%s()' % self.__class__.__name__


class ModifyDataFrame(Operation):
    """
    Declares an operation that modifies a schema from a DataFrame.
    """
    def __init__(self, ops):
        self.ops = ops

    def transform(self, key, schema):
        current_schema = schema.get(key, schemaflow.types._DataFrame({}))
        for column, op in self.ops.items():
            current_schema = op.transform(column, current_schema)
        schema[key] = current_schema
        return schema

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.ops)
