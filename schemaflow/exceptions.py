class SchemaFlowError(Exception):
    """
    The base exception of Pipeline
    """
    pass


class MissingRequirement(Exception):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when a requirement is missing
    """
    def __init__(self, requirement):
        self.requirement = requirement


class WrongData(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the data passed to `fit` or `transform` miss arguments.
    """
    pass


class WrongParameter(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when an unexpected parameter is passed to `fit`.
    """
    pass


class WrongSchema(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the schema of a datum is wrong (e.g. wrong shape)
    """
    def __init__(self, expected_column, columns, location: str=''):
        self.expected_column = expected_column
        self.columns = columns
        self.location = location


class WrongType(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the type of the datum is wrong
    """
    def __init__(self, expected_type, base_type, location: str=''):
        self.expected_type = expected_type
        self.base_type = base_type
        self.location = location


class WrongShape(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the shape of the datum is wrong
    """
    def __init__(self, expected_shape, shape, location: str=''):
        self.expected_shape = expected_shape
        self.shape = shape
        self.location = location
