class SchemaFlowError(Exception):
    """
    The base exception of Pipeline
    """
    def __init__(self, locations: list=None):
        if locations is None:
            locations = []
        self.locations = locations


class NotFittedError(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when someone tries to access a non-fitted parameter.
    """
    def __init__(self, pipe, key, locations: list=None):
        self.pipe = pipe
        self.key = key
        super().__init__(locations)

    def __str__(self):
        return 'The pipe \'%s\' %s needs to be fitted before its state \'%s\' is usable' % \
            (self.pipe.__class__.__name__, ' '.join(self.locations), self.key)


class MissingRequirement(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when a requirement is missing
    """
    def __init__(self, object_type: type, requirement: str, locations: list=None):
        super().__init__(locations)
        self.object_type = object_type
        self.requirement = requirement


class WrongSchema(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the schema of a datum is wrong (e.g. wrong shape)
    """
    def __init__(self, expected_columns, passed_columns, locations: list=None):
        super().__init__(locations)
        self.expected_columns = expected_columns
        self.passed_columns = passed_columns

    def __str__(self):
        return 'Missing arguments %s:'\
                '\nRequired arguments: %s\nPassed arguments:   %s' % \
               (' '.join(self.locations), self.expected_columns, self.passed_columns)


class WrongParameter(WrongSchema):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when unexpected parameters are passed to `fit`.
    """
    def __str__(self):
        return 'Incompatible arguments %s:'\
                '\nExpected arguments: %s\nPassed arguments:   %s' % \
               (' '.join(self.locations), self.expected_columns, self.passed_columns)


class WrongType(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the type of the datum is wrong
    """
    def __init__(self, expected_type, base_type, locations: list=None):
        super().__init__(locations)
        self.expected_type = expected_type
        self.base_type = base_type

    def __str__(self):
        return 'Wrong type %s:'\
                '\nRequired type: %s\nPassed type:   %s' % \
               (' '.join(self.locations), self.expected_type, self.base_type)


class WrongShape(SchemaFlowError):
    """
    :class:`~schemaflow.exceptions.SchemaFlowError` raised when the shape of the datum is wrong
    """
    def __init__(self, expected_shape, shape, locations: list=None):
        super().__init__(locations)
        self.expected_shape = expected_shape
        self.shape = shape

    def __str__(self):
        return 'Wrong shape %s:'\
                '\nRequired shape: %s\nPassed shape:   %s' % \
               (' '.join(self.locations), self.expected_shape, self.shape)
