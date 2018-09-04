class PipelineError(Exception):
    """
    The base exception of Pipeline
    """
    pass


class MissingRequirement(Exception):
    """
    :class:`~pipeline.exceptions.PipelineError` raised when a requirement is missing
    """
    def __init__(self, requirement):
        self.requirement = requirement


class WrongData(PipelineError):
    """
    :class:`~pipeline.exceptions.PipelineError` raised when the data passed to `fit` or `transform` miss arguments.
    """
    pass


class WrongParameter(PipelineError):
    """
    :class:`~pipeline.exceptions.PipelineError` raised when an unexpected parameter is passed to `fit`.
    """
    pass


class WrongSchema(PipelineError):
    """
    :class:`~pipeline.exceptions.PipelineError` raised when the schema of a datum is wrong (e.g. wrong shape)
    """
    def __init__(self, expected_column, columns, location: str=''):
        self.expected_column = expected_column
        self.columns = columns
        self.location = location


class WrongType(PipelineError):
    """
    :class:`~pipeline.exceptions.PipelineError` raised when the type of the datum is wrong
    """
    def __init__(self, expected_type, base_type, location: str=''):
        self.expected_type = expected_type
        self.base_type = base_type
        self.location = location


class WrongShape(PipelineError):
    """
    :class:`~pipeline.exceptions.PipelineError` raised when the shape of the datum is wrong
    """
    def __init__(self, expected_shape, shape, location: str=''):
        self.expected_shape = expected_shape
        self.shape = shape
        self.location = location
