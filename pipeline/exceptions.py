class PipelineError(Exception):
    pass


class MissingRequirement(Exception):
    def __init__(self, requirement):
        self.requirement = requirement


class WrongData(PipelineError):
    pass


class WrongParameter(PipelineError):
    pass


class WrongSchema(PipelineError):
    def __init__(self, expected_column, columns, location: str=''):
        self.expected_column = expected_column
        self.columns = columns
        self.location = location


class WrongType(PipelineError):
    def __init__(self, expected_type: type, base_type: type, location: str=''):
        self.expected_type = expected_type
        self.base_type = base_type
        self.location = location


class WrongShape(PipelineError):
    def __init__(self, expected_shape, shape, location: str=''):
        self.expected_shape = expected_shape
        self.shape = shape
        self.location = location
