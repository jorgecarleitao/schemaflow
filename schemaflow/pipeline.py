import collections

import schemaflow.pipe


class Pipeline(schemaflow.pipe.Pipe):
    """
    A linear sequence of :class:`~schemaflow.pipe.Pipe`'s that operate in sequence.

    :class:`~schemaflow.schemaflow.Pipeline` is a :class:`~schemaflow.pipe.Pipe` and can be part of
    another :class:`~schemaflow.schemaflow.Pipeline`.

    :param pipes: a ``list`` or ``OrderedDict`` of :attr:`~schemaflow.pipe.Pipe`.
        If passed as a list, you can either pass pipes or tuples ``(name, Pipe)``.
    """
    def __init__(self, pipes):
        super().__init__()
        assert len(pipes) > 0

        if isinstance(pipes, collections.OrderedDict):
            self.pipes = pipes
            return
        elif not isinstance(pipes, list):
            raise TypeError('Pipes must a list or OrderedDict')

        #: An ``OrderedDict`` whose keys are the pipe's names or ``str(index)`` where ``index`` is the pipe's
        #: position in the sequence and the values are :attr:`~schemaflow.pipe.Pipe`'s.
        self.pipes = collections.OrderedDict()
        for i, item in enumerate(pipes):
            if isinstance(item, tuple):
                assert len(item) == 2 and isinstance(item[1], schemaflow.pipe.Pipe) and \
                       isinstance(item[0], str) and '/' not in item[0]
                self.pipes[item[0]] = item[1]
            elif isinstance(item, schemaflow.pipe.Pipe):
                self.pipes[str(i)] = item
            else:
                raise TypeError('Items must be pipes or a tuple with `(str, Pipe)`')

    @property
    def fit_data(self):
        """
        The :attr:`~schemaflow.pipe.Pipe.fit_data` of the first pipe of the schemaflow.
        """
        return self.pipes[0].fit_data

    @property
    def transform_data(self):
        """
        The :attr:`~schemaflow.pipe.Pipe.transform_data` of the first pipe of the schemaflow.
        """
        return self.pipes[0].transform_data

    @property
    def transform_modifies(self):
        """
        The :attr:`~schemaflow.pipe.Pipe.transform_modifies` of the last pipe of the schemaflow.
        """
        return self.pipes[-1].transform_modifies

    @property
    def fitted_parameters(self):
        """
        Parameters assigned to fit of each pipe.

        :return: a dictionary with the pipe's name and their respective :attr:`~schemaflow.pipe.Pipe.fitted_parameters`.
        """
        fitted_parameters = {}
        for name, pipe in self.pipes.items():
            fitted_parameters[name] = pipe.fitted_parameters
        return fitted_parameters

    @property
    def requirements(self):
        """
        :return: the union of all :attr:`~schemaflow.pipe.Pipe.requirements` of all pipes in the schemaflow.
        """
        requirements = set()
        for pipe in self.pipes:
            requirements = requirements.union(pipe.requirements)
        return requirements

    def check_transform(self, data: dict=None):
        errors = []
        for key, pipe in self.pipes.items():
            errors += pipe.check_transform(data)
            data = pipe.apply_transform_schema(data)
        return errors

    def check_fit(self, data: dict, parameters: dict=None):
        if parameters is None:
            parameters = {}

        errors = []
        for key, pipe in self.pipes.items():
            if key in parameters:
                errors += pipe.check_fit(data, parameters[key])
            else:
                errors += pipe.check_fit(data, parameters)
            data = pipe.apply_transform_schema(data)
        return errors

    def transform(self, data: dict):
        for key, pipe in self.pipes.items():
            data = pipe.transform(data)
        return data

    def fit(self, data: dict, parameters: dict=None):
        """
        Fits all pipes in sequence.
        """
        if parameters is None:
            parameters = {}
        for key, pipe in self.pipes.items():
            if key in parameters:
                pipe.fit(data, parameters[key])
            else:
                pipe.fit(data)
            data = pipe.transform(data)
