import collections

import schemaflow.pipe
import schemaflow.exceptions as _exceptions


class Pipeline(schemaflow.pipe.Pipe):
    """
    A linear sequence of :class:`~schemaflow.pipe.Pipe`'s that operate in sequence.

    :class:`~schemaflow.pipeline.Pipeline` is a :class:`~schemaflow.pipe.Pipe` and can be part of
    another :class:`~schemaflow.pipeline.Pipeline`.

    :param pipes: a ``list`` or ``OrderedDict`` of :attr:`~schemaflow.pipe.Pipe`.
        If passed as a list, you can either pass pipes or tuples ``(name, Pipe)``.
    """
    def __init__(self, pipes):
        super().__init__()
        if isinstance(pipes, collections.OrderedDict):
            self.pipes = pipes
            return
        elif not isinstance(pipes, list):
            raise TypeError('Pipes must a list or OrderedDict')

        assert len(pipes) > 0
        #: An ``OrderedDict`` whose keys are the pipe's names or ``str(index)`` where ``index`` is the pipe's
        #: position in the sequence and the values are :attr:`~schemaflow.pipe.Pipe`'s.
        self.pipes = collections.OrderedDict()
        for i, item in enumerate(pipes):
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], schemaflow.pipe.Pipe) and \
                       isinstance(item[0], str):
                self.pipes[item[0]] = item[1]
            elif isinstance(item, schemaflow.pipe.Pipe):
                self.pipes[str(i)] = item
            else:
                raise TypeError('Items must be pipes or 2-element tuples of the form `(str, Pipe)`')

    @property
    def fit_data(self):
        """
        The data schema required in :meth:`~fit` of the whole Pipeline.
        """
        fit_data = {}
        data = {}
        for key, pipe in self.pipes.items():
            # not in data => a previous pipe already added this key
            # not in transform_data => previous pipe already required this key
            if pipe.fit_data:
                required_data = pipe.fit_data
            else:
                required_data = pipe.transform_data

            new_keys = dict((key, type) for key, type in required_data.items()
                            if key not in data and key not in fit_data)
            fit_data.update(new_keys)
            data = pipe._transform_schema(data)

        return fit_data

    @property
    def transform_data(self):
        """
        The data schema required in :meth:`~transform` of the whole Pipeline.
        """
        transform_data = {}
        data = {}
        for key, pipe in self.pipes.items():
            # not in data => a previous pipe already added this key
            # not in transform_data => previous pipe already required this key
            new_keys = dict((key, type) for key, type in pipe.transform_data.items()
                            if key not in data and key not in transform_data)
            transform_data.update(new_keys)
            data = pipe._transform_schema(data)

        return transform_data

    @property
    def transform_modifies(self):
        """
        All the modifications that this Pipeline apply during ``transform``.

        When a key is modified more than once, changes are appended as a list.
        """
        transform_modifies = {}
        for key, pipe in self.pipes.items():
            for key_1, op in pipe.transform_modifies.items():
                if key_1 in transform_modifies:
                    if not isinstance(transform_modifies[key_1], list):
                        if op != transform_modifies[key_1]:
                            transform_modifies[key_1] = [transform_modifies[key_1], op]
                    elif op != transform_modifies[key_1][-1]:
                        transform_modifies[key_1].append(op)
                else:
                    transform_modifies.update(pipe.transform_modifies)
        return transform_modifies

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

    def check_transform(self, data: dict=None, raise_: bool=False):
        errors = []
        for key, pipe in self.pipes.items():
            errors += pipe.check_transform(data, raise_)
            data = pipe.transform_schema(data)
        return errors

    def check_fit(self, data: dict, parameters: dict=None, raise_: bool=False):
        if parameters is None:
            parameters = {}

        errors = []
        for key, pipe in self.pipes.items():
            if key in parameters:
                errors += pipe.check_fit(data, parameters[key], raise_)
            else:
                errors += pipe.check_fit(data, {}, raise_)
            data = pipe.transform_schema(data)
        return errors

    def transform(self, data: dict):
        for key, pipe in self.pipes.items():
            data = pipe.transform(data)
        return data

    def transform_schema(self, schema: dict):
        for key, pipe in self.pipes.items():
            try:
                pipe.check_transform(schema, True)
            except _exceptions.SchemaFlowError as e:
                e.locations.append('in step %s of %s' % (key, self.__class__.__name__))
                raise e
            schema = pipe.transform_schema(schema)
        return schema

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
