import collections
import logging

import schemaflow.pipe
import schemaflow.types
import schemaflow.exceptions as _exceptions

logger = logging.getLogger('schemaflow')
logger.setLevel(logging.DEBUG)


class Pipeline(schemaflow.pipe.Pipe):
    """
    A list of :class:`~schemaflow.pipe.Pipe`'s that are applied sequentially.

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
    def fit_requires(self):
        """
        The data schema required in :meth:`~fit`.
        """
        fit_schema = {}
        data = {}
        for key, pipe in self.pipes.items():
            # not in data => a previous pipe already added this key
            # not in transform_requires => previous pipe already required this key
            if pipe.fit_requires:
                required_data = pipe.fit_requires
            else:
                required_data = pipe.transform_requires

            new_keys = dict((key, datum_type) for key, datum_type in required_data.items()
                            if key not in data and key not in fit_schema)
            fit_schema.update(new_keys)
            data = pipe._transform_schema(data)

        return fit_schema

    @property
    def transform_requires(self):
        """
        The data schema required in :meth:`~transform`.
        """
        transform_data = {}
        data = {}
        for key, pipe in self.pipes.items():
            # not in data => a previous pipe already added this key
            # not in transform_requires => previous pipe already required this key
            new_keys = dict((key, datum_type) for key, datum_type in pipe.transform_requires.items()
                            if key not in data and key not in transform_data)
            transform_data.update(new_keys)
            data = pipe._transform_schema(data)

        return transform_data

    @property
    def transform_modifies(self):
        """
        The schema modifications that this Pipeline apply in ``transform``.

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

    @property
    def requirements(self):
        """
        Set of packages required by the Pipeline. The union of all
        :attr:`~schemaflow.pipe.Pipe.requirements` of all pipes in the Pipeline.
        """
        requirements = set()
        for pipe in self.pipes.values():
            requirements = requirements.union(pipe.requirements)
        return requirements

    def check_transform(self, data: dict=None, raise_: bool=False):
        errors = []
        for key, pipe in self.pipes.items():
            try:
                errors += pipe.check_transform(data, raise_)
            except _exceptions.SchemaFlowError as e:
                e.locations.append('of pipe \'%s\' of %s' % (key, self.__class__.__name__))
                raise e

            data = pipe._transform_schema(data)
        return errors

    def check_fit(self, data: dict, parameters: dict=None, raise_: bool=False):
        if parameters is None:
            parameters = {}

        errors = []
        for key, pipe in self.pipes.items():
            try:
                if key in parameters:
                    errors += pipe.check_fit(data, parameters[key], raise_)
                else:
                    errors += pipe.check_fit(data, {}, raise_)
            except _exceptions.SchemaFlowError as e:
                e.locations.append('of pipe \'%s\' of %s' % (key, self.__class__.__name__))
                raise e

            try:
                errors += pipe.check_transform(data, raise_)
            except _exceptions.SchemaFlowError as e:
                e.locations.append('of pipe \'%s\' of %s' % (key, self.__class__.__name__))
                raise e

            data = pipe._transform_schema(data)
        return errors

    def transform(self, data: dict):
        """
        Applies each of :meth:`~schemaflow.pipe.Pipe.transform` sequentially into ``data``.

        :param data: a dictionary of pairs ``str, object``.
        :return: the transformed data.
        """
        for key, pipe in self.pipes.items():
            data = pipe.transform(data)
        return data

    def transform_schema(self, schema: dict):
        for key, pipe in self.pipes.items():
            try:
                pipe.check_transform(schema, True)
            except _exceptions.SchemaFlowError as e:
                e.locations.append('of pipe \'%s\' of %s' % (key, self.__class__.__name__))
                raise e
            schema = pipe.transform_schema(schema)
        return schema

    def fit(self, data: dict, parameters: dict=None):
        """
        Fits the :attr:`pipes` in sequence: ``p1.fit``, ``p1.transform``, ``p2.fit``, ``p2.transform``,
        ..., ``pN.transform``.

        :param data: a dictionary of pairs ``(str, object)``.
        :param parameters: a dictionary ``{pipe_name: {str: object}}``, where each of its value is the parameters
            to be passed to the respective's pipe named ``pipe_name``.
        :return: ``None``
        """
        if parameters is None:
            parameters = {}
        for key, pipe in self.pipes.items():
            if key in parameters:
                pipe.fit(data, parameters[key])
            else:
                pipe.fit(data)
            data = pipe.transform(data)

    def _logged_transform(self, key, data):
        input_schema = schemaflow.types.infer_schema(data)
        logger.info('Started transform \'%s\' (%s): %s' % (key, self.pipes[key].__class__.__name__, input_schema))

        for error in self.pipes[key].check_transform(input_schema):
            error.locations.append('in %s' % key)
            logger.error(str(error))

        data = self.pipes[key].transform(data)
        output_schema = schemaflow.types.infer_schema(data)

        for error in self.pipes[key].check_transform_modifies(input_schema.copy(), output_schema):
            error.locations.append('in %s' % key)
            logger.error(str(error))

        logger.info('Ended   transform \'%s\' (%s): %s' % (key, self.pipes[key].__class__.__name__, output_schema))
        return data

    def logged_transform(self, data: dict):
        """
        Performs the same operation as :meth:`transform` while logging the schema on each intermediary step.

        It also logs schema inconsistencies as errors. Specifically, for each pipe, it checks if its input data
        is consistent with its :attr:`~schemaflow.pipes.Pipe.transform_requires`, and whether its output data
        is consistent with its :attr:`~schemaflow.pipes.Pipe.transform_modifies`.

        This greatly helps the Pipeline developer to identify problems in the pipeline.

        :param data: a dictionary of pairs ``str`` :class:`~schemaflow.types.Type`.
        :return: the transformed data.
        """
        for key in self.pipes:
            data = self._logged_transform(key, data)
        return data

    def logged_fit(self, data: dict, parameters: dict = None):
        """
        Performs the same operation as :meth:`fit` while logging the schema on each intermediary step.

        It also logs schema inconsistencies as errors. Specifically, for each pipe, it checks if its input data
        is consistent with its :attr:`~schemaflow.pipes.Pipe.fit_requires`, and whether its state changes is consistent
        with its :attr:`~schemaflow.pipes.Pipe.fitted_parameters`.

        This greatly helps the Pipeline developer to identify problems in the pipeline.

        :param data: a dictionary of pairs ``(str, object)``.
        :param parameters: a dictionary of pairs ``(str, object)``.
        :return: ``None``
        """
        if parameters is None:
            parameters = {}
        for key, pipe in self.pipes.items():
            schema = schemaflow.types.infer_schema(data)
            logger.info('Started fit \'%s\' (%s): %s' % (key, self.pipes[key].__class__.__name__, schema))

            if key in parameters:
                pipe.fit(data, parameters[key])
            else:
                pipe.fit(data)
            state_schema = dict((key, type(value)) for key, value in pipe.state.items())
            logger.info('Ended   fit \'%s\' (%s): state=%s' % (key, self.pipes[key].__class__.__name__, state_schema))
            data = self._logged_transform(key, data)
