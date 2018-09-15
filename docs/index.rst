Schematic data pipelines in Python
==================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

This is a package to write robust pipelines for data science and data engineering in Python 3.
Thanks for checking it out.

A major challenge in creating a robust data pipeline is guaranteeing interoperability between
pipes. Data transformations often change the underlying data representation (e.g. change column type,
add columns, convert PySpark DataFrame to Pandas or H2O DataFrames). This makes it difficult to track
what exactly is going on at a certain point of the pipeline, which often requires running the whole pipeline
until that point to debug a certain pipe.

This package declares a simple API to define data transformations that know what schema they require to run,
what schema they return, and what states they depend on.

Under this API, you define a :class:`~schemaflow.pipe.Pipe` as follows (an example):

.. code-block:: python

    from pipeline import pipe, types


    class MyPipe(pipe.Pipe):
        requirements = {'sklearn'}

        fit_requires = {
            # (arbitrary items, arbitrary features)
            'x': types.Array(np.float64, shape=(None, None)),
            'y': types.List(float)
        }

        transform_requires = {
            'x': types.List(float)
        }

        fit_parameters = {
            'gamma': float
        }

        # parameter assigned in fit; the pipe's state
        fitted_parameters = {
            'a': float
        }

        # type and key of transformed data
        transform_modifies = {
            'b': float
        }

        def fit(self, data, parameters=None):
            # accesses data['x'], data['y'] and parameters['gamma']; expects the types defined above
            # assigns a float to self['a']

        def transform(self, data):
            # assigns a float to data['b']
            return data

Without reading nor executing ``fit`` and ``transform``, we know how ``data`` will flow through this pipe:

1. it requires an ``'x'`` and ``'y'`` and a parameter ``gamma`` in fit
2. it is stateful through ``a``
3. it transforms ``data['b']``.

This allows to check whether a ``Pipeline`` is consistent **without** executing
``fit`` or ``transform`` of *any* pipe.

Specifically, you can execute the pipe using the traditional fit-transform idiom,

.. code-block:: python

    p = MyPipe()
    p.fit(train_data, {'gamma': 1.0})
    result = p.transform(test_data)

but also check whether the data format that you pass is consistent with its requirements:

.. code-block:: python

    p = MyPipe()
    exceptions_fit = p.check_fit({'x': 1}, {'gamma': 1.0})
    assert len(exceptions_fit) > 0

    exceptions_transform = p.check_transform({'x': 1})
    assert len(exceptions_transform) > 0

which does not execute ``fit`` nor ``transform``.

The biggest advantage of this declaration is that when the pipes are used within a pipeline,
**Schemaflow** can compute how the schema flows and therefore know the schema flow of a Pipeline:

.. code-block:: python

    p = schemaflow.pipeline.Pipeline([
        ('fix_ids', PipeA()),
        ('join_tables_with_fix', PipeB()),
        ('featurize', Featurize1_Pipeline()),
        ('model', Model1_Pipeline()),
        ('export_metrics', Export_results_PDF_Pipeline()),
        ('export_metrics', PushResultsToCache())
    ])

    print(p.transform_modifies)

I.e. because we know how each Pipe modifies the schema, we can compute how the schema flows through it and
therefore obtain what are the dependencies of ``p`` and what it transforms.

When to use this package
^^^^^^^^^^^^^^^^^^^^^^^^

Use it when you are fairly certain that:

* there is the need for a complex data pipeline (e.g. more than 1 data source and different data types)
* the data transforms are expensive (e.g. Spark, Hive, SQL)
* your data pipeline aims to be maintainable and reusable (e.g. production code)

Pipe
----

.. automodule:: schemaflow.pipe
   :members:

Pipeline
--------

.. automodule:: schemaflow.pipeline
   :members:

Types
-----

.. automodule:: schemaflow.types
   :members:

Exceptions
----------

.. automodule:: schemaflow.exceptions
   :members:
