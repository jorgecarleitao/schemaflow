Robust data pipelines in Python
===============================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Introduction
------------

This is a package to write robust pipelines for data science and data engineering in Python.
Thanks for checking it out.

What is a pipeline?
^^^^^^^^^^^^^^^^^^^

A pipeline represents a sequence of stateful transformations (pipes) that convert
a generic set of data (e.g. spark DFs and constants) into another set of
data (e.g. pandas DFs and a ML model).

We use the word stateful because pipes can have parameters computed from training data.

The problem that this abstraction solves
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A major challenge in creating a robust data pipeline is guaranteeing interoperability between
pipes. I.e. how do we guarantee that the pipe that someone wrote is compatible
with my pipeline?

There are 2 common ways to address this:

1. Run the whole thing and hope that it runs
2. Read the pipe's source code and understand how to use it and what it expects

The problem of option 1. is that we face a halting problem: we may need to wait a large amount of time
(e.g. Spark jobs to be executed) to conclude whether the whole thing runs. This is the ultimate test,
but takes development time.

The problem of option 2. is that unless there is an agreement on how to write the pipes, different authors will have
different conventions and different ways to building the pipe.

The solution this package adopts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This package declares an interface to define a stateful data transformation that gives the developer the opportunity to
declare what comes in, what comes out, and what states are modified.

Under this interface, as a developer, you define a pipe as follows:

.. code-block:: python

    from pipeline import pipe, types


    class MyPipe(pipe.Pipe):
        requirements = {'sklearn'}

        # variables required by fit (supervised learning)
        fit_data = {
            # (arbitrary items, arbitrary features)
            'x': types.Array(np.float64, shape=(None, None)),
            'y': types.List(float)
        }

        # variables required by transform
        transform_data = {
            'x': types.List(float)
        }

        # parameter passed to fit()
        fit_parameters = {
            'gamma': float
        }

        # parameter assigned in fit()
        fitted_parameters = {
            'a': float
        }

        # type and key of transformed data
        transform_modifies = {
            'b': float
        }

        def fit(self, data, parameters=None):
            # accesses data['x'], data['y'] and parameters['a']; expects the types defined above
            # assigns a float to self['a']

        def transform(self, data):
            # assigns a float to data['b']
            return data

Without reading nor executing ``fit`` and ``transform``, we know how ``data`` will flow through this pipe:

1. it requires an ``'x'`` and ``'y'`` and a parameter ``gamma`` in fit
2. it is stateful through ``a``
3. it transforms ``data['b']``.

This allows to check whether a ``Pipeline`` is consistent **without** executing
``fit`` or ``transform`` of *any* pipe. I.e. it is a fast check.

Pipeline
--------

.. automodule:: pipeline.pipeline
   :members:

Pipe
----

.. automodule:: pipeline.pipe
   :members:

Types
-----

.. automodule:: pipeline.types
   :members:

Exceptions
----------

.. automodule:: pipeline.exceptions
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
