# Data Science Pipeline

This is a package to write systematic pipelines for data science
and data engineering in Python. Thanks for checking it out.

## What is a pipeline

A pipeline is a sequence of stateful transformations (pipes) that convert
a generic set of data (e.g. spark DFs and constants) into another set of  
data (e.g. pandas DFs and a ML model).

- Stateful because some pipes can have parameters fitted in data.

## The problem that this package solves

A major challenge in creating a robust pipeline is guaranteeing interoperability between
pipes. I.e. how do we guarantee that the pipe that someone wrote is compatible
with my pipeline *without* having access to data or having to run an expensive query
against a large data.

This is the halting problem and is unsolved in general.

## The solution this package adopts
 
There are good techniques to mitigate this problem, and this package 
formalizes one of such techniques: type checking.
Specifically, this package declares an "ORM" for data transformations:

```python
import pipeline.pipe as pipe


class Pipe(pipe.BasePipe):
    # a parameter passed to fit
    alpha = pipe.Parameter(float)

    # a parameter assigned in fit (an sklearn object in this case, see below)
    model_ = pipe.FittedParameter(object)

    # the output key and type of transform
    model = pipe.Output(object)

    # variables required by fit (supervised learning)
    x = pipe.FitPlaceholder(pipe.Array(float))
    y = pipe.FitPlaceholder(pipe.List(float))

    def fit(self, data, parameters=None):
        import sklearn.linear_model
        # pipe developer must guarantee that this obeys the declaration above
        self.model_ = sklearn.linear_model.Lasso(parameters['alpha'])

        self.model_.fit(data['x'], data['y'])

    def transform(self, data):
        # pipe developer must guarantee that this obeys the declaration above
        data['model'] = self.model_
        return data
```

With the declaration above, without actually executing `fit` or `transform`, 
we know how `data` will flow through this pipe:

1. it requires an `'x'` and `'y'` and a parameter `alpha` in fit
2. it is stateful through `model_`
3. it adds/updates `data['model']`.

This allows to check whether a `Pipeline` is consistent **without** executing
`fit` or `transform` of *any* pipe. I.e. it is a fast check.

```python
from pipeline import Pipeline

p = Pipeline([Resample_a(), Standardize(), ('model', Pipe())])
errors = p.check_fit({'a': numpy.array([[1.0], [2.0]]), 'b': [1.0, 1.0]},
                     {'model': {'alpha': 0.1}})
assert len(errors) == 0
```

Any errors explain if the flow `fit` is correct, i.e. if the sequence of fit, transform 
that the pipes declared match.

## Use cases

1. You have a hadoop cluster with csv/etc., use PySpark to process them
into and fit a model. There are multiple processing steps developed by many people.

## Caveats

Note that while this does not solve all problems (e.g. Lasso predict requires the same 
number of fitted and transformed features), it solves an important set of them.
