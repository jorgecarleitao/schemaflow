# Data Science Pipeline

This is a package to write systematic pipelines for data science
and data engineering in Python. Thanks for checking it out.

## The problem that this package solves

A major challenge in creating a robust pipeline is guaranteeing interoperability between
pipes. I.e. how do we guarantee that the pipe that someone wrote is compatible
with my pipeline *without* running the whole pipeline multiple times until I get it right?

## The solution this package adopts
 
This package declares an interface to define a stateful data transformation that gives 
the developer the opportunity to declare what comes in, what comes out, and what states are modified
on each pipe and therefore the whole pipeline.

## Install 

    # git clone the repository
    pip install .

## Run tests

    pip install -r requirements_tests.txt
    python -m unittest discover

## Build documentation

    pip install -r requirements_docs.txt
    cd docs && make html && cd ..
    open docs/build/html/index.html

## Use cases

1. You have a hadoop cluster with csv/etc., use PySpark to process them
into and fit a model. There are multiple processing steps developed by many people.

## Caveats

Note that while this does not solve all problems (e.g. Lasso predict requires the same 
number of fitted and transformed features), it solves an important set of them.
