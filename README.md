[![Build Status](https://travis-ci.org/jorgecarleitao/schemaflow.svg?branch=master)](https://travis-ci.org/jorgecarleitao/schemaflow)
[![Coverage Status](https://coveralls.io/repos/github/jorgecarleitao/schemaflow/badge.svg)](https://coveralls.io/github/jorgecarleitao/schemaflow)
[![Documentation Status](https://readthedocs.org/projects/schemaflow/badge/?version=latest)](https://schemaflow.readthedocs.io/en/latest/?badge=latest)

# SchemaFlow

This is a a package to write data pipelines for data science systematically in Python.
Thanks for checking it out.

Check out the very comprehensive documentation [here](https://schemaflow.readthedocs.io/en/latest/).

## The problem that this package solves

A major challenge in creating a robust data pipeline is guaranteeing interoperability between
pipes: how do we guarantee that the pipe that someone wrote is compatible
with others' pipe *without* running the whole pipeline multiple times until we get it right?

## The solution that this package adopts
 
This package declares an API to define a stateful data transformation that gives 
the developer the opportunity to declare what comes in, what comes out, and what states are modified
on each pipe and therefore the whole pipeline. Check out 
[`tests/test_pipeline.py`](https://github.com/jorgecarleitao/schemaflow/blob/master/tests/test_pipeline.py) or 
[`examples/end_to_end_kaggle.py`](https://github.com/jorgecarleitao/schemaflow/blob/master/examples/end_to_end_kaggle.py)

## Install 

    pip install schemaflow

or, install the latest (recommended for now):

    git clone https://github.com/jorgecarleitao/schemaflow
    cd schemaflow && pip install -e .

## Run examples

We provide one example that demonstrate the usage of SchemaFlow's API
on developing an end-to-end pipeline applied to 
[one of Kaggle's exercises](https://www.kaggle.com/c/house-prices-advanced-regression-techniques).

To run it, download the data in that exercise to `examples/all/` and run

    pip install -r examples/requirements.txt
    python examples/end_to_end_kaggle.py

You should see some prints to the console as well as the generation of 3 files at 
`examples/`: two plots and one `submission.txt`.

## Run tests

    pip install -r tests/requirements.txt
    python -m unittest discover

## Build documentation

    pip install -r docs/requirements.txt
    cd docs && make html && cd ..
    open docs/build/html/index.html
