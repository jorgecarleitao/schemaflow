# Dataflow

This is a a package to write data pipelines for data science systematically in Python.
Thanks for checking it out.

## The problem that this package solves

A major challenge in creating a robust data pipeline is guaranteeing interoperability between
pipes: how do we guarantee that the pipe that someone wrote is compatible
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

You have a hadoop cluster with csv/etc., use PySpark to process them
and fit a model. There are multiple processing steps developed by many people.
