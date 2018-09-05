import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='schemaflow',
    author='Jorge C. Leitao',
    version='0.1.0',
    description='a package to write data pipelines for data science systematically',
    long_description=README,
    packages=find_packages(exclude='tests'),
)
