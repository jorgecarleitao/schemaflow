import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='pipeline',
    version='0.0.1',
    description='',
    long_description=README,
    packages=find_packages(exclude='tests'),
)
