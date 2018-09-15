import os
import setuptools

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='schemaflow',
    author='Jorge C. Leitao',
    version='0.2.0',
    description='A package to write schema-aware data pipelines',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jorgecarleitao/schemaflow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
