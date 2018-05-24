#!/usr/bin/env python3
from setuptools import setup

setup(name='radarstudy',
      version='0.1',
      description='Tools for use with RADAR-CNS data',
      url='https://github.com/KHP-Informatics/RADAR-scripts',
      author='Callum Stewart',
      author_email='callum.stewart€kcl.ac.uk',
      packages=['radar'],
      install_requires=[
          'numpy',
          'scipy',
          'pandas',
          'matplotlib',
          'bokeh',
          'tables',
      ],
      include_package_data=True
)
