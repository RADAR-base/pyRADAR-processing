#!/usr/bin/env python3
from setuptools import setup

setup(name='radarstudy',
      version='0.1',
      description='Offline processing and visualisation tools for use with data from RADAR studies',
      url='https://github.com/RADAR-base/pyRADAR-processing',
      author='Callum Stewart',
      author_email='callum.stewart@kcl.ac.uk',
      packages=['radar'],
      install_requires=[
          'numpy',
          'scipy',
          'pandas',
          'matplotlib',
          'bokeh',
          'dask',
          'requests',
      ],
      include_package_data=True
)
