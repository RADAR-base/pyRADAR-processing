#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(name='radarstudy',
      version='0.2.1',
      description='Offline processing and visualisation tools for use with data from RADAR studies',
      url='https://github.com/RADAR-base/pyRADAR-processing',
      author='Callum Stewart',
      author_email='callum.stewart@kcl.ac.uk',
      packages=find_packages(),
      install_requires=[
          'numpy',
          'scipy',
          'pandas',
          'dask[complete]',
          'requests',
          'matplotlib',
          'bokeh',
          'yml'
          ],
      include_package_data=True,
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Science/Research"
          ]
)
