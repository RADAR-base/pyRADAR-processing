#!/usr/bin/env python3
from setuptools import setup, find_packages
import glob

schema_files = glob.glob('radar/Schemas/commons', recursive=True)
spec_files = glob.glob('radar/Schemas/specifications/**', recursive=True)
extra_files = schema_files + spec_files + ['radar/config.yml']

setup(name='radarstudy',
      version='0.2',
      description='Offline processing and visualisation tools for use with data from RADAR studies',
      url='https://github.com/RADAR-base/pyRADAR-processing',
      author='Callum Stewart',
      author_email='callum.stewart@kcl.ac.uk',
      setup_requires=[ "setuptools_git >= 0.3", ],
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
      package_data={'': extra_files}
)
