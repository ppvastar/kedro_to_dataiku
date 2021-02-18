#!/usr/bin/env python

from setuptools import setup
from kedro_to_dataiku import __version__

setup(name='kedro_to_dataiku',
      version=__version__,
      description='Deploy Kedro project to Dataiku',
      author='Peng Zhang',
      author_email='p.zhang@zoho.com',
      install_requires=['pyspark','kedro==0.16.5','dataiku-api-client>=8.0.0'],
      license='LICENSE.txt'
    )
