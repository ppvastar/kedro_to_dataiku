#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='kedro_to_dataiku',
      version='0.0.1',
      description='Deploy Kedro project to Dataiku',
      author='Peng Zhang',
      author_email='p.zhang@zoho.com',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      license='LICENSE.txt',
    )