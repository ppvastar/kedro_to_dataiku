#!/usr/bin/env python

from setuptools import setup

setup(name='kedro_to_dataiku',
      version='0.2',
      description='Deploy Kedro project to Dataiku',
      author='Peng Zhang',
      author_email='p.zhang@zoho.com',
      install_requires=['importlib','pandas','PyYAML'],
      packages=['kedro_to_dataiku'],
      license='MIT',
      url="https://github.com/ppvastar/kedro_to_dataiku",
      keywords = ['data science','pipeline','flow' 'dataiku', 'kedro'],
      classifier=[
          'Development Status :: 3 - Alpha',   
          'License :: OSI Approved :: MIT License', 
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          ]
    )
