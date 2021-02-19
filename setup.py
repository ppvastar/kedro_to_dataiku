#!/usr/bin/env python

from setuptools import setup

setup(name='kedro_to_dataiku',
      version='0.3.1',
      download_url="https://github.com/ppvastar/kedro_to_dataiku/archive/v03.tar.gz",
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
          ],
      long_description=long_description,
      long_description_content_type="text/markdown",
    )
