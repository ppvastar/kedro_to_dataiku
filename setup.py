#!/usr/bin/env python

from setuptools import setup
from os import path

from kedro_to_dataiku.version import __version__

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()


setup(name='kedro_to_dataiku',
      version=__version__,
      download_url="https://github.com/ppvastar/kedro_to_dataiku/raw/main/dist/kedro_to_dataiku-"+__version__+".tar.gz",
      description='Deploy Kedro project to Dataiku',
      author='Peng Zhang',
      author_email='p.zhang@zoho.com',
      install_requires=['importlib','pandas','PyYAML','gitpython'],
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
