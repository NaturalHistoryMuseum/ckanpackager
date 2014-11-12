from setuptools import setup, find_packages

setup(
  name='ckanpackager',
  version='0.2',
  description='Service to package CKAN data into ZIP files and email the link to the file to users',
  url='http://github.com/NaturalHistoryMuseum/ckanpackager',
  packages=find_packages(exclude='tests'),
  entry_points={
      'console_scripts': [
          'ckanpackager = ckanpackager.cli:run'
      ]
  }
)
