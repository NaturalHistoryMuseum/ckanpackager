from setuptools import setup, find_packages

with open('ckanpackager/version.py') as f:
    exec (f.read())

setup(
    name='ckanpackager',
    version=__version__,
    description='Service to package CKAN data into ZIP files and email the link to the file to users',
    url='http://github.com/NaturalHistoryMuseum/ckanpackager',
    packages=find_packages(exclude='tests'),
    entry_points={
        'console_scripts': [
            'ckanpackager = ckanpackager.cli:run',
            'ckanpackager-service = ckanpackager.service:run',
            'ckanpackager-caretaker = ckanpackager.caretaker:run'
        ]
    }
)
