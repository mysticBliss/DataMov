"""
This is the setup script for the DataMov package.
It uses setuptools to define the package metadata and dependencies.
"""
from setuptools import setup, find_packages

# with open('README.md', 'r', encoding='utf-8') as fh:
#     long_description = fh.read()

setup(
    name='DataMov',
    version='1.2.0',
    author='Saqib Mujtaba',
    author_email='saqib.mj@gmail.com',
    channel="CustomDevelop",
    license="MIT",
    description='Running Data movement operations with Spark in python 3 version (support Cloudera Data Platform (CDP))',
    long_description_content_type='text/markdown',
    url='https://github.com/mysticBliss/sparkDataMov',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'pyspark>=3.0.0',
        'great_expectations',
    ],
)
