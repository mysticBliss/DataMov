"""
This is the setup script for the DataMov package.
It uses setuptools to define the package metadata and dependencies.
"""
from setuptools import setup, find_packages

# with open('README.md', 'r', encoding='utf-8') as fh:
#     long_description = fh.read()

setup(
    name='DataMov',
    version='1.1.2',
    author='Saqib Mujtaba',
    author_email='saqib.mj@gmail.com',
    channel="CustomDevelop",
    license="MIT",
    description='Running Data movement operations with Spark in python 2.4.3 version (support Cloudera Data Platform (CDP) [Tested for CDP 7.1.8])',
    long_description_content_type='text/markdown',
    url='https://github.com/mysticBliss/sparkDataMov',
    packages=find_packages(),
    bdist_wheel='py27', 
    classifiers=[
        'Programming Language :: Python :: 2.7',  # Python 2.7 compatibility
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=2.7',  # Requires Python 2.7
    install_requires=[
        # 'pyspark==2.4.3',  # PySpark 2.4.x compatible with Python 2.7 [assuming CDP already has already this since pyspark should be working afater instaliing CDP]
    ],
)
