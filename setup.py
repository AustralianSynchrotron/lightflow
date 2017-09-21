"""
Lightflow
---------

Lightflow is a lightweight, distributed workflow system.

It is based on a directed acyclic graph structure, with tasks as nodes and arbitrary data
flowing between tasks.

"""

from setuptools import setup, find_packages
import re

with open('lightflow/version.py') as file:
    version = re.search(r"__version__ = '(.*)'", file.read()).group(1)

setup(
    name='Lightflow',
    version=version,
    description='A lightweight, distributed workflow system',
    long_description=__doc__,
    url='https://github.com/AustralianSynchrotron/Lightflow',

    author='The Australian Synchrotron Software Group',
    author_email='python@synchrotron.org.au',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    packages=find_packages(exclude=['tests']),

    install_requires=[
        'celery>=4.0.2',
        'click>=6.7',
        'colorlog>=2.10.0',
        'networkx>=1.11,<2',
        'pymongo>=3.4.0',
        'pytz>=2016.10',
        'redis>=2.10.5',
        'ruamel.yaml>=0.14.2',
        'cloudpickle>=0.2.2'
    ],

    entry_points={
        'console_scripts': [
            'lightflow=lightflow.scripts.cli:cli',
        ],
    },
)
