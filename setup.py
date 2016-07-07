from setuptools import setup, find_packages
import re

with open('lightflow/__init__.py') as file:
    version = re.search(r"__version__ = '(.*)'", file.read()).group(1)

setup(
    name='lightflow',
    version=version,
    description='A lightweight, high performance pipeline system for synchrotrons',
    url='https://stash.synchrotron.org.au/projects/DR/repos/lightflow/browse',

    author='The Australian Synchrotron Python Group',
    author_email='python@synchrotron.org.au',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.5',
    ],

    packages=find_packages(exclude=['tests', 'examples']),

    install_requires=[
        'celery>=3.1.23',
        'click>=6.6',
        'colorlog>=2.7.0',
        'networkx>=1.11',
        'pymongo>=3.2.2',
        'pytz>=2016.4',
        'redis>=2.10.5',
        'ruamel.yaml>=0.11.11',
        'pyzmq>=15.2.0'
    ],

    entry_points={
        'console_scripts': [
            'lightflow=lightflow.scripts.cli:cli',
        ],
    },
)
