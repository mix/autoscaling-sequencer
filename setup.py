#!/usr/bin/env python

import sys

from setuptools import setup, find_packages
from setuptools.command.test import test


class PyTest(test):
    def __init__(self):
        self.test_args = []
        self.test_suite = True

    def finalize_options(self):
        test.finalize_options(self)

    def run_tests(self):
        import pytest

        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


with open('README.md') as f:
    readme = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='autoscaling-sequencer',
    version='0.1.0',
    description='Utility to concurrently create unique id for instances in an aws auto-scaling group',
    long_description=readme,
    author='Ankit Chaudhary',
    author_email='ankit@mix.com',
    url='https://github.com/mix/autoscaling-sequencer',
    install_requires=requirements,
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
    packages=find_packages(),
    package_data={'': ['*.json']},
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python :: 2.7'
        'Development Status :: Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
    ],
    use_2to3=True,
    entry_points='''
        [console_scripts]
        sequencer=sequencer.cli:generate
    ''',
)
