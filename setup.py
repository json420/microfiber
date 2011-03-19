#!/usr/bin/env python3

"""
Install `microfiber`.
"""

import sys
from distutils.core import setup
from distutils.cmd import Command
from unittest import TestLoader, TextTestRunner
from doctest import DocTestSuite
import microfiber


class Test(Command):
    description = 'run unit tests and doc tests'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        pynames = ['microfiber', 'test_microfiber']

        # Add unit-tests:
        loader = TestLoader()
        suite = loader.loadTestsFromNames(pynames)

        # Add doc-tests:
        for name in pynames:
            suite.addTest(DocTestSuite(name))

        # Run the tests:
        runner = TextTestRunner(verbosity=2)
        result = runner.run(suite)
        if not result.wasSuccessful():
            sys.exit(1)


setup(
    name='microfiber',
    description='fabric for a lightweight Couch',
    url='https://launchpad.net/microfiber',
    version=microfiber.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@novacut.com',
    license='PSF',
    py_modules=['microfiber'],
    cmdclass={'test': Test},
)
