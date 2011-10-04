#!/usr/bin/env python3

# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `microfiber`.
#
# `microfiber` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `microfiber` is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `microfiber`.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#

"""
Install `microfiber`.
"""

from distutils.core import setup
from distutils.cmd import Command
from distutils.command.build import build
from unittest import TestLoader, TextTestRunner
from doctest import DocTestSuite
import os
from os import path
import subprocess
from urllib.parse import urlparse

import microfiber


class Test(Command):
    description = 'run unit tests and doc tests'

    user_options = [
        ('no-live', None, 'skip live tests against tmp CouchDB instances'),
        ('oauth', None, 'run live tests using oauth instead of basic auth'),
    ]

    def initialize_options(self):
        self.no_live = 0
        self.oauth = 0

    def finalize_options(self):
        pass

    def run(self):
        # Possibly set environ variables for live test:
        if self.no_live:
            os.environ['MICROFIBER_TEST_NO_LIVE'] = 'true'

        if self.oauth:
            os.environ['MICROFIBER_TEST_OAUTH'] = 'true'

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
            raise SystemExit(2)


class build_with_docs(build):

    def find_sphinx_path(self):
        for prefix in os.environ['PATH'].split(':'):
            file_path = os.path.join(prefix, 'sphinx-build')
            if path.isfile(file_path) and os.access(file_path, os.X_OK):
                return file_path

        return None

    def run(self):
        super().run()
        sphinx = self.find_sphinx_path() 
        if sphinx is None:
            print("WARNING: Documentation not generated. python-sphinx missing")
            return
        tree = path.dirname(path.abspath(__file__))
        src = path.join(tree, 'doc')
        dst = path.join(tree, 'doc', '_build', 'html')
        doctrees = path.join(tree, 'doc', '_build', 'doctrees')
        cmd = [
            sphinx,
            '-W',  # Turn  warnings  into  errors
            '-E',  # Don't  use a saved environment
            '-b', 'html',
            '-d', doctrees,
            src,
            dst
        ]
        subprocess.check_call(cmd)


setup(
    name='microfiber',
    description='fabric for a lightweight Couch',
    url='https://launchpad.net/microfiber',
    version=microfiber.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@novacut.com',
    license='LGPLv3+',
    py_modules=['microfiber'],
    cmdclass={
        'test': Test,
        'build': build_with_docs,
    },
)
