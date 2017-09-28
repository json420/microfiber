#!/usr/bin/env python3
#
# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011-2016 Novacut Inc
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

import sys
if sys.version_info < (3, 4):
    sys.exit('Microfiber requires Python 3.4 or newer')

from distutils.core import setup
from distutils.cmd import Command
import os
from os import path
import subprocess

import microfiber
from microfiber.tests.run import run_tests


def run_sphinx_doctest():
    sphinx_build = '/usr/share/sphinx/scripts/python3/sphinx-build'
    if not os.access(sphinx_build, os.R_OK | os.X_OK):
        print('warning, cannot read and execute: {!r}'.format(sphinx_build))
        return
    tree = path.dirname(path.abspath(__file__))
    doc = path.join(tree, 'doc')
    doctest = path.join(tree, 'doc', '_build', 'doctest')
    cmd = [sys.executable, sphinx_build, '-EW', '-b', 'doctest', doc, doctest]
    subprocess.check_call(cmd)


class Test(Command):
    description = 'run unit tests and doc tests'

    user_options = [
        ('skip-all', None, 'skip all tests'),
        ('no-live', None, 'skip live tests against tmp CouchDB instances'),
        ('skip-slow', None, 'skip only the slow 30 second live timeout test'),
    ]

    def initialize_options(self):
        self.skip_all = 0
        self.no_live = 0
        self.skip_slow = 0

    def finalize_options(self):
        pass

    def run(self):
        if self.skip_all:
            sys.exit(0)
        if self.no_live:
            os.environ['MICROFIBER_TEST_NO_LIVE'] = 'true'
        if self.skip_slow:
            os.environ['MICROFIBER_TEST_SKIP_SLOW'] = 'true'
        if not run_tests():
            raise SystemExit('2')
        # FIXME: The doctests are makeing the build hang on the build servers,
        # probably because there is a lingering CouchDB process; disable for
        # now so we can release 14.02:
        #run_sphinx_doctest()


setup(
    name='microfiber',
    description='fabric for a lightweight Couch',
    url='https://launchpad.net/microfiber',
    version=microfiber.__version__,
    author='Jason Gerard DeRose',
    author_email='jderose@novacut.com',
    license='LGPLv3+',
    packages=['microfiber', 'microfiber.tests'],
    cmdclass={'test': Test},
)
