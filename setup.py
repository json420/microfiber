#!/usr/bin/env python3
# -*- coding: utf-8; tab-width: 4; mode: python -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: t -*-
# vi: set ft=python sts=4 ts=4 sw=4 noet 
#
# microfiber: fabric for a lightweight Couch
# Copyright (C) 2011-2012 Novacut Inc
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
if sys.version_info < (3, 3):
    sys.exit('Microfiber requires Python 3.3 or newer')

from distutils.core import setup
from distutils.cmd import Command
from unittest import TestLoader, TextTestRunner
from doctest import DocTestSuite
import os

import microfiber


class Test(Command):
    description = 'run unit tests and doc tests'

    user_options = [
        ('no-live', None, 'skip live tests against tmp CouchDB instances'),
        ('skip-slow', None, 'skip only the slow 30 second live timeout test'),
    ]

    def initialize_options(self):
        self.no_live = 0
        self.skip_slow = 0

    def finalize_options(self):
        pass

    def run(self):
        # Possibly set environ variables for live test:
        if self.no_live:
            os.environ['MICROFIBER_TEST_NO_LIVE'] = 'true'
        if self.skip_slow:
            os.environ['MICROFIBER_TEST_SKIP_SLOW'] = 'true'

        pynames = ['microfiber', 'microfiber.tests']

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
