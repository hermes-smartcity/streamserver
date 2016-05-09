# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2012 Jesus Arias Fisteus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
#
import os
import sys
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

if sys.version_info[0] != 2 or sys.version_info[1] not in [7]:
    print 'hermes-semserver needs Python 2.7'
    sys.exit(1)

# Dependencies
requirements = ['setuptools',
                'ztreamy>=0.4.2.dev7',
                ]


setup(
    name = "hermes-semserver",
    version = "0.34",
    author = "Jesus Arias Fisteus",
    description = ("A framework for publishing semantic events for HERMES"),
    keywords = "rdf sensors web semantic-sensor-web",
    url = "http://hermes.gast.it.uc3m.es/",
    packages=['semserver'],
    long_description=read('README'),
    install_requires = requirements,
)
