#!/usr/bin/env python
from __future__ import print_function

import os
import sys

v = sys.version_info
if v[:2] < (3, 3):
    error = "ERROR: Jupyter Hub requires Python version 3.3 or above."
    print(error, file=sys.stderr)
    sys.exit(1)



from distutils.core import setup

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
integration_str = "drill" # This could change to hive, drill, elastic etc.

with open(pjoin(here, integration_str + '_core', '_version.py')) as f:
    exec(f.read(), {}, version_ns)

integration_base_ver_min = '0.1.0'

try:
    import integration_core
except:
    print("jupyter_%s requires jupyter integration_base (version %s required). No jupyter_integration_base found, please install from https://github.com/johnomernik/jupyter_integration_base" % (integration_str, integration_base_ver_min))
    sys.exit(1)

if integration_core.__version__ < integration_base_ver_min:
    print("jupyter_%s requires jupyter_integration_base version %s or higher. You are on version %s. Please update" % (integration_str, integration_base_ver_min, integration_core.__version__))
    sys.exit(1)

setup_args = dict(
    name='jupyter_' + integration_str,
    packages=[integration_str + '_core'],
    version=version_ns['__version__'],
    description="""An Interface Jupyter Notebooks.""",
    long_description="A magic function for working with Apache Drill for Python3 based Jupyter Notebooks",
    author="John Omernik",
    author_email="mandolinplayer@gmail.comm",
    url="https://github.com/JohnOmernik/jupyter_" + integration_str,
    license="Apache",
    platforms="Linux, Mac OS X",
    keywords=['Interactive', 'Interpreter', 'Shell', 'Notebook', 'Jupyter', integration_str],
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: Apache',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
)

if 'bdist_wheel' in sys.argv:
    import setuptools

# setuptools requirements
if 'setuptools' in sys.modules:
    setup_args['install_requires'] = install_requires = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            req = line.strip()
            if not req or req.startswith(('-e', '#')):
                continue
            install_requires.append(req)


def main():
    setup(**setup_args)

if __name__ == '__main__':
    main()
