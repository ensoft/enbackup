#!/bin/sh

set -e

# Build .deb
debuild -uc -us -tc -I

# Build .rpm
python setup.py bdist_rpm
