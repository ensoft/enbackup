#!/bin/sh

# Recent versions of udev used cgroups to track child
# processeses spawned by udev rules, and kill any that
# run for more than a couple of minutes.
#
# To work around this, use 'at' to launch the enbackup-archive.py
# script on the requested device.
echo /usr/bin/enbackup-archive.py --device $DEVPATH | at now
