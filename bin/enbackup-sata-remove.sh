#!/bin/bash

#
# enbackup-sata-remove.sh
#
# Safely remove a given SATA device from the system. Must be run as root.
#

if [[ $EUID -ne 0 ]]; then
   echo "ERROR: This script must be run as root" 1>&2
   exit 1
fi

#
# Modified version of script from URL below, to safely handle removal of a
# given SATA device (without requiring knowledge of which channel the device
# is connected to).
#
# http://blog.shadypixel.com/safely-removing-external-drives-in-linux/
#
# Note we can't use scsiadd as the link suggests, because that replies on
# access to /proc/scsi/scsi, which is deprecated.  Instead we write
# directly to /sys/scsi_device/... as recommended by (among others):
#
# https://access.redhat.com/knowledge/docs/en-US/Red_Hat_Enterprise_Linux
#  /5/html-single/Online_Storage_Reconfiguration_Guide/index.html#
#  clean-device-removal
#
# Usage: enbackup-sata-remove.sh {device name}
#
if [ $# -ne 1 ]; then
    echo "Usage: $0 <device>"
    exit 1
fi

if ! which lsscsi >/dev/null 2>&1; then
    echo "Error: lsscsi not installed";
    exit 1
fi

dev=$1

if [[ ! $dev =~ ^\/dev\/sd[a-z]$ ]]; then
    echo "Device name not in expected format, given $dev, expected /dev/sd[a-z]";
    exit 1
fi


#
# Above here is just sanity checking.
#
# Below here actually does things (so take care!)
#
device=`lsscsi | grep $dev`

if [ -z "$device" ]; then
    echo "Error: could not find device: $dev"
    exit 1
fi

#
# Construct the host:channel:id:lun string to identify the device.  This
# should be 
#
hcil=`echo $device | awk \
    '{print substr($0, 2, 7)}'`

if [[ ! $hcil =~ [0-9]:[0-9]:[0-9]:[0-9] ]]; then
    echo "Host:channel:id:lun not in expected format, found '$hcil'";
    exit 1
fi

#
# Flush I/O, wait a moment for things to settle;
# Remove the device; wait a moment more.
# Finally, check the device has gone and report success/failure
#
blockdev --flushbufs $dev
sleep 1
echo 1 > /sys/class/scsi_device/${hcil}/device/delete
sleep 1

device=`lsscsi | grep $1`

if [ -z "$device" ]; then
    echo "Success, device removed!"
else
    echo "Failure, device still exists:"
    echo "$device"
    exit 1
fi
