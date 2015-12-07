#!/bin/bash

#
# enbackup-sata-insert.sh
#
# Find all new SATA devices on the system and add each one. Must be run
# as root.
#

if [[ $EUID -ne 0 ]]; then
   echo "ERROR: This script must be run as root" 1>&2
   exit 1
fi

#
# Find all the sata hosts on this system, and for each do a scan for new
# devices
#
host_ids=`lsscsi --hosts | grep sata_mv | cut -f 2 -d "[" | cut -f 1 -d "]"`

for host_id in $host_ids; do
    scsiadd -s $host_id;
done

