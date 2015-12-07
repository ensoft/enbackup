#!/bin/bash

#
# For the given user, parse the logs showing date + mirror/source size - in
# reverse.  Helpful for finding out when someone's backups jumped in size
#

if [[ $# -ne 1 ]]; then
    echo "Invalid arguments - expect user name";
    exit -1;
fi

user=$1
start_match="Starting backup at";

find /var/log/enbackup/ -type f -name "[0-9]*-*log" | sort -n -r |  xargs grep -h -A1000 "$start_match"  | egrep -A3 "^\/home\/$user|$start_match"  | egrep "$start_match|Directory" | less
