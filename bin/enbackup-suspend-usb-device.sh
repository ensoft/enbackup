#!/bin/bash
#
#  Based on suspend-usb-device, updated to remove usage of the now-
#  deprecated power/level file in favour of power/control and
#  power/autosuspend_delay_ms:
#
#     As of kernel version 2.6.38 there are two parameters we need to
#     write to configure autosuspend:
#
#     - power/control:
#         "on" to force the device on (disallow suspend)
#         "auto" to allow autosuspend when idle
#
#     - power/autosuspend_delay_ms:
#         Idle time in ms to wait before suspending.
#
#     The default values are "on" and "2000"; we set these to "auto" and "0" here
#     to allow immediate suspension as soon as the device becomes idle.
#
#     See http://www.mjmwired.net/kernel/Documentation/usb/power-management.txt
#     for more details.
#
#  Original copyright header follows:
#
#  suspend-usb-device: an easy-to-use script to properly put an USB
#  device into suspend mode that can then be unplugged safely
#
#  Copyright (C) 2009, Yan Li <elliot.li.tech@gmail.com>
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  To reach the auther, please write an email to the address as stated
#  above.

#  ACKNOWLEDGEMENTS:
#      Christian Schmitt <chris@ilovelinux.de> for firewire supporting
#      David <d.tonhofer@m-plify.com> for improving parent device
#      search and verbose output message


usage()
{
    cat<<EOF
suspend-usb-device  Copyright (C) 2009  Yan Li <elliot.li.tech@gmail.com>

This script is designed to properly put an USB device into suspend
mode that can then be unplugged safely. It sends a SYNCHRONIZE CACHE
command followed by a START-STOP command (if the device supports it),
unbinds the device from the driver and then suspends the USB
port. After that you can disconnect your USB device safely.

usage:
$0 [options] dev

sample:
$0 /dev/sde

options:
  -l     show the device and USB bus ID only
  -h     print this usage
  -v     verbose

This program comes with ABSOLUTELY NO WARRANTY.  This is free
software, and you are welcome to redistribute it under certain
conditions; for details please read the licese at the beginning of the
source code file.
EOF
}

set -e -u

SHOW_DEVICE_ONLY=0
VERBOSE=0
while getopts "vlh" opt; do
    case "$opt" in
        h)
            usage
            exit 2
            ;;
        l)
            SHOW_DEVICE_ONLY=1
            ;;
        v)
            VERBOSE=1
            ;;
        ?)
            echo
            usage
            exit 2
            ;;
    esac
done
DEV_NAME=${!OPTIND:-}

if [ -z ${DEV_NAME} ]; then
    usage
    exit 2
fi

# mount checking
if mount | grep "^${DEV_NAME}[[:digit:]]* "; then
    1>&2 echo
    1>&2 echo "the above disk or partition is still mounted, can't suspend device"
    1>&2 echo "unmount it first using umount"
    exit 1
fi

# looking for the parent of the device with type "usb-storage:usb", it
# is the grand-parent device of the SCSI host, and it's devpath is
# like
# /devices/pci0000:00/0000:00:1d.7/usb5/5-8 (or /fw5/fw5-8 for firewire devices)

# without an USB hub, the device path looks like:
# /devices/pci0000:00/0000:00:1d.7/usb2/2-1/2-1:1.0/host5/target5:0:0/5:0:0:0
# here the grand-parent of host5 is 2-1

# when there's a USB HUB, the device path is like:
# /devices/pci0000:00/0000:00:1d.0/usb5/5-2/5-2.2/5-2.2:1.0/host4/target4:0:0/4:0:0:0
# and the grand-parent of host4 is 5-2.2

DEVICE=$(udevadm info --query=path --name=${DEV_NAME} --attribute-walk | \
    egrep "looking at parent device" | head -1 | \
    sed -e "s/.*looking at parent device '\(\/devices\/.*\)\/.*\/host.*/\1/g")

if [ -z $DEVICE ]; then
    1>&2 echo "cannot find appropriate parent USB/Firewire device, "
    1>&2 echo "perhaps ${DEV_NAME} is not an USB/Firewire device?"
    exit 1
fi

# the trailing basename of ${DEVICE} is DEV_BUS_ID ("5-8" in the
# sample above)
DEV_BUS_ID=${DEVICE##*/}

[[ $VERBOSE == 1 ]] && echo "Found device $DEVICE associated to $DEV_NAME; USB bus id is $DEV_BUS_ID"

if [ ${SHOW_DEVICE_ONLY} -eq 1 ]; then
    echo Device: ${DEVICE}
    echo Bus ID: ${DEV_BUS_ID}
    exit 0
fi

# flush all buffers
sync

# root check
if [ `id -u` -ne 0 ]; then
    1>&2 echo error, must be run as root, exiting...
    exit 1
fi


# send SCSI sync command, some devices don't support this so we just
# ignore errors with "|| true"
[[ $VERBOSE == 1 ]] && echo "Syncing device $DEV_NAME"
sdparm --command=sync "$DEV_NAME" >/dev/null || true
# send SCSI stop command
[[ $VERBOSE == 1 ]] && echo "Stopping device $DEV_NAME"
sdparm --command=stop "$DEV_NAME" >/dev/null

# unbind it; if this yields "no such device", we are trying to unbind the wrong device
[[ $VERBOSE == 1 ]] && echo "Unbinding device $DEV_BUS_ID"
if [[ "${DEV_BUS_ID}" == fw* ]]
then
    echo -n "${DEV_BUS_ID}" > /sys/bus/firewire/drivers/sbp2/unbind
else
    echo -n "${DEV_BUS_ID}" > /sys/bus/usb/drivers/usb/unbind

    # suspend it if it's an USB device (we have no way to suspend a
    # firewire device yet)
    #
    #     As of kernel version 2.6.38 there are two parameters we need to
    #     write to configure autosuspend:
    #
    #     - power/control:
    #         "on" to force the device on (disallow suspend)
    #         "auto" to allow autosuspend when idle
    #
    #     - power/autosuspend_delay_ms:
    #         Idle time in ms to wait before suspending.
    #
    #     The default values are "on" and "2000"; we set these to "auto" and "0" here
    #     to allow immediate suspension as soon as the device becomes idle.
    #
    #     See http://www.mjmwired.net/kernel/Documentation/usb/power-management.txt
    #     for more details.
    #

    POWER_CONTROL_FILE=/sys${DEVICE}/power/control
    POWER_SUSPEND_FILE=/sys${DEVICE}/power/autosuspend_delay_ms
    if [ ! -f "$POWER_CONTROL_FILE" ]; then
	1>&2 cat<<EOF
Cannot find control file $POWER_CONTROL_FILE
EOF
	exit 4
    fi

    if [ ! -f "$POWER_SUSPEND_FILE" ]; then
	1>&2 cat<<EOF
Cannot find autosuspend file $POWER_SUSPEND_FILE
EOF
	exit 5
    fi

    echo "0" > "$POWER_SUSPEND_FILE"
    echo "auto" > "$POWER_CONTROL_FILE"
fi
