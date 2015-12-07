#!/usr/bin/python
###############################################################################
# enbackup-sata-poll.py - run "scsiadd -p", print the output in way to make it
#                         easier to spot what devices are attached.
#
# February 2010, Anthony Toole
#
# Copyright (c) 2010 by Ensoft Ltd.
# All right reserved
#
# Version 1.0 - hacked from old enbackup-sata-op, which was replaced by
#               -insert/-remove scripts.  This is a cut down version of the
#               original to do poll only (and print output clearly).
###############################################################################


import os
import re
import sys
import optparse
import subprocess


#
# external devices
#
devicename_sata = "ST31000520AS"


#
# Hardcoding of the devices that we expected and some clearer names of what
# they are.
#
devicename_mapping = {devicename_sata: "eSATA backup disk",
                      "CD-ROM LTN-489S": "internal CD-ROM",
                      "ST31000340AS": "iSATA disk",
                      "ST31000340NS": "iSATA disk",
                      "ST31000528AS": "iSATA disk",}


#
# Convert between a "Model" value returned by scsiadd and a prettier string
# defined by devicename_mapping
#
def model_to_devicename(model):
    if model in devicename_mapping:
        device = devicename_mapping[model]
    else:
        device = "Unknown device (%s)" % (model)
    return device


#
# Regex to parse output from scsiadd, in particular to grab the "Model" values.
#
poll_regex = re.compile("Model: *(.*?) *Rev:")


#
# Debug is great
#
debug_enabled = False
def debug(debug_str):
    global debug_enabled
    if debug_enabled:
        print(debug_str)


#
# Run the given command, print an error if return code is non-zero,
# return stdout regardless.  stderr is not redirected, so will just be printed
# to caller.
#
# 'cmd' is a list, for example ["ls", "-lq"]
#
def run_command(cmd):
    cmd_str = " ".join(cmd)
    debug("Running '%s'" % (cmd_str))
    a = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    a.wait()
    if a.returncode != 0:
        print("ERROR: command '%s' returned %u" % (cmd_str, a.returncode))
    return "".join(a.stdout.readlines())


#
# Run command to poll all SCSI devices, and return a list of the models
# attached to this machine
#
def scsiadd_poll():
    output = run_command(["scsiadd", "-p"])
    debug(output)
    res = poll_regex.findall(output)
    return res


#
# Take a list of devices, print output based on this
#
def output_device_list(poll_devices, output_prefix=""):
    index = 0
    for device in poll_devices:
        index += 1
        print("%s  %u: %s" %
              (output_prefix, index, model_to_devicename(device)))


if __name__ == "__main__":
    #
    # Parse options, we don't have many  :)
    #
    p = optparse.OptionParser()
    p.add_option("-d", "--debug-enable", dest="debug",
                 action="store_true", default=False, help="Enable debugging")
    (options, args) = p.parse_args()

    debug_enabled = options.debug
    if len(args) != 0:
        print("Invalid arguments specified (expect only optional mode): %s" %
              (args))
        sys.exit(-1)

    #
    # 1) Work out currently inserted devices
    # 2) Do something if needed
    # 3) Work out the "after" state, which will be the same as #1 in poll case
    #
    devices = scsiadd_poll()

    print("%u devices currently attached:" % (len(devices)))
    output_device_list(devices)

    if devicename_sata in devices:
        print "NOTE: eSATA device is attached"
    else:
        print "NOTE: No known eSATA devices attached"

