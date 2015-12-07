###############################################################################
# ui.py - Top-level user interface
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial functionality
###############################################################################
#
# Overview: implement the command-line interface for the enbackup tools.
#
# This script simply checks which command is being requested, and
# invokes the appropriate module as required.
#


import sys
import os
import pwd
import enbackup.utils
import enbackup.log


#
# A list of valid subcommands.  Anything not in this list will
# be rejected.  Note these match the module names; the import
# code below relies on this.
#
valid_subcommands = [ "backup", "mirror", "remote", "notify" ]


#
# Usage string for this script:
#
usage = "Usage:\n" \
        "enbackup {{ {0} }} <subcommand-args>\n".format(
    " | ".join(valid_subcommands))


def main():
    """
    The main body of the ui module.  This does the work required to
    run the requested subcommand.
    """
    #
    # First of all, check that the requested user ID is valid, otherwise
    # we won't get very far!
    #
    # Note that we can't even use our standard logging support in this
    # case, so just have to write to stderr instead.
    #
    user = enbackup.utils.get_username()
    try:
        pwd.getpwnam(user)
    except KeyError:
        sys.stderr.write("Username {0} not recognized\n".format(user))
        sys.exit(1)

    #
    # Rather than relying on OptParse, which could get confused by
    # the extra arguments for each subcomand, just check the first
    # argument:
    # - if it's one of the supported subcommands then go ahead and
    #   run it.
    # - if not recognised, print an error message.  Note that we
    #   haven't initialized any logging at this stage so just
    #   print directly to stderr.
    #
    if len(sys.argv) > 1:
        subcmd = sys.argv[1]

        if subcmd in valid_subcommands:
            module = __import__("enbackup.cmd." + subcmd,
                                globals(),
                                locals(),
                                ["main"])
            module.main(sys.argv[2:])
        else:
            sys.stderr.write("Unrecognized command '{0}'. {1}".
                             format(subcmd, usage))
    else:
        sys.stderr.write(usage)
