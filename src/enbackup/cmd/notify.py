###############################################################################
# notify.py - Run enbackup commands asynchronously
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial version
###############################################################################
#
# This script allows other enbackup commands to be invoked asynchronously,
# allowing them to be spawned by some trigger (e.g. udev, ssh, etc) without
# causing long-running commands to block the trigger until they complete. 
#

import re
import os
import sys
import time
import enbackup.cmd.mirror
import enbackup.log
from enbackup.utils import run_cmd,     get_username


#
# Logger for this module
#
logger = enbackup.log.Logger("notify")


#
# Usage description, when run as a standalone script
#
usage = "'enbackup notify [--sever <server>] CMD' where CMD is one of " \
        "the following:\n" \
        "  " + enbackup.cmd.mirror.usage_subcmd


#
# Test helper function
#
def notify_test(args):
    """
    Test function for checking that async notifications work. 
    """
    logger.debug("Start test, about to sleep...")
    time.sleep(float(args[0]))
    logger.debug("End test")
    print("Finished test!")


#
# notify_cmds -- a mapping from keywords to commands that we can run.
#
notify_cmds = { "mirror": enbackup.cmd.mirror.main,
                "test": notify_test }


def run_async(subcmd, subcmd_args):
    """
    Run a command asynchronously.

    Allows long-running commands to be triggered over ssh, by udev, etc,
    without blocking the invoking process/daemon.

    - subcmd:      the function to invoke
    - subcmd_args: arguments to pass to the function
    """
    #
    # Run the command asynchronously.  We do this by forking ourselves and
    # executing the command from the child process, and redirecting the
    # child's stdin, stdout, and stderr to /dev/null
    #
    retval = os.fork()

    if retval == 0:
        #
        # This is the child process.
        #
        # Overwrite the inherited FDs (so that, if running over SSH then
        # the SSH doesn't keep waiting while the subcommand runs) and then
        # just run the requested subcommand.  Note the use of os._exit()
        # rather than sys.exit() since this is a forked child.
        #
        devnull_in = open("/dev/null", "r")
        devnull_out = open("/dev/null", "a+")
        os.dup2(devnull_in.fileno(), sys.stdin.fileno())
        os.dup2(devnull_out.fileno(), sys.stdout.fileno())
        os.dup2(devnull_out.fileno(), sys.stderr.fileno())

        notify_cmds[subcmd](subcmd_args)
        os._exit(os.EX_OK)
    else:
        #
        # Log a debug message just to record that the subcommand has
        # been spawned.
        #
        logger.log("Spawned subprocess {0} to execute '{1}' with args '{2}'".
                   format(retval, subcmd, subcmd_args))


def notify_local(subcommand):
    #
    # Run the command if known; otherwise log an error.
    #
    if subcommand[0] in notify_cmds:
        run_async(subcommand[0], subcommand[1:])
    else:
        logger.error("Subcommand '{0}' is not supported.  Usage: \n" \
                     "{1}".format(" ".join(subcommand),
                                  usage))


def notify_remote(server, remote_subcommand):
    ssh_user = get_username()

    #
    # Verify that the 'server' argument is a simple name -- no spaces,
    # escaping, quotes, etc. permitted -- before running the command.
    # If running as root, switch to the enbackup user before running
    # the SSH so that the right key gets picked up.  In future we
    # could make this configurable...
    #
    if re.match("([A-Za-z0-9/\-_:]+)", server):
        ssh_cmd_str = "ssh {0}@{1} enbackup notify {2}".format(
                        ssh_user, server, " ".join(remote_subcommand))

        if os.geteuid() == 0:
            ssh_cmd = ["su", ssh_user, "--command", ssh_cmd_str]
        else:
            ssh_cmd = ssh_cmd_str.split()

        run_cmd(ssh_cmd, logger)
    else:
        logger.error("Server '{0}' name is not in the correct format".format(
                     server))


def main(subcommand):
    """
    Main body of 'enbackup notify'

    subcommand: A list containing the subcommand to execute, followed
    by any arguments to that subcommand
    """
    #
    # Read incoming arguments.  We don't currently support any options
    # of our own, but rather expect that:
    # - the first positional argument indicates which subcommand we're
    #   supposed to spawn.
    # - the remaining arguments are passed to the subcommand, which will
    #   sanity-check them and take the appropriate action.
    #
    # Note that we deliberately don't support invoking arbitrary commands,
    # because of the potential security risk.
    #
    if len(subcommand) > 0:
        #
        # First work out whether the command is local or remote.
        # Python's optparse module would get confused by the fact
        # the argument list could contain arguments from the subcommand,
        # so for simplicity just do an explicit check for a 'server'
        # argument here.
        #
        if subcommand[0] == "--server":
            notify_remote(subcommand[1], subcommand[2:])
        else:
            notify_local(subcommand)
    else:
        logger.error("Subcommand missing.  Usage: \n" \
                     "{0}".format(usage))
                

if __name__ == "__main__":
    main(sys.argv[1:])
