###############################################################################
# remote.py - helper script to safely run remote enbackup commands
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial functionality
###############################################################################
#
# Overview: this script is designed to intercept and validate commands being
# run over SSH, making sure that they're limited to a known subset required
# by the enbackup suite of tools.  A dedicated user account can then be set
# up which allows only this script to be run over SSH, thereby limiting the
# set of functionality available when logging in remotely.
#

import os
import re
import sys
import enbackup.cmd.mirror
import enbackup.cmd.backup
import enbackup.log

#
# Regular expressions for the expected commands that we support.  Note
# that where these expect path formats, they're quite conservative in
# the set of characters that they accept.
#
remote_rsync_regexp = "rsync --server --sender "                              \
                                     "-[A-Za-z.]+ \. ([A-Za-z0-9/\-_:]+)$"
remote_rdiff_backup_regexp = "enbackup backup --remote "                      \
                                     "--tgt-dir ([A-Za-z0-9/\-_:]+) "         \
                                     "--src-server ([A-Za-z0-9/\-_:]+)"
remote_notify_regexp = "enbackup notify (.*)"

#
# Logger for commands we're going to invoke.  This script is essentially
# just 'glue' between other modules so we don't want to spam people with
# extra mails for every hop between servers; instead just use the basic
# logging facility which will record everything in the standard debug file.
#
logger = enbackup.log.Logger("remote")


############################################################################
# Remote command workers                                                   #
############################################################################


#
# Handle an invokation of rsync in 'server' mode:
#
def do_rsync_server(rsync_cmd):
    #
    # This is an rsync operation being run as the remote end of
    # the mirror operation.  Simply extract the path from the
    # command and delegate the remainder of the verification/
    # processing to the remote mirroring handler:
    #
    m = re.match(remote_rsync_regexp, rsync_cmd)

    logger.debug("> Running mirror (remote) for {0}".format(m.group(1)))
    enbackup.cmd.mirror.mirror_remote(m.group(1))


#
# Handle an invokation of rdiff-backup in 'server' mode
#
def do_rdiff_backup(rdiff_cmd):
    #
    # Extract the target directory, source process name, and source server,
    # then invoke the enbackup script in remote mode.
    #
    m = re.match(remote_rdiff_backup_regexp, rdiff_cmd)
    arguments = "--remote --tgt-dir {0} --src-server {1}".format(m.group(1),
                                                                 m.group(2))
    logger.debug("> Running backup (remote) to {0} for {1}".format(m.group(1),
                                                                   m.group(2)))
    enbackup.cmd.backup.main(arguments.split())


def do_notify(notify_cmd):
    #
    # This is a 'notify' command.  All we need to do here is invoke
    # the notify script with the supplied arguments; the script
    # will handle checking of subsequent arguments.
    #
    # Split up the incoming command string based on spaces; we don't
    # support escaping/quoting etc.
    #
    m = re.match(remote_notify_regexp, notify_cmd)

    logger.log("About to run 'notify {0}'".format(m.group(1)))
    enbackup.cmd.notify.main(m.group(1).split())


############################################################################
# Main script body                                                         #
############################################################################


def main(arguments):
    """
    The main body of 'enbackup remote'

    Note: all arguments are ignored; this command only expects to be
    run over SSH, and inspects the SSH_ORIGINAL_COMMAND environment
    variable to determine what actions to take.
    """
    try:
        original_cmd = os.environ['SSH_ORIGINAL_COMMAND']
    except KeyError:
        logger.error("SSH_ORIGINAL_COMMAND not found.  Please invoke "
                     "this command over SSH")
    else:
        logger.debug("Original command: {0}".format(original_cmd))

        #
        # Check that this is one of the expected commands, and invoke the
        # appropriate handling.
        #
        if re.match(remote_rsync_regexp, original_cmd):
            do_rsync_server(original_cmd)
        elif re.match(remote_rdiff_backup_regexp, original_cmd):
            do_rdiff_backup(original_cmd)
        elif re.match(remote_notify_regexp, original_cmd):
            do_notify(original_cmd)
        else:
            logger.error("enbackup remote: Command is not recognized: {0}".
                         format(original_cmd))


if __name__ == "__main__":
    main(sys.argv[1:])
