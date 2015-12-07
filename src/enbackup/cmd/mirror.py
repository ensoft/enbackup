###############################################################################
# mirror.py - mirror files between servers, using rsync
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial version
###############################################################################
#
# Overview: this script is can be used to mirror files between servers, for
# taking a copy of an entire backup repository.  It only supports 'pulling'
# files from an existing backup repository to a mirror.
#
# It has two modes of operation:
#
# Local  - invoked on the server to mirror *to*
# Remote - invoked on the server to mirror *from*
#

import os
import re
import sys
import ConfigParser
import optparse
import datetime
import enbackup.log
from enbackup.utils import DirLock, StringFilter
from enbackup.utils import run_cmd, run_cmd_output, get_username

#
# Mirror commands.  Note: if the local command changes, the remote
# command likely needs to be updated too (because 'enbackup remote'
# intercepts the original command and we run the remote version
# directly).
#
# The remote command string can be worked out by running an rsync
# between servers and checking which command rsync tries to run
# at the remote end.
#
mirror_cmd_remote = "rsync --server --sender -lHogDtpAXrxe.iLsf . "
mirror_cmd_local = "rsync -rlptgoDHAXh --delete --stats --links --one-file-system "

#
# Usage information:
# - usage_subcmd: the subcommand of 'enbackup' that this script implements
# - usage:        the full command line
#
usage_subcmd = "mirror --source <source-keyword> --destination <dst-dir>"
usage = "Usage:\n" \
        "enbackup " + usage_subcmd

#
# Configurtion file details.  Note the path is fixed, because when
# the script is invoked indirectly by sshd there's no way for the user
# to specify which configuration file to use.
#
cfg_file_path = "/etc/enbackup.d/enbackup-mirror.rc"
src_dir_restrict_default = "/enbackup/"
tgt_dir_restrict_default = "/enbackup/"
email_default = "root@localhost"


class MirrorCmdError(Exception):
    """Exception raised for errors in a mirroring operation.

    Attributes:
        cmd -- the command the was attempted
        msg -- explanation of the error
    """
    def __init__(self, cmd, msg):
        self.cmd = cmd
        self.msg = msg
        super(MirrorCmdError, self).__init__(cmd, msg)


###########################
# START: Worker functions #
###########################


def validate_local_dir(dir_path, restrict_path):
    """
    Check that the path is valid on this server, i.e:
     - it is a contained within the 'restrict_path'
     - it doesn't contain any instances of '..'
     - is a valid filesystem path
    """
    if not dir_path.startswith(restrict_path):
        return (False,
                "Path '{0}' is outside allowed directory '{1}'".
                format(dir_path, restrict_path))

    if re.match(".*\.\..*", dir_path):
        return (False, "Path must not contain ..")

    if not os.path.exists(dir_path):
        return (False,
                "Path '{0}' does not exist".format(dir_path))

    #
    # If those checks pass, report that the path is valid
    # (there's no error message)
    #
    return (True, None)


def start_logger():
    """
    Start a logger to record details of a mirroring operation.
    This creates a logfile, with the timestamp in the name, and
    returns the associated logger.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    logname = timestamp + ".mirror"

    return (enbackup.log.Logger('mirror', logfile=logname))


##################################
# START: Mirroring functionality #
##################################

#
# Start up remote mirroring of the requested source directory.
#
# This function will validate that the supplied source directory is
# valid, and spawn an rsync server to start the mirroring.
#
def mirror_remote(src_dir):
    #
    # This isn't invoked as a top-level operation so don't create a full
    # logfile; just use standard debugging-level support.
    #
    logger = enbackup.log.Logger('mirror')

    config = ConfigParser.ConfigParser({ "src_dir_restrict":
                                         src_dir_restrict_default })
    config.read(cfg_file_path)
    src_dir_restrict = config.get("Paths", "src_dir_restrict")

    (path_is_valid, msg) = validate_local_dir(src_dir, src_dir_restrict)

    if not path_is_valid:
        logger.log(msg)
        raise MirrorCmdError(src_dir, msg)

    #
    # If all is well, start the rsync server to mirror the requested
    # directory.  If currently running as root, make sure we drop down to
    # the enbackup user account for actually running the rsync.  We don't
    # really expect this to happen on the remote end, but best to be
    # sure!
    #
    rsync_cmd_str = mirror_cmd_remote + "{0}".format(src_dir)

    if os.geteuid() == 0:
        rsync_cmd = ["su", get_username(), "--command", rsync_cmd_str]
    else:
        rsync_cmd = rsync_cmd_str.split()

    logger.debug("Remote mirror command: {0}".format(rsync_cmd))

    logger.debug("About to lock '{0}' for remote mirror".
                format(src_dir))
    with DirLock("mirror-remote (PID {0})".format(os.getpid()),
                 src_dir):
        run_cmd(rsync_cmd, logger)
    logger.debug("Remote mirror done")

#
# Start mirroring the specified source directory to the specified target
# directory.
#
def mirror_local(src, tgt_dir):
    #
    # Start recording details of this operation to a logfile:
    #
    logger = start_logger()

    config = ConfigParser.ConfigParser({ "tgt_dir_restrict":
                                         tgt_dir_restrict_default,
                                         "src_server": "",
                                         "log_email":
                                         email_default,
                                         "log_filter": ""})
    config.read(cfg_file_path)
    tgt_dir_restrict = config.get("Paths", "tgt_dir_restrict")
    email_to = config.get("Logging", "log_email")
    log_filter_file = config.get("Logging", "log_filter")

    #
    # Check the target directory is valid on this server.
    #
    (path_is_valid, msg) = validate_local_dir(tgt_dir, tgt_dir_restrict)

    if not path_is_valid:
        logger.log(msg)
        raise MirrorCmdError(tgt_dir, msg)

    #
    # Get the source directory.  This is selected from the config file, based on
    # the incoming argument, so we trust it's a sane value:
    #
    try:
        src_server = config.get(src, "src_server")
        src_dir    = config.get(src, "src_dir")
    except ConfigParser.NoSectionError:
        logger.log("No source entry found in %s matching keyword '%s'" %
                   (cfg_file_path, src))
        raise

    #
    # Work out which directory we're going to be writing to so we can
    # lock it.  The target directory depends on the format of the
    # source directory passed to rsync:
    # - If the source ends in a '/' then the contents are mirrored;
    #   the target is just the target dir as passed by the caller.
    # - If the source does not end in a '/' then everything will
    #   be copied into a subdirectory -- we should append the source
    #   directory name to the passed target directory to get the
    #   path to lock.
    #
    # This behavior falls out nicely if we just use the 'basename'
    # function to extract the last element of the source directory:
    #
    lock_dir = os.path.join(tgt_dir, os.path.basename(src_dir))
    
    #
    # If all is well, spawn rsync to mirror the data across.
    # Any failures will result in an exception which will
    # get propagated up to the caller.  If currently running as root,
    # make sure we drop down to the enbackup user account for actually
    # running the rsync (we don't want to open any remote connections
    # as root).
    #
    if src_server != "":
        src = "{0}:{1}".format(src_server, src_dir)
    else:
        src = src_dir

    rsync_cmd_str = mirror_cmd_local + "{0} {1}".format(src, tgt_dir)

    if os.geteuid() == 0:
        rsync_cmd = ["su", get_username(), "--command", rsync_cmd_str]
    else:
        rsync_cmd = rsync_cmd_str.split()

    logger.debug("Local mirror command: {0}".format(rsync_cmd))

    logger.debug("About to lock '{0}' for local mirror".format(lock_dir))
    start_time = datetime.datetime.now()
    with DirLock("mirror-local (PID: {0})".format(os.getpid()),
                 lock_dir):
        (rc, output, err) = run_cmd_output(rsync_cmd, logger)
    logger.debug("Local mirror done")
    duration = datetime.datetime.now() - start_time

    #
    # Filter the results of the rsync to remove noise, then record them
    # in a log file, and send an email with the details.
    #
    if log_filter_file != "":
        log_filter = StringFilter(log_filter_file)
        filtered_output = log_filter.filter(output.split("\n"), True)
        filtered_output = "\n".join(filtered_output)
    else:
        filtered_output = output

    if rc == 0:
        subject = "Mirror {0} completed".format(src)
        result_msg =                                                          \
            "Mirroring has completed successfully!\n"                         \
            "    from: {0}\n"                                                 \
            "    to:   {1}\n"                                                 \
            "Total elapsed time: {2}\n\n"                                     \
            "############ rsync summary log ############\n\n"                 \
            "{3}".format(src, tgt_dir, duration, filtered_output)
    else:
        subject = "Mirror {0} FAILED".format(src)
        result_msg =                                                          \
            "Mirroring of {0} to {1} FAILED (code {2}), please investigate."  \
            "\n\n{3}\n\n{4}".format(src, tgt_dir, rc, filtered_output, err)

    logger.log(result_msg)
    logger.send_email(subject, result_msg, email_to)


def main(arguments):
    """
    Main body of the command.

    arguments - the argument list to parse; typically just sys.argv[1:]
    to parse options passed on the command line.
    """
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-s", "--source", action="store", dest="source")
    parser.add_option("-d", "--destination", action="store", dest="dst_dir")

    (options, args) = parser.parse_args(args=arguments)

    #
    # Both source and destination are required:
    #
    if options.source is None or options.dst_dir is None:
        sys.stderr.write(usage)
        sys.exit(1)
    
    #
    # Start a local mirror with the supplied arguments; this function
    # will perform sanity checking to make sure they're reasonable.
    #
    mirror_local(options.source, options.dst_dir)
    

if __name__ == "__main__":
    #
    # Invoke the main command with options passed by the user.
    #
    main(sys.argv[1:])
