###############################################################################
# log.py - enbackup logging support
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial version
###############################################################################
#
# This module provides a Logger class which provides several levels of logging,
# in increasing order of importance:
# - Debug
# - Log
# - Error
#
# A user of the module creates a logger, optionally specifying a log file
# and a debug file.  If no debug file is specified then a default is
# used; if no log file is specified then messages are only printed to the
# debug file.
#
# The behavior is then:
# - Debug: prints messages to the debug file only.
# - Log:   prints messages to the debug file and log file (if specified)
# - Error: prints messages to the debug file, log file, and stderr.
#

import os
import sys
import logging
import tempfile
from enbackup.utils import run_cmd_output, FileLock, chown_enbackup

default_format =                                                              \
        "%(asctime)s %(levelname)-6s %(process)6d [%(name)-8s]: %(message)s"
default_log_dir = "/var/log/enbackup/"
default_debugfile = "enbackup.debug"

logging_lockfile = "enbackup.log.lock"


class EnbackupFileHandler(logging.FileHandler):
    def _open(self):
        #
        # Call the standard handler, and then change the file ownership
        # to enbackup:
        #
        retval = logging.FileHandler._open(self)
        chown_enbackup(self.baseFilename)
        return retval


class Logger(object):
    logfile = None
    debugfile = None
    
    def __init__(self, name, logfile=None, debugfile=None):
        #
        # We support logging messages to up to two different files;
        # one for log messages, and one for debug.  We add filters
        # to the handlers to ensure only messages of the desired
        # verbosity get sent to each target.
        #
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        self.formatter = logging.Formatter(default_format)

        if not os.path.exists(default_log_dir):
            os.makedirs(default_log_dir)

        chown_enbackup(default_log_dir)

        #
        # Log messages go to the specified log file, if any
        #
        if logfile != None:
            self.logfile = default_log_dir + logfile
            self.handler = EnbackupFileHandler(self.logfile)
            self.handler.setFormatter(self.formatter)
            self.handler.setLevel(logging.INFO)
            self.logger.addHandler(self.handler)

        #
        # Debug and Log messages go to the specified debugfile,
        # or the default enbackup debugfile if none was provided.
        #
        if debugfile == None:
            debugfile = default_debugfile
            
        self.debugfile = default_log_dir + debugfile
        self.debughandler = EnbackupFileHandler(self.debugfile)
        self.debughandler.setFormatter(self.formatter)
        self.debughandler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.debughandler)

        #
        # Add a handler for printing error messages to stderr.
        # Note these will also get recorded in the debug and log
        # files.
        #
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.ERROR)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(handler)

    #
    # Grab a lock while actually emitting a log message, since the
    # logging infra is not multi-process-safe.
    #
    def log(self, msg):
        """
        Log a message.

        The message will get recorded in the log file and the debug file
        """
        with FileLock("Logging from PID {0}".format(os.getpid()),
                      logging_lockfile):
            self.logger.info(msg)

    def debug(self, msg):
        """
        Print a debug message

        The message will get recorded in the debug file
        """
        with FileLock("Debug from PID {0}".format(os.getpid()),
                      logging_lockfile):
            self.logger.debug(msg)

    def error(self, msg):
        """
        Print an error message

        The message will get recorded in the debug file and the log file,
        and printed to stderr.
        """
        with FileLock("Error from PID {0}".format(os.getpid()),
                      logging_lockfile):
            self.logger.error(msg)

    def send_email(self, subject, body, to_address):
        """
        Send an email to the supplied body to the specified address, logging
        any errors that are hit.
        """
        with tempfile.NamedTemporaryFile() as tmpfile:
            tmpfile.write(body)
            tmpfile.seek(0)

            mail_cmd = ["mail",
                        "-s", "enbackup: " + subject,
                        to_address]
            (rc, output, err) = run_cmd_output(mail_cmd,
                                               self,
                                               cmd_stdin=tmpfile.fileno())

        if rc != 0:
            self.error("Failed to send email to {0}, return code {1}:\n"
                       "{1}\n{2}".format(to_address, rc, output, err))

