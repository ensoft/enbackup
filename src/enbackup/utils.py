###############################################################################
# utils.py - enbackup utilities
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial functionality
###############################################################################
#
# Utility functions for the enbackup package
#

import os
import sys
import fcntl
import string
import time
import subprocess
import pwd
import ConfigParser

#
# Global configuration:
#
global_lock_dir = "/var/lock/enbackup/"
global_config_file = "enbackup.rc"
cfg_file_dir = "/etc/enbackup.d/"
cfg_file_path = os.path.join(cfg_file_dir, "enbackup.rc")


###############################################################################
# Locking
###############################################################################
        

class LockError(Exception):
    """Exception raised for locking errors.

    Attributes:
        op  -- operation (lock, unlock, etc)
        msg -- explanation of the error
    """
    def __init__(self, op, msg):
        self.op  = op
        self.msg = msg
        super(LockError, self).__init__(op, msg)


def chown_enbackup(path):
    """
    Change ownership of a file/directory to the enbackup user, if possible
    """
    if os.geteuid() == 0:
        enbackup_uid = pwd.getpwnam(enbackup_user).pw_uid
        enbackup_gid = pwd.getpwnam(enbackup_user).pw_gid
        os.chown(path, enbackup_uid, enbackup_gid)


def makedirs(path, logger, change_ownership):
    """
    Create a directory, along with any required parent directories.

    If 'change_ownership' is True and the function is run as root the created
    directories' ownership will be changed to that of the global enbackup user;
    otherwise they are owned by the UID of the caller.

    Takes no action if the specified directores already exist.
    Raises an exception if any directories do not exist but cannot be created,
    or if the specified path is not a directory.
    """
    #
    # First normalize the path to remove extra '/'s.  This makes sure the
    # 'dirname' call separates the path items correctly.
    #
    normalized = os.path.normpath(path)

    if not os.path.exists(normalized):
        #
        # Recursively create parent directories, then create this directory.
        #
        parentdir = os.path.dirname(normalized)
        makedirs(parentdir, logger, change_ownership)
        if logger != None:
            logger.log("Creating {}".format(normalized))
        os.mkdir(normalized, 0755)
        #
        # Change ownership if requested (this does nothing if not running
        # as root).
        #
        if change_ownership:
            chown_enbackup(normalized)

    if not os.path.isdir(normalized) and logger != None:
        logger.error("Expected {} to be a directory".format(normalized))


class FileLock(object):
    """
    File lock for synchronization between processes
    """
    fd = None
    locked = False

    def __init__(self, owner_id, lockfile):
        #
        # Create the lockfile, if it doesn't already exist, as well as
        # any parent directories.
        #
        self.lockfile = global_lock_dir + lockfile
        self.owner_id = owner_id
        
        if not os.path.exists(self.lockfile):
            parent_dir = os.path.dirname(self.lockfile)
            makedirs(parent_dir, None, True)

            open(self.lockfile, "a").close()
            chown_enbackup(self.lockfile)

    def lock(self):
        """
        Aquire the lock
        """
        #
        # Open the file and lock it using flock...
        #
        self.fd = open(self.lockfile)
        fcntl.flock(self.fd.fileno(), fcntl.LOCK_EX)
        #
        # ...and write some useful information
        #
        writing_fd = open(self.lockfile, "w")
        writing_fd.write("Time: {0}\n"
                         "Owner ID: {1}\n".
                         format(time.ctime(), self.owner_id))
        writing_fd.flush()
        writing_fd.close()
        self.locked = True

    def unlock(self):
        """
        Release the lock.  Asserts that the lock is held.
        """
        #
        # Unlock the file.  The only way this can fail is if we don't
        # have the lock, in which case the fd will be invalid and the
        # below code will throw an exception.
        #
        assert(self.locked)
        fcntl.flock(self.fd.fileno(), fcntl.LOCK_UN)
        self.fd.close()
        self.fd = None
        self.locked = False

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, exctype, value, tb):
        self.unlock()
        return False


class DirLock(FileLock):
    """
    Directory lock for synchronization between processes, to ensure no
    two enbackup scripts are attempting to read/write a backup directory
    at the same time
    """
    def __init__(self, owner_id, lock_dir):
        #
        # Normalize the directory path and flatten it into a single
        # lock file name, then use the standard initialization:
        #
        lock_dir = os.path.normpath(lock_dir)
        lock_file = lock_dir.replace("/", "_") + ".lock"
        super(DirLock, self).__init__(owner_id, lock_file)


class GlobalLock(FileLock):
    """
    Global lock for synchronization between processes.  
    """
    def __init__(self, owner_id):
        super(GlobalLock, self).__init__(owner_id, "enbackup_global.lock")


###############################################################################
# Output filtering
###############################################################################
        

class StringFilter(object):
    """
    A simple filter used for skipping unwanted output in log emails
    """
    def __init__(self, filter_file):
        #
        # Open the filter file:
        #
        f = open(filter_file)

        version = f.readline()

        if version == "1\n":
            #
            # We only support exact full-line matches.  Add each line to
            # a set; we'll use presence in the set to work out whether
            # to filter.
            #
            self.filter_set = set([line.rstrip("\n") for line in f])
        else:
            sys.stderr.write("Unrecognized version, no filtering")
            self.filter_set = set([])


    def match(self, line):
        """
        Check whether a line matches the filter, returning True if it
        does, False otherwise.
        """
        if line in self.filter_set:
            return True
        else:
            return False

    def filter(self, lines, do_annotate):
        """
        Filter a collection of lines, i.e. return a list which contains only
        those lines that do *not* match the filter.

        Argument: lines
          List of lines to filter

        Argument: do_annotate
          If set to True, the filtered output will include annotations
          (of the form 'Filtered X lines') when lines are removed from
          the output.
        """
        filtered_count = 0
        output = []

        for line in lines:
            line = line.rstrip("\n")
            if not self.match(line):
                if filtered_count > 0 and do_annotate:
                    output.append(
                        "[ ... Filtered {0} lines ... ]".format(filtered_count))
                    filtered_count = 0
                    
                output.append(line)
            else:
                filtered_count = filtered_count + 1

        return output


###############################################################################
# Utility/helper functions
###############################################################################


def get_username():
    return enbackup_user
        

def run_cmd(cmd, logger):
    """
    Run a command, logging a debug message before starting, and
    again if an error is returned.  Any errors result in exceptions.

    cmd:    the command to run, in the form of a list where the first
            element is the executable name, and the subsequent elements
            are the arguments.
    logger: enbackup logger to use for debug
    """

    try:
        logger.log("Running command: {0}".format(cmd))
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        logger.error("Failed to run {0}, return code {1}".format(cmd,
                                                                 e.returncode))
        raise e
    except:
        logger.error("Unhandled exception when calling {0}".format(cmd))
        raise


def run_cmd_output(cmd, logger, cmd_stdin=None):
    """
    Run a command, logging a debug message before starting, and
    again if an error is returned.  Errors are returned to the caller.

    cmd:    the command to run, in the form of a list where the first
            element is the executable name, and the subsequent elements
            are the arguments.
    logger: enbackup logger to use for debug
    cmd_stdin: optionally specify a file descriptor for the process to use
            as standard input.

    Returns a tuple containing (returncode, stdout, stderr)
    """

    #
    # It would be simpler to just use subprocess.check_output() here, but
    # unfortunately that won't exist until Python 2.7...
    #

    logger.log("Running command: {0}".format(cmd))
    p = subprocess.Popen(cmd,
                         stdin=cmd_stdin,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    (output, err) = p.communicate()

    return (p.returncode, output, err)


###############################################################################
# Module initialization
###############################################################################


#
# Read the global config file to find out the username to use for all
# processing, and verify that the user exists:
#
config = ConfigParser.ConfigParser({ "username": "enbackup" })
config.read(cfg_file_path)
enbackup_user = config.get("Global", "username")

