###############################################################################
# backup.py - linux based backup script using rdiff-backup
#
# October 2006, Anthony Toole
#
# Copyright (c) 2006-2012 by Ensoft Ltd.
# All right reserved
#
# Version 1.0 - all working, but everything hard coded within script
#
# Version 2.0 - now reads behaviour from config file, and has locking to stop
#               multiple scripts running concurrnetly
#
# Version 2.1 - few fixes and enhancements:
#               - stats log created automatically if missing
#               - stats log can optionally update with each run
#               - can override output dir on CLI
#               - for src_dir_list can specify exclusions
#               - block backups to do we don't write root files to
#               - stick server name in email subject and body
#
# Version 2.2 - few minor bug fixes:
#               - remove : from log file names (upsets samba)
#               - don't barf if CLI specified tgt_dir doesn't exist
#               - better logging if tgt_dir is broken symlink
#
# Version 3.0 - modified to work in "cp" type mode to "scp" type mode, if
#               either of source and destination server are provided in config
#               file.
#
# Version 3.1 - add ability to retry for a fixed time ('retry_time') if some
#               other process owns the lock.  This should cope better with
#               enbackup and enbackup-archive both trying to work at once.
#             - allow a user configurable reference file name (mostly for
#               testing, though could be useful in real world).
#
# Version 3.2 - exclude VNC log files, seeing some very large logs we want to
#               always exclude.
#
# Version 3.3 - allow scriping of backup "ageing" to keep the backup sizes on
#               servers to reasonable size..
#
# Version 3.4 - when talking to remote servers, check we can ping them before
#               we try anything - this means when servers go down again
#               things will be handled more gracefully (eg. it won't appear as
#               if all backup sets failed just because the remote server isn't
#               there).
#
# Version 3.5 - exclude fuse mounted file systems (or in fact any remote
#               filesystem).
#
# Version 3.6 - script changes to handle writing backup destination files as
#               root (instead of regular users)
#
# Version 3.7 - previously only a single set of backup sets was allowed (as
#               defined by src_dir_list and src_dir_single config).  Made
#               changes to allow multiple sets of sets - done by calling
#               enbackup.py multiple times with different config (including
#               source and destination directories, and the reference file
#               name).
# Version 3.8 - more enhancements to cope with running multiple instances of
#               enbackup on a single machine in a single day:
#               - always sleep if lock file exists (don't special case when the
#                 lock is owned by an instance of enbackup)
#               - aquire lock after we have read config file, so we can send a
#                 useful email.  This still leaves corner cases where we get
#                 the fall back email, but they are very unlikely to be hit by
#                 cron'd instance (so email less useful)
#               - don't barf if lock file isn't expected format.
#
# Version 3.9 - add --create-full-path as the default behaviour, so that
#               destination directories (including on remote servers) get
#               created automatically.
#
# Version 4.0 - Renamed from enbackup.py to backup.py; the script is now run
#               as 'enbackup backup ...'
#               Added options for running in remote mode, and triggering
#               a remote mirror via 'enbackup notify' at the end of a backup.
###############################################################################


import datetime
import os
import re
import smtplib
import sys
import time
import traceback
import pickle
import pprint
import optparse
import rdiff_backup.Time
import rdiff_backup.statistics
import pwd
import grp
import subprocess
import socket
from enbackup.utils import DirLock, GlobalLock
from enbackup.utils import run_cmd, get_username
import enbackup.log
import enbackup.cmd.notify

#
# This script is used to automate backups of all machines on a given network,
# where all files to be backed up are visible as locally mounted files (eg.
# files on disk, mounted via SMB, etc.).
#
# The script operates on a set of backup 'sets' - each typically corresponding
# to a single directory on a server (or in the case of home directories,
# corresponding to a single user's home directory).
#
# This script wraps the call to 'rdiff-backup' in a way that allows automated
# analysis of changes to the backup size, logging of errors, emailing
# results, etc.
#


###############################
# START: Configurable options #
###############################

#
# Set the default rdiff options for backup here.
#
# We use the following options:
#  * display statistics (which we parse when forming the log file),
#  * force it to be a backup operation (otherwise rdiff-backup can
#    sometimes guess we are trying to restore incorrectly - if it
#    spots an "rdiff-backup-data" directory),
#  * skip filesystems mounted from other devices
#  * create target directories automatically (to handle initial
#    backup of new data).
#  * exclude all special files apart from symlinks -- we don't
#    expect any of the data we back up to include devices, pipes,
#    or sockets, and trying to back them up is only likely to
#    cause confusion.
#
# We add the 'default_exclude_directories' entries to this, as well as any
# config specified paths
#
default_rdiff_options_backup = " ".join(["--backup --print-statistics",
                                         "--exclude-other-filesystems",
                                         "--create-full-path",
                                         "--exclude-sockets",
                                         "--exclude-fifos",
                                         "--exclude-device-files"])

#
# Set the default rdiff options for local backups here - ie. for backups in
# which the destination is on the same host as the script is being run.
#
default_rdiff_options_backup_local = " ".join([" --user-mapping-file",
                                               "{0}",
                                               "--group-mapping-file",
                                               "{1} "])

#
# Set the default rdiff options for backup here.
#
# Need to use "force" - as otherwise only a single backup set can be removed at
# a time.
#
default_rdiff_options_age = " ".join(["--force",
                                      "--remove-older-than",
                                      "%s"])

#
# Set default exclude directories - we always exclude the "nobackup" directory,
# to match the previous behaviour and "tgt-linux" to exclude the linux builds.
#
default_exclude_directories = ["nobackup", "tgt-linux", ".vnc/*.log",
                               ".gvfs", ".pst"]
exclude_directory_fmt = ' --exclude "**%s"'

#############################
# END: Configurable options #
#############################


########################################
# START: Parameters set by config file #
########################################

#
# If this is set to True then will NOT do any backups.  It can do some things
# - such as creating intermediate directories, but largely it is a no-op from
# backup point of view.
#
show_only = False

#
# Directory to write the backups to.
#
backup_output_dir = None

#
# NOTE: these are the variables that will be changed to add or remove backup
#       'sets'.
#
backup_sets = []

#
# E-mail addresses to send logs to.  To avoid changing this value based on
# where used, pick an address that is reachable from all systems.
#
# This is only needed if we fail early (otherwise we get the appropriate value
# from config)..  Could fix it to always read the value from config file, but
# would need to cope with invalid config file (though that should be very rare
# case).
#
logfile_email_to = "root@localhost"

#
# rdiff-backup reference version.  We print a warning if the current version
# changes.
#
rdiff_backup_reference_version = None

#
# threshold_TYPE
#
# When reporting changes to backup times or sizes, only report the problem if
# the old or new values are greater than these minimum values - and if the
# change is greater than the percentage change.
#
threshold_time  = None
threshold_bytes = None
threshold_percentage = None

#
# Set to name of source and target server - format for server name is "host::"
# as rdiff-backup wants it.
#
# If either server is "" then the source/destination files will be assumed to
# be local (as appropriate).
#
source_server = ""
target_server = ""

#
# If set to a non-zero value, and the script finds a lock file _from another
# process_ then it will try to obtain the lock for this time (in minutes)
#
# Due to getting the lock before actually reading the config file, this value
# isn't (currently) configurable.  Could fix that if that was ever an issue.
#
# This number is currently pretty large - if we could speed up copies to the
# external disks that would help (taking ~12 hours, starting at around 18:00)
#
retry_time = 300

######################################
# END: Parameters set by config file #
######################################


#
# stats_type_TYPE
#
# Passed to eg. percentage() to identify that a value is of a given type, as
# this determins what the threshold values are.
#
stats_type_bytes = "bytes"
stats_type_time  = "time"
stats_type_count = "count"

#
# Directory to which backup logs and stats are written.
#
log_output_dir = "/var/log/enbackup/"

#
# Log file written to 'log_output_dir'
#
log_file_name = "enbackup.log"

#
# Files containing mappings for all users and groups respectively (needed for
# rdiff-backup, to map every user and group on to root.root - to minmise the
# chance of someone being able to overwrite both the original file and the
# backup.  The only exception this cannot fix are "other" writable files).
#
users_dump_file_name = os.path.join(log_output_dir, "enbackup-users.dump")
groups_dump_file_name = os.path.join(log_output_dir, "enbackup-groups.dump")

#
# Absolute path to the reference stats file, found in 'log_output_dir'
#
reference_file_name = "enbackup-reference.stats"
reference_file_name_full = os.path.join(log_output_dir, reference_file_name)

#
# Time to sleep for between retries.  See 'retry_time'.
#
retry_sleep_time = 60

############################
# START: Global variables  #
############################

#
# Name of the lock file that prevents multiple processes running on one
# machine, and flag showing if this process acquired the flag.
#
# Intentionally heavy handed with the locking granularity.
#
lock_file_name = "/tmp/enbackup.lock"
lock_file_acquired = False
server_lock = GlobalLock("Process {0}, PID {1}".format(sys.argv[0],
                                                       os.getpid()))

#
# Local server name used in logs
#
local_server_name = os.uname()[1]

#########################
# END: Global variables #
#########################


#
# log_string/debug_string
#
# Write a message to either the backup log or debug log - auto appending a
# newline after each string.
#
# Logfile is emailed each day so should contain minimal set of information
# needed.  Debug file is intended to track more information.
#
debugfile = None
logfile = None
logger = None

def log_string(string):
    logger.log(string)
    if logfile:
        logfile.write(string + "\n")
        logfile.flush()
        if debugfile:
            debugfile.write(string + "\n")
            debugfile.flush()
    else:
        print(string)


def debug_string(string):
    logger.debug(string)
    if debugfile:
        debugfile.write(string + "\n")
        debugfile.flush()
    else:
        print(string)


###########################
# START: Worker Functions #
###########################

def remote_host_available(hostname):
    ping = subprocess.Popen("ping -c 1 %s" % (hostname),
                            shell=True,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            close_fds=True)
    return (ping.wait() == 0)


#
# Function to handle parsing of the raw output to get the stats
#
# Returns: a dictionary containing the parsed statisitics for this backup set.
#
def parse_stats(raw_output):
    #
    # For most of these we primarily want to match the most basic value (time
    # in seconds, size in bytes, etc.) - but may want to grab the prettier
    # version too (eg. "1.5Mb")
    #
    # We assume that none of this parsing can fail - if it has then either the
    # process failed to run (which we've already checked), or something went
    # wrong with rdiff-backup (in which case we just let the exception be
    # caught, as we cannot handle it).
    #
    try:
        new_stats = {}

        new_stats["time"] = re.match("ElapsedTime ([.0-9]*) \((.*)\)",
                                     raw_output[3]).groups()
        new_stats["src_count"] = re.match("SourceFiles ([.0-9]*)",
                                          raw_output[4]).groups()
        new_stats["src_size"] = re.match("SourceFileSize ([0-9]*) \((.*)\)",
                                         raw_output[5]).groups()
        new_stats["mir_count"] = re.match("MirrorFiles ([.0-9]*)",
                                          raw_output[6]).groups()
        new_stats["mir_size"] = re.match("MirrorFileSize ([0-9]*) \((.*)\)",
                                         raw_output[7]).groups()
        new_stats["mir_size_incr"] = re.match(
                               "TotalDestinationSizeChange ([-0-9]*) \((.*)\)",
                               raw_output[17]).groups()
        new_stats["error_count"] = re.match("Errors ([0-9]*)",
                                            raw_output[18]).group(1)
    except:
        log_string("*" * 60)
        log_string("ERROR: parsing of stats failed - has rdiff-backup "
                   "returned junk?")
        for line in raw_output:
            log_string("  >> %s" % (line.strip()))
        raise

    return(new_stats)


#
# Given the old and new values, calculate the percentage change in the value.
#
# If the change is found to be too large then a WARNING message will be printed
# in the log.  To avoid reporting problems unnecessarily, we supress the
# warnings if old and new values are both below a certain threshold.
#
# Argument: new_value, new_string, old_value, old_string
#
#     Previous and current values of the backup size/time/etc.  The *_value
#     version will be the value in the smallest unit for that item (seconds for
#     time, bytes for size, etc) and is used for the calculations.  The
#     *_string parameters are the nicer versions of these values for printing
#     in messages (eg. 2.5 Mb, 1m20s, etc.).
#
# Argument: backup_set
#
#     Path of this backup set (eg. "/var") used in debug/log messages.
#
# Argument: stats_name
#
#     Long description of the stats name, used in debug/log messages.
#
# Argument: stats_type
#
#     Value showing what the value being compared is - one of the
#     stats_type_TYPE values.
#
def percentage_change(new_value, new_string,
                      old_value, old_string,
                      backup_set,
                      stats_name,
                      stats_type):
    new = float(new_value)
    old = float(old_value)

    if old > 0:
        pc_change = (new - old) * 100 / old
    elif new > 0:
        pc_change = 100
    else:
        pc_change = 0

    #
    # If the value has changed by more than 10% in either direction, and one or
    # both or the values are greater than the threshold then report the
    # problem.  Note: for error counts we do not currently have a threshold,
    # as all errors are unexpected.
    #
    if pc_change > threshold_percentage or pc_change < -(threshold_percentage):
        if (stats_type == stats_type_time and
            old < threshold_time and
            new < threshold_time):
            debug_string("ignoring warning for small time: %s for '%s' "
                         "has changed by %d%% (%s -> %s)" %
                         (stats_name, backup_set, pc_change,
                          old_string, new_string))
        elif (stats_type == stats_type_bytes and
              old < threshold_bytes and
              new < threshold_bytes):
            debug_string("ignoring warning for small size: %s for '%s' has "
                         "changed by %d%% (%s -> %s)" %
                         (stats_name, backup_set, pc_change,
                          old_string, new_string))
        else:
            log_string("WARNING: %s for '%s' has changed by %d%% (%s -> %s)" %
                       (stats_name, backup_set, pc_change,
                        old_string, new_string))

    return(int(pc_change))


#
# Given a set of new stats, and optionally a set of reference stats, return a
# string containing the values.
#
def get_stats_string(backup_set, new_stats, old_stats):
    if old_stats:
        pc_change = percentage_change(new_stats["src_size"][0],
                                      new_stats["src_size"][1],
                                      old_stats["src_size"][0],
                                      old_stats["src_size"][1],
                                      backup_set, "source directory",
                                      stats_type_bytes)
        srcdir = "%s (%s%%), %s files" % (new_stats["src_size"][1], pc_change,
                                          new_stats["src_count"][0])

        pc_change = percentage_change(new_stats["mir_size"][0],
                                      new_stats["mir_size"][1],
                                      old_stats["mir_size"][0],
                                      old_stats["mir_size"][1],
                                      backup_set, "mirror directory",
                                      stats_type_bytes)
        mirdir = "%s (%s%%), %s files" % (new_stats["mir_size"][1], pc_change,
                                    new_stats["mir_count"][0])

        pc_change = percentage_change(new_stats["time"][0],
                                      new_stats["time"][1],
                                      old_stats["time"][0],
                                      old_stats["time"][1],
                                      backup_set, "elapsed time",
                                      stats_type_time)
        time = "%s (%s%%)" % (new_stats["time"][1], pc_change)

        pc_change = percentage_change(new_stats["error_count"],
                                      new_stats["error_count"],
                                      old_stats["error_count"],
                                      old_stats["error_count"],
                                      backup_set, "error count",
                                      stats_type_count)
        error = "%s (%s%%)" % (new_stats["error_count"], pc_change)
    else:
        srcdir = "%s, %s files" % (new_stats["src_size"][1],
                                   new_stats["src_count"][0])
        mirdir = "%s, %s files" % (new_stats["mir_size"][1],
                                   new_stats["mir_count"][0])
        time   = "%s" % (new_stats["time"][1])
        error  = "%s" % (new_stats["error_count"])

    retval =  "%s\n" % (backup_set)
    retval += "%s\n" % ("=" * len(backup_set))
    retval += "SourceDirectory: %s\n" % (srcdir)
    retval += "MirrorDirectory: %s\n" % (mirdir)
    retval += "MirrorIncrement: %s\n" % (new_stats["mir_size_incr"][1])
    retval += "ElapasedTime:    %s\n" % (time)
    retval += "ErrorCount:      %s\n" % (error)

    return(retval)


#
# Calculate the total from a set of statistics, and return a matching
# dictionary entry.
#
def calculate_totals(stats):
    num_time          = 0.0
    num_src_count     = 0
    num_src_size      = 0
    num_mir_count     = 0
    num_mir_size      = 0
    num_mir_size_incr = 0
    num_error_count   = 0

    byte_to_str = rdiff_backup.statistics.StatsObj().get_byte_summary_string

    for backup_set in stats.keys():
        num_time          += float(stats[backup_set]["time"][0])
        num_src_count     += int(stats[backup_set]["src_count"][0])
        num_src_size      += int(stats[backup_set]["src_size"][0])
        num_mir_count     += int(stats[backup_set]["mir_count"][0])
        num_mir_size      += int(stats[backup_set]["mir_size"][0])
        num_mir_size_incr += int(stats[backup_set]["mir_size_incr"][0])
        num_error_count   += int(stats[backup_set]["error_count"])

    totals = {}
    totals["time"]          = [num_time,
                               rdiff_backup.Time.inttopretty(num_time)]
    totals["src_count"]     = [num_src_count]
    totals["src_size"]      = [num_src_size,
                               byte_to_str(num_src_size)]
    totals["mir_count"]     = [num_mir_count]
    totals["mir_size"]      = [num_mir_size,
                               byte_to_str(num_mir_size)]
    totals["mir_size_incr"] = [num_mir_size_incr,
                               byte_to_str(num_mir_size_incr)]
    totals["error_count"]   = num_error_count

    return totals


#
# Acquire the lock to prevent any other rdiff process writing to the output
# directory.  We are very heavy handed with the locking to avoid anyone
# managing to somehow screw things up accidentally - one lock per machine.
#
def get_backup_lock():
    global server_lock

    log_string("About to acquire global lock...")
    server_lock.lock()
    log_string("Acquired global lock")


#
# Release the backup lock - obviously only if this process acquired it.
#
def release_backup_lock():
    global server_lock

    if server_lock.locked:
        server_lock.unlock()
    log_string("Lock released")


#
# Parse a file for a set of options, where each options is a single line of the
# form "item: value".  Options can be either single values or multiple values.
#
# Note: If only single values are required, then having multiple values
# specified in the file will cause earlier instances to be ignored.
#
# Note: Any options in the file not matching entries in 'single' or 'multi'
# will just be silently ignored, as will any lines that don't fit
# "item: value" pattern.
#
# Return: dictionary of {item: value} where value is single item or list of
# items depending on whether it is single or multi option.
#
# Argument: file
#     Configuration file to read.
#
# Arugment: single
#     Dictionary of {item: default_value} listing all single value options
#     required.
#
# Argument: multi
#     Dictionary of {item: [default_value]} listing all multi value options
#     required.
#
def parse_config_file(filename, single, multi):
    opts_single = {}
    opts_multi = {}

    for item in single:
        opts_single[item] = None
    for item in multi:
        opts_multi[item] = []

    for line in open(filename).readlines():
        res = re.findall("^([a-z_]*): (.*)", line)
        if len(res) == 1:
            item, value = res[0]
            if item in opts_single:
                opts_single[item] = value
            if item in opts_multi:
                opts_multi[item].append(value)

    for item in opts_single:
        if opts_single[item] == None:
            opts_single[item] = single[item]
    for item in opts_multi:
        if len(opts_multi[item]) == 0:
            opts_multi[item] = multi[item]

    #
    # Merge options into one, single takes priority
    # (though we shouldn't really get duplicates!)
    #
    for item in opts_multi:
        if not item in opts_single:
            opts_single[item] = opts_multi[item]

    return(opts_single)

#########################
# END: Worker Functions #
#########################


#
# Now actually think about doing the backup!
#
# The statistics reported by rdiff-backup aren't entirely useful for spotting
# the conditions we want - as the only delta it can report is the change in
# mirror size (which is always positive - because the mirror is never flushed).
#
# Hence we need to load a set of reference values to compare against.
#
def backup_main(rdiff_options, report_incremental_stats, starttime):
    new_stats = {}        # dictionary containing parsed stats from this run
    error_count_batch = 0 # errors affecting whole backup sets
    error_count_set = 0   # errors within any backup set
    new_stats_filename = None
                          # filename to write the latest stats set

    #
    # Check the version of rdiff-backup.  If this has changed we print a
    # warning, but carry on and hope nothing is broken :)
    #
    cmd = "rdiff-backup -V"
    debug_string("Running '%s'" % (cmd))
    proc_fd = os.popen(cmd)
    raw_output = proc_fd.readlines()
    retval = proc_fd.close()
    if retval:
        log_string("ERROR: failed to run '%s' to check rdiff-backup "
                   "version: %d" % (retval))
        error_count_batch += 1
    else:
        cur_version = raw_output[0].strip()
        if cur_version != rdiff_backup_reference_version:
            log_string("ERROR: rdiff-backup version mismatch"
                       " - wanted '%s', got %s" %
                       (rdiff_backup_reference_version, cur_version))
            error_count_batch += 1

    #
    # Now work through the list of backup sets, preforming each backup!
    #
    for backup_set in backup_sets:
        backup_set_dir = backup_output_dir + backup_set

        debug_string("*" * 60)
        if (not os.path.exists(backup_set_dir)) and (target_server == ""):
            log_string("WARNING: Making path '%s' - expected only if backup "
                       "set new" % (backup_set_dir))
            os.makedirs(backup_set_dir)
        else:
            debug_string("Handling set '%s'" % (backup_set))

        options = rdiff_options

        options_file = "%s/.rdiff-backup" % (backup_set)
        if os.path.exists(options_file):
            debug_string("Have config file for %s" % (backup_set))
            debug_string("".join(open(options_file).readlines()))
            options += " --include-globbing-filelist %s" % (options_file)
        else:
            debug_string("No config file for %s" % (backup_set))

        debug_string("Rdiff options: %s" % (options))
        debug_string("")

        #
        # Run the backup command
        #
        # If this fails for a single backup set we keep going - this could
        # simply be that the .rdiff-backup file is badly formed
        #
        cmd = "rdiff-backup {0} {1}{2} {3}{4}/".format(
              options, source_server, backup_set,
              target_server, backup_set_dir)
        debug_string("Running '%s'" % (cmd))
        if not show_only:
            proc_fd = os.popen(cmd)
            raw_output = proc_fd.readlines()
            retval = proc_fd.close()
            if retval:
                log_string("ERROR: rdiff-backup command failed for '%s': %d" %
                           (backup_set, retval))
                error_count_batch += 1
            else:
                debug_string("rdiff-backup success!")
                new_stats[backup_set] = parse_stats(raw_output)
                error_count_set += int(new_stats[backup_set]["error_count"])

    #
    # Now work out the totals, and stick those the the new_stats dictionary.
    # [we do this now so that these values are stored in the .stats file]
    #
    new_stats["TOTAL"] = calculate_totals(new_stats)

    debug_string("All new stats:")
    debug_string(pprint.pformat(new_stats))

    #
    # Open the reference file.  If it doesn't exist then we report that as an
    # error but otherwise carry on as normal.  Also store the current stats in
    # a file with the current timestamp (no need to check it exists - we've
    # already validates the time for the log file).
    #
    if os.path.exists(reference_file_name_full):
        old_stats = pickle.load(open(reference_file_name_full))
    else:
        log_string("ERROR: no reference stats to compare.  "
                   "This is unexpected unless this is a new backup.  "
                   "The current stats will be written now.")
        error_count_batch += 1
        old_stats = {"TOTAL": 0}

    #
    # Store the current stats into a file, and if required also overwrite the
    # reference file with the nightly stats.
    #
    new_stats_filename = os.path.join(log_output_dir, starttime + ".stats")
    pickle.dump(new_stats, open(new_stats_filename, "w"))
    if report_incremental_stats or not os.path.exists(reference_file_name_full):
        pickle.dump(new_stats, open(reference_file_name_full, "w"))

    #
    # If we have reference stats then compare the values..
    #
    # Most important is to check that every set in "old_stats" has a
    # corresponding value in "new_stats".  We can also check the reverse is
    # true, though that can happen when backup sets are added and the reference
    # file is out of date.
    #
    stats_summary_str = []

    #
    # Sort the keys so the order looks good, and make sure TOTAL is at the
    # beginning
    #
    keys = old_stats.keys()
    keys.sort()
    keys.remove("TOTAL")
    keys.insert(0, "TOTAL")
    for backup_set in keys:
        if not new_stats.has_key(backup_set):
            error_count_batch += 1
            log_string("ERROR: Backup set '%s' in reference does not exist in "
                       "latest backup" % (backup_set))
        else:
            stats_summary_str.append(
                get_stats_string(backup_set,
                                 new_stats[backup_set], old_stats[backup_set]))

    keys = new_stats.keys()
    keys.sort()
    for backup_set in keys:
        if not old_stats.has_key(backup_set):
            if len(old_stats) > 0:
                error_count_batch += 1
                log_string("ERROR: Backup set '%s' in new backup does not "
                           "exist in reference set" % (backup_set))
            stats_summary_str.append(
                get_stats_string(backup_set, new_stats[backup_set], None))


    #
    # Write to the log file the summary of any errors that occured..
    #
    if error_count_batch > 0:
        log_string("ERROR: hit errors affecting entire backup sets: %d" %
                   (error_count_batch))
    if error_count_set > 0:
        log_string("ERROR: hit errors within one or more backup sets: %d" %
                   (error_count_set))

    #
    # Finally write the stats summary to the log file
    #
    log_string("")
    for line in stats_summary_str:
        log_string(line)


#
# Now actually think about doing the aging backup!
#
def age_main(rdiff_options):
    error_count_batch = 0 # errors affecting whole backup sets

    #
    # Now work through the list of backup sets, preforming each backup!
    #
    for backup_set in backup_sets:
        backup_set_dir = backup_output_dir + backup_set

        debug_string("*" * 60)
        if (not os.path.exists(backup_set_dir)) and (target_server == ""):
            log_string("WARNING: Making path '%s' - expected only if backup "
                       "set new" % (backup_set_dir))
            os.makedirs(backup_set_dir)
        else:
            debug_string("Handling set '%s'" % (backup_set))

        options = rdiff_options

        debug_string("Rdiff options: %s" % (options))
        debug_string("")

        #
        # Run the backup command
        #
        # If this fails for a single backup set we keep going - this could
        # simply be that the .rdiff-backup file is badly formed
        #
        cmd = "rdiff-backup %s %s%s/" % (options, target_server, backup_set_dir)
        debug_string("Running '%s'" % (cmd))
        if not show_only:
            proc_fd = os.popen(cmd)
            raw_output = proc_fd.readlines()
            retval = proc_fd.close()
            if retval:
                log_string("ERROR: rdiff-backup command failed for '%s': %d" %
                           (backup_set, retval))
                error_count_batch += 1
            else:
                debug_string("rdiff-backup success!")
                increment_count = 0
                for line in raw_output:
                    matched = False
                    line = line.strip()
                    debug_string(line)
                    ignore_str = ["No increments older than",
                                  "Deleting increments at times:",
                                  "Deleting increment at time:"]
                    increment_regex = re.compile("... ... .. ..:..:.. ....")
                    res = increment_regex.match(line)
                    if res:
                        increment_count += 1
                    else:
                        for string in ignore_str:
                            if line[0:len(string)] == string:
                                matched = True
                        if not matched:
                            log_string("Unexpected ouput: %s" % (line))
                log_string("Removed %u increments for %s" %
                           (increment_count, backup_set_dir))

    #
    # Write to the log file the summary of any errors that occured..
    #
    if error_count_batch > 0:
        log_string("ERROR: hit errors affecting entire backup sets: %d" %
                   (error_count_batch))


#################################################################
# START: Code in which exceptions are intentionally not caught  #
#################################################################


def openlog_local(timestamp):
    #
    # Open a log file - with a name containing date/time.
    #
    global logger
    global logfile

    logger = enbackup.log.Logger("backup")

    if not os.path.exists(log_output_dir):
        os.makedirs(log_output_dir)

    logname = os.path.join(log_output_dir, "%s.log" % (timestamp))

    if os.path.exists(logname):
        print("ERROR: File '%s' already exists - exiting." % (logname))
        sys.exit(-1)

    logfile = open(logname, "w")

    return logname


def openlog_remote():
    global logger

    logger = enbackup.log.Logger('remote rdiff')


#
# The main functionality when running in remote mode.  In this case we
# just need to run rdiff-backup in server mode, to allow the originating
# rdiff-backup to copy files to this server.
#
def do_remote(parser_options):
    #
    # Work required:
    #  - Lock the target directory
    #  - Spawn rdiff-backup and wait for it to complete
    #  - Unlock the target directory
    #
    # First though, we need to read in the arguments to find out
    # what the target directory is, and get some details about the
    # originating server to write into the lockfile.
    #
    error_hit = False
    openlog_remote()

    if parser_options.tgt_dir is None or \
       parser_options.src_server is None:
        logger.error("ERROR: target directory and source server "
                     "must be specified when running in remote mode")
        error_hit = True

    if not error_hit:
        tgt_dir = parser_options.tgt_dir
        src_server = parser_options.src_server

        owner_id = "Server: {0}, Process 'enbackup'\n".format(src_server)

        #
        # Now run rdiff-backup in server mode.  Note there's no need for
        # any additional arguments.  If this fails, just let the exception
        # propagate up to the top level so it gets returned to whoever
        # invoked the script.
        #
        rdiff_backup_cmd = "rdiff-backup --server"

        logger.debug("About to lock '{0}' during rdiff-backup".format(tgt_dir))
        with DirLock(owner_id, tgt_dir):
            run_cmd(rdiff_backup_cmd.split(), logger)
        logger.debug("rdiff-backup complete")

#
# The main functionality when running in local mode.  In this case we
# actually need to initiate and monitor a full backup run based on
# a supplied configuration file.
#
def do_local(parser, parser_options, parser_args):
    #
    # Check we are root - if not barf.  Should only hit this if being run
    # manually, or someone has setup cron job wrongly.
    #
    if (os.geteuid() != 0):
        print("ERROR: backup command must be run as root - aborting.  "
              "(EUID %s)" % (os.geteuid()))
        sys.exit(-1)

    #
    # From this point onwards, use a flag to record whether or not a 
    # non-terminal error is hit.  In very severe cases we just exit, but
    # in most cases we want to continue so that a log is stored and emailed.
    #
    # Also initialize the local backup log file and record that we're starting
    # a backup, and set up default values controlling script behavior:
    #
    # report_incremental_stats:
    #   If set to true then we write the latest stats to the reference file
    #   after doing the comparison against the old file (if it existed).
    #   This is useful on automated systems, where backups are expected to
    #   change a lot - this way the stats reported are deltas against the
    #   last run.
    #
    # config_file:
    #   Mandatory CLI option controlling what gets backed up.
    #
    error_hit = False
    perform_backup = True
    report_incremental_stats = False
    config_file = None

    #
    # Define the global variables we might update in this function (when
    # we override default values with user configuration).
    #
    global logfile_email_to
    global rdiff_backup_reference_version
    global source_server
    global target_server
    global threshold_time
    global threshold_bytes
    global threshold_percentage
    global backup_output_dir
    global show_only
    global debugfile
    global reference_file_name_full

    starttime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    logname = openlog_local(starttime)

    log_string("Starting backup at %s" % (starttime))

    if len(parser_args) != 1:
        log_string("ERROR: invalid number of arguments (%d) - expect only "
                   "config file" % (len(parser_args)))
        parser.print_help()
        error_hit = True
    else:
        config_file = parser_args[0]

        if parser_options.output_dir != None:
            backup_output_dir = parser_options.output_dir

    if config_file and (not os.path.exists(config_file) or not
                        os.path.isfile(config_file)):
        log_string("ERROR: specified config file does not exist or is not a "
                   "file (%s)" % (config_file))
        error_hit = True
    show_only = parser_options.show_only

    if not error_hit:
        #
        # XXX check that path arguments are just single paths, and not multiple
        #     entries
        #
        opts = parse_config_file(config_file,
                                 {"tgt_dir": "/backup/",
                                  "rdiff_version": "rdiff-backup 1.1.5",
                                  "threshold_seconds": 300,
                                  "threshold_mbytes": 200,
                                  "threshold_percentage": 10,
                                  "report_incremental_stats": "False",
                                  "src_server": "",
                                  "tgt_server": "",
                                  "reference_file_name": "",
                                  "notify_cmd": "",
                                  "notify_server": ""},
                                 {"src_dir_single": [],
                                  "src_dir_list": [],
                                  "exclude_directory": [],
                                  "log_email_to": []})
        #
        # By default all options parameters are required - check that here
        # (with few exceptions for optional parameters)
        #
        optional_args = ["exclude_directory", "src_dir_single", "src_dir_list",
                         "notify_cmd", "notify_server"]
    
        for item in opts:
            if opts[item] == [] or opts[item] == None:
                if not item in optional_args:
                    log_string("ERROR: config file does not contain required value"
                               " for which there is no default. Item: %s" % (item))
                    error_hit = True
    
    if not error_hit:
        #
        # Get the bare minimum details to be able to send a useful mail - the to
        # address, and the backup_output_dir
        #
        logfile_email_to = " ".join(opts["log_email_to"])
    
        if not backup_output_dir:
            backup_output_dir = opts["tgt_dir"]
            if len(backup_output_dir.split()) != 1:
                log_string("ERROR: config file contains invalid tgt_dir option - "
                           "aborting.  Option: '%s'" % (opts["tgt_dir"]))
                error_hit = True
    
    if not error_hit:
        #
        # We can finally grab the backup lock - as if we fail now we can send a
        # useful email!
        #
        get_backup_lock()

        debugfile = open(os.path.join(log_output_dir,
                                      "%s" % (log_file_name)), "a")
        debug_string("*" * 60)
        debug_string("Starting backup at %s" % (starttime))
    
        #
        # Now form the arguments that we need to run the backup.
        #
        # First build up the list of backup sets.  Some are single directories
        # provided, some are directories which we want to list to get all the
        # backup sets (eg. for home directories).
        #
    
        #
        # Single directories should be specified without any trailing information
        #
        for parent_dir in opts["src_dir_single"]:
            if len(parent_dir.split()) != 1:
                log_string("ERROR: config file contains invalid src_dir_single "
                           "option - aborting.  Option: '%s'" %
                           (opts["src_dir_single"]))
                error_hit = True
            else:
                backup_sets.append(parent_dir)
    
    if not error_hit:
        #
        # Expect src dir list to fit the pattern below, where the parent dir to
        # list must be specified (and specified first), and the children to exclude
        # from this list are optional and prefixed with '-'.
        #
        # "/parent/ [-child_to_exclude1 ... -child_to_excludeN]"
        #
        for parent_dir in opts["src_dir_list"]:
            split_parent_all = parent_dir.split()
            split_parent = split_parent_all[0]
            split_parent_exclude = []
            if split_parent[0] == "-":
                error_hit = True
            elif len(split_parent_all) > 1:
                for child_exclude in split_parent_all[1:]:
                    if child_exclude[0] == "-":
                        split_parent_exclude.append(child_exclude[1:])
                    else:
                        error_hit = True
    
            if not error_hit:
                if os.path.exists(split_parent):
                    child_dirs = os.walk(split_parent).next()[1]
                    for child_dir in child_dirs:
                        if not child_dir in split_parent_exclude:
                            child_dir_abs = os.path.join(split_parent, child_dir)
                            if (os.path.isdir(child_dir_abs) and
                                not os.path.islink(child_dir_abs)):
                                backup_sets.append(child_dir_abs)
                else:
                    log_string("ERROR: specified output dir '%s' does not exist"
                               "or is a broken symlink" % (split_parent))
                    error_hit = True
        if error_hit:
            # report error here..
            log_string("ERROR: config file contains invalid src_dir_list options "
                       "- aborting.  Options: '%s', backup sets '%s'" %
                       (opts["src_dir_list"], backup_sets))
    
    if not error_hit:
        #
        # Have read all 'src_dir_list' and 'src_dir_single' we have, so make sure
        # we do actually have something to do now..
        #
        if len(backup_sets) == 0:
            log_string("ERROR: missing any backup sets.  Options: "
                       "src_dir_list '%s', src_dir_single '%s'" %
                       (opts["src_dir_list"], opts["src_dir_single"]))
            error_hit = True
    
    if not error_hit:
        #
        # Now setup the exclude paths based on the default and config, and form the
        # global rdiff options based on this and the static options.
        #
        if parser_options.age_period == None:
            perform_backup = True
    
            rdiff_options = default_rdiff_options_backup
            if opts["tgt_server"] == "":
                rdiff_options += default_rdiff_options_backup_local.format(
                                 users_dump_file_name, groups_dump_file_name)
            for option in default_exclude_directories:
                rdiff_options += exclude_directory_fmt % (option)
            for option in opts["exclude_directory"]:
                rdiff_options += exclude_directory_fmt % (option)
        else:
            perform_backup = False
            rdiff_options = default_rdiff_options_age % (parser_options.age_period)
    
        rdiff_backup_reference_version = opts["rdiff_version"]
        if opts["report_incremental_stats"] == "True":
            report_incremental_stats = True
        if opts["reference_file_name"] != "":
            if os.path.isabs(opts["reference_file_name"]):
                reference_file_name_full = opts["reference_file_name"]
            else:
                reference_file_name_full = os.path.join(log_output_dir,
                                                        opts["reference_file_name"])
    
        #
        # Leave local files as they are, but for remote files specify the user
        # (always enbackup), and the host name.
        #
        if opts["src_server"] != "":
            source_server = "%s@%s::" % (get_username(), opts["src_server"])
        if opts["tgt_server"] != "":
            target_server = "%s@%s::" % (get_username(), opts["tgt_server"])
        if (source_server != "") or (target_server != ""):
            #
            # Running remotely: the remote instance of rdiff-backup is
            # spawned by our remote wrapper -- specify the required
            # arguments here.
            #
            remote_cmd = "enbackup backup --remote " \
                                            "--tgt-dir %s " \
                                            "--src-server %s" % (
                                            backup_output_dir,
                                            socket.gethostname())
            rdiff_options += " --remote-schema '%s'" % (
                'su %s -c "ssh -C %s %s"' % (get_username(), "%s", remote_cmd))

        try:
            threshold_time       = int(opts["threshold_seconds"])
            threshold_bytes      = int(opts["threshold_mbytes"]) * 1024 * 1024
            threshold_percentage = int(opts["threshold_percentage"])
        except ValueError:
            log_string("ERROR: expected integer value in config file, but "
                       "conversion failed (file, value) = seconds (%s, %s), "
                       "mbytes (%s, %s), percentage (%s, %s)" %
                       (threshold_time,       opts["threshold_seconds"],
                        threshold_bytes,      opts["threshold_mbytes"],
                        threshold_percentage, opts["threshold_percentage"]))
            error_hit = True
    
    if not error_hit:
        if opts["src_server"] != "":
            if not remote_host_available(opts["src_server"]):
                log_string("ERROR: source host '%s' is not available - "
                           "abort backup" % (opts["src_server"]))
                error_hit = True
        if opts["tgt_server"] != "":
            if not remote_host_available(opts["tgt_server"]):
                log_string("ERROR: source host '%s' is not available - "
                           "abort backup" % (opts["tgt_server"]))
                error_hit = True
    
    if not error_hit:
        #
        # Create the files for rdiff-backup containing mapping from every user and
        # group to "root".  See comments for XXX for details.
        #
        outf = open(users_dump_file_name, "w")
        for id in pwd.getpwall():
            outf.write("%s:root\n" % (id[0]))
        outf.close()
    
        outf = open(groups_dump_file_name, "w")
        for id in grp.getgrall():
            outf.write("%s:root\n" % (id[0]))
        outf.close()
    
    if not error_hit:
        #
        # Create the output directory if it doesn't exist.  We can't do much if the
        # target is remote - user will just have to make sure the directory exists!
        #
        if (not os.path.exists(backup_output_dir)) and (target_server == ""):
            os.makedirs(backup_output_dir)
        if perform_backup:
            log_string("Performing backup for %s to '%s'" %
                       (local_server_name, backup_output_dir))
        else:
            log_string("Performing ageing of backup for %s to '%s' - "
                       "removing increments older than %s" %
                       (local_server_name, backup_output_dir,
                        parser_options.age_period))
    
    
        #
        # Also check that the files we create in the output directory have write
        # permissions (which they might not, eg. over nfs).  Only testable for
        # local target directory - let rdiff-backup cope with remote case.
        #
        if target_server == "":
            tmp_filename = os.path.join(backup_output_dir, ".enbackup.tmp")
            open(tmp_filename, "w").write("temporary file - delete me\n")
            if os.stat(tmp_filename).st_uid != 0:
                print("ERROR: enbackup.py must be able to write output files as "
                      "root - aborting.  (EUID %s)" %
                      (os.stat(tmp_filename).st_uid))
                sys.exit(-1)
    
            if os.path.exists(tmp_filename):
                os.remove(tmp_filename)
    
        try:
            if perform_backup:
                backup_main(rdiff_options, report_incremental_stats, starttime)
            else:
                age_main(rdiff_options)
        except:
            #
            # Something bad happened.  Grab a stacktrace and write it to the log
            #
            log_string("*" * 60)
            log_string("Exception hit during backup operation")
            log_string("*" * 60)
    
            #
            # Urgh.  Need this until we're guaranteed to be running on
            # >= python 2.4 (and so format_exc supported)
            #
            raise
            log_string(traceback.format_exc())
    
    release_backup_lock()

    #
    # Successful completion, notify the next command (if requested)
    #
    if not error_hit:
        notify_cmd    = opts["notify_cmd"]
        notify_server = opts["notify_server"]
        if notify_cmd != None and notify_cmd != "":
            if notify_server != None and notify_server != "":
                enbackup.cmd.notify.notify_remote(notify_server, notify_cmd.split())
            else:
                enbackup.cmd.notify.notify_local(notify_cmd.split())
    
    logfile.close()
    
    #
    # Now send the log as an email.
    #
    if logfile_email_to != None:
        if not backup_output_dir:
            backup_output_dir = "{unknown backup}"
        if perform_backup:
            subject_str = ""
        else:
            subject_str = "ageing "
        retval = os.popen('mail -s "Backup %slog for %s (%s, %s)" %s < %s' %
                          (subject_str, local_server_name, backup_output_dir,
                          starttime, logfile_email_to, logname)).close()
        if retval:
            debug_string("Failed to email results to (%s): %s" %
                         (logfile_email_to, retval))
        else:
            debug_string("Mail successfully sent for %s to (%s)" %
                         (local_server_name, logfile_email_to))


def main(arguments):
    #
    # We want to acquire the backup lock as early as possible - but parsing
    # arguments and reading the config allow us to get the email address to send to
    # - quite useful!  Get that far before grabbing the lock..
    #
    # NOTE: We can write to the log file, but it is not safe to write to the debug
    # file until we are sure there are no other processes (ie. we have the lock).
    #
    parser = optparse.OptionParser()
    parser.add_option("-o", "--output_directory", dest="output_dir",
                      help="Optional output directory, which overrides "
                           "the directory set in the config file.  Useful "
                           "for writing a separate backup set at a different "
                           "frequency.")
    parser.add_option("-a", "--age", dest="age_period",
                      help="Do not perform a backup, instead just age the "
                           "backup directory using the specified period "
                           "(eg. 1W or 1D to remove all backups older than "
                           "1 week or 1 day)")
    parser.add_option("-s", "--show_only", dest="show_only",
                      action="store_true", default=False,
                      help="Do not actually run rdiff-backup, just indicate "
                           "what would be done")
    parser.add_option("--remote", dest="is_remote",
                      action="store_true", default=False,
                      help="Request remote operation.  This option is not "
                      "expected to be specified manually, but is used "
                      "internally by when backing up to a remote server")
    parser.add_option("--tgt-dir", dest="tgt_dir",
                      help="Specify the target directory for remote "
                      "operation.  This option is not "
                      "expected to be specified manually, but is used "
                      "internally by when backing up to a remote server")
    parser.add_option("--src-server", dest="src_server",
                      help="Specify the originating server for a remote "
                      "operation.  This option is not "
                      "expected to be specified manually, but is used "
                      "internally by when backing up to a remote server")
    (parser_options, parser_args) = parser.parse_args(arguments)

    #
    # First of all, work out whether this is a local or a remote backup,
    # and invoke the correct worker function.  Additional checking of
    # arguments is done by those functions, since they know which arguments
    # are required in each case:
    #
    if parser_options.is_remote:
        do_remote(parser_options)
    else:
        do_local(parser, parser_options, parser_args)


if __name__ == "__main__":
    main(sys.argv[1:])


###############################################################
# END: Code in which exceptions are intentionally not caught  #
#                                                             #
#      Nothing should come after here :)                      #
###############################################################

