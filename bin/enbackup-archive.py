#!/usr/bin/python
###############################################################################
# enbackup-archive.py - worker script to store backups on external disks
#
# August 2008, Anthony Toole
#
# Copyright (c) 2008-2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - all working, does all it should apart from postponing final
#               "cp" until off peak hours.
# Version 1.1 - now copy is postponed until 18:00
# Version 1.2 - only open log in child process!
#             - check we are root (only important when run manually)
#             - don't touch the log file until we have the lock (to avoid
#               scribbling over it if duplicate processes were started!)
#             - send a mail from parent process, just saying to expect a mail
#               from child in the next day (ie. just confirm that udev started 
#               us correctly)
# Version 1.3 - will now postpone final cp until between set of times
#               (so if run late in the day it should still complete the same
#               day)
#             - allows deleting of quarterly backups as long as a certain
#               number of backups still remain (this avoids the most frequent
#               need for user intervention)
# Version 1.4 - fix bug where lock file is released even if we didn't get it
# Version 1.5 - build on 1.3 fix to allow deletion of weekly/monthly backups as
#               long as we have the minimum number of remaining sets.
# Version 1.6 - report the time taken by the copy - and calculate copy rate.
# Version 1.7 - everything is postponed to 21:00
#             - reduce the number of backup sets kept due to increasing backup
#               size.
# Version 1.8 - Parallelized the 'rm' and 'du' to achieve a faster run time.
# Version 1.9 - Reduced 'rm' and 'du' parallelization to reduce disk load
#               (a candidate trigger for triggering kernel crashes).
#             - Added default config file support.
#             - Misc fixes to log messages to tidy up unused arguments.
# Version 1.10 - Migrate to lockfile-based locking 
#              - Use enbackup Logger support for debug/error messages
#              - Use 'udevadm info' for more robust detection of USB vs. eSATA
#              - Add handling to suspend USB, as well as eSATA, devices,
#                before unplugging them.
###############################################################################
#
# Overview: this is the script that the udev infra will call when a disk
# insertion has occured.  We register for these events using the config file
# eg. /etc/udev/rules.d/10-enbackups.rules.
#
# In broad strokes, this script will be run when a backup disk is inserted, the
# disk will be mounted, backup sets will be copied to the external disk
# (deleting older backups as required), before finally unmounting the disk and
# sending an email to the administrator.
#
# As this has been triggered off a udev event and might take a considerable
# amount of time to run, we should fork immediately to return control to udev.
#
#
# Work To do:
#
# - would be nice if the output of enbackup-rdiff-to-mirror could be displayed,
#   especially useful if it could be shown as it progresses.  Probably not too
# hard to do - just need to read lines rather than wait for all input.
#
# - should we include today's date in list passed to "filter_dates"?  Probably
#   as this counts towards our allowance, though we need to make sure that this
#   is given preference over any backup in the last week (especially as it
#   doesn't exist, so any attempt to delete it will fail!).
#

import os
import sys
import re
import time
import datetime
import subprocess
import traceback
import shutil
import ConfigParser
from enbackup.utils import DirLock, GlobalLock
from enbackup.utils import makedirs
import enbackup.log

#
# Directory where backup scripts are located.
#
script_dir = "/usr/bin/"

#
# Directory to which backup logs are written.
#
log_output_dir = "/var/log/enbackup/"
log_file_name = os.path.join(log_output_dir, "enbackup-archive.log")

operation_success = False
logfile_email_to = None
logfile_email_to_default = "root@localhost"

#
# Time to wait until to start the archiving at.  By default, don't wait
# at all (i.e. start any time from 00:00 until the end of the day).
#
start_at_default = "00:00"
start_at_fmt     = "%H:%M"

#
# Definitions of where backups are stored or need copying to.
#
mount_dir = None
mount_dir_default = "/media/usbdisk"
target_dir = None
target_dir_default = "backups"
mirror_dir = "mirror-only"

source_dir = None
source_dir_default = "/enbackup/"

#
# Config file location
#
config_dir = "/etc/enbackup.d/"

#
# Minimum number of "keep" backup sets we need, before we delete
# "keep_optional" ones
#
minimum_backup_sets_required = 1


#
# Logging stuff - debug just writes to file, error raises exception (which we
# catch and display nicely before cleaning up).
#
logger = enbackup.log.Logger("archive")

def write_to_debugfile(str):
    if not debugfile.closed:
        debugfile.write("%s\n" % (str))
        debugfile.flush()


def debug(str):
    logger.debug(str)
    write_to_debugfile(str)


def error(str):
    logger.error(str)
    write_to_debugfile(str)
    raise Exception(str)


#
# Locks to protect directories we're reading from, as well as a whole
# server-level lock to stop multiple instance of this or other enbackup
# scripts interfering with each other.
#
dir_locks = None
server_lock = GlobalLock("Process {0}, PID {1}".format(sys.argv[0],
                                                       os.getpid()))


#
# Acquire the lock to prevent any other rdiff process writing to the output
# directory.  We are very heavy handed with the locking to avoid anyone
# managing to somehow screw things up accidentally - one lock per machine.
#
# Note: if this raises exception this will be caught!
#
def get_backup_locks():
    global dir_locks
    global server_lock

    debug("About to acquire global lock")

    #
    # First, grab the global lock.  This stops us interfering with
    # other enbackup scripts (and stops other enbackup scripts
    # interfering with us).
    #
    server_lock.lock()

    #
    # Also grab a lock for all the directories we're about
    # to read from, to make sure no other scripts are
    # going to be writing to them.  We find the set of directories
    # by listing the contents of the parent dir.  This is a bit
    # hacky -- it would be nice to do this based on some notion of
    # configured backup sets instead -- but is sufficient for now.
    #
    # Hack: actually, the above logic isn't quite enough because
    # our config has multiple levels of directories for aged vs.
    # unaged data.  Grab locks for each of those subdirectories
    # too: more than we need, but this should be safe.
    #
    dir_contents = [os.path.join(source_dir, x)
                    for x in os.listdir(source_dir)]
    lock_dirs = [x for x in dir_contents if os.path.isdir(x)]
    lock_dirs_aged = [ os.path.join(x, "aged") for x in lock_dirs ]
    lock_dirs_unaged = [ os.path.join(x, "unaged") for x in lock_dirs ]

    lock_dirs = lock_dirs + lock_dirs_aged + lock_dirs_unaged
    
    debug("About to lock directories {0}".format(lock_dirs))

    lock_details = "Time %s, PID %s\n" % (time.ctime(), os.getpid())
    dir_locks = [DirLock(lock_details, x) for x in lock_dirs]
    
    for lock in dir_locks:
        lock.lock()

    debug("Acquired all locks")

    return


#
# Release the backup locks - obviously only if this process acquired it.
#
def release_backup_locks():
    global dir_locks
    global server_lock
    unlock_count = 0

    for lock in dir_locks:
        if lock.locked:
            lock.unlock()
            unlock_count = unlock_count + 1

    debug("Released {0} directory locks".format(unlock_count))

    if server_lock.locked:
        server_lock.unlock()
        debug("Lock file released")
    else:
        debug("Lock file not aquired - do not release")


def sleep_until_between_times(from_time, to_time):
    #
    # Want to sleep until some time between from_time and to_time - ie. if
    # before from_time we sleep until then, if between the two we just go
    # for it, if after to_time we sleep until from_time again.
    #
    # Calculate how long frow now until the time specified - and then sleep
    # for that long
    #
    # Obviously just overwriting the time we want in the current time won't
    # guarantee 'then' is in the future - so cope with wrapping.
    #
    from_hour = from_time[0]
    from_min = from_time[1]
    to_hour = to_time[0]
    to_min = to_time[1]
    now = datetime.datetime.today()
    debug("Sleeping until between %2.2d:%2.2d and %2.2d:%2.2d "
          "(now %2.2d:%2.2d:%2.2d)" %
          (from_hour, from_min, to_hour, to_min,
           now.hour, now.minute, now.second))
    then_start = now.replace(hour=from_hour, minute=from_min, second=0)
    then_end = now.replace(hour=to_hour, minute=to_min, second=0)
    diff = then_start - now
    if (now > then_start) and (now < then_end):
        debug("Current time betwen start and end - run now")
    else:
        if diff.days == -1:
            debug("End time specified is in the past - set for tomorrow")
            diff += datetime.timedelta(days=1)
        if diff.days != 0:
            #
            # The diff (day, second) should only ever have a "second"
            # element, as obviously it won't be more than a day in advance.
            #
            error("ERROR: time diff is unexpected: %s, %s, %s" %
                  (diff.days, diff.seconds, diff))
        debug("Sleeping for %u seconds" % (diff.seconds))
        time.sleep(diff.seconds)


def month_to_quarter(month):
    month = int(month)
    quarter = (month + 2) / 3
    return quarter


def date_to_ages(date, basedate, max_dates):
    #
    # Convert the date we have to a date, using the basedate as a refence.
    # In normal operation basedate is today - but want this controllable
    # for testing purposes.
    #
    # "Age" here not just simply calculated from the number of days between
    # two dates, but instead from the week/month/quarter of the year.
    #
    # The age in months can be categorised simply by inspection of the month
    # of the year, the age in quarters can similarly be done if we divide the
    # year up (0-2, 3-6, 7-9, 10-12 months).  The age in weeks cannot be done
    # by inspection of the date alone - but conversion to iso format lets us
    # calculate the difference between the ISO week of the two dates (this
    # ensures that, for example, if a backup was run on Thursday one week and
    # Tuesday the next the difference is a week, even though it is less than
    # this).
    #
    if basedate.toordinal() < date.toordinal():
        raise "Invalid basedate - must be more recent than all backup dates"
    age_days = basedate.toordinal() - date.toordinal()
    if age_days > max_dates["age_days"]:
        max_dates["age_days"] = age_days
    age_weeks = basedate.isocalendar()[1] - date.isocalendar()[1]
    age_weeks += 52 * (basedate.isocalendar()[0] - date.isocalendar()[0])
    if age_weeks > max_dates["age_weeks"]:
        max_dates["age_weeks"] = age_weeks
    age_months = basedate.month - date.month
    age_months += 12 * (basedate.year - date.year)
    if age_months > max_dates["age_months"]:
        max_dates["age_months"] = age_months
    age_quarters = (month_to_quarter(basedate.month) -
                    month_to_quarter(date.month))
    age_quarters += 4 * (basedate.year - date.year)
    if age_quarters > max_dates["age_quarters"]:
        max_dates["age_quarters"] = age_quarters
    return {"age_days": age_days,
            "age_weeks": age_weeks,
            "age_months": age_months,
            "age_quarters": age_quarters}


def parse_iso_date(datestr):
    #
    # Take string in ISO format (YYYY-MM-DD) and return [year, month, day]
    # tuple.
    #
    res = re.match("([0-9]{4})-([0-9]{2})-([0-9]{2})", datestr)
    year = int(res.groups()[0])
    month = int(res.groups()[1])
    day = int(res.groups()[2])
    return([year, month, day])


def is_iso_date(datestr):
    #
    # Return TRUE/FALSE if is/isn't ISO date string
    #
    is_date = True
    try:
        parse_iso_date(datestr)
    except AttributeError:
        is_date = False
    return is_date


def filter_dates(dates):
    #
    # Instead of having a rolling calendar, lets make things simple.
    #
    # Keep: - 1 a week for last 'weekly_history' weeks - 1 a month for last
    # 'monthly_history' months (where month boundaries are as on the calendar,
    # not rolling) - 1 a quarter beyond that (again, with static quarter
    # boundaries, ie. 0-3, 4-6, 7-9, and 10-12 months).
    #
    weekly_history = 4
    monthly_history = 3 # ie. a quarter!
    keep = []
    keep_optional = []

    #
    # Sort the list - newest first
    #
    dates.sort()
    dates.reverse()
    debug("Incoming (sorted) dates: %s" % (dates))
    basedate = datetime.date.today()

    #
    # Sort the dates into buckets based on the week, month and year they fall
    # in to.
    #
    # Dictionary version of data array..
    #
    dates_d = {}
    max_dates = {"age_days": 0, "age_weeks": 0,
                 "age_months": 0, "age_quarters": 0}
    for datestr in dates:
        year, month, day = parse_iso_date(datestr)
        date = datetime.date(year, month, day)
        ages = date_to_ages(date, basedate, max_dates)
        debug("Date %s is age %s days, %s weeks, %s months, %s quarters" %
              (datestr, ages["age_days"], ages["age_weeks"],
               ages["age_months"], ages["age_quarters"]))
        dates_d[datestr] = ages

    #
    # We now know the age in each of the various measures.  Need 'buckets' for
    # every week in that range, every month in that range, and every quarter
    # in that range
    #
    by_week = []
    by_month = []
    by_quarter = []
    for i in range(0, max_dates["age_weeks"] + 1):
        by_week.append([])
    for i in range(0, max_dates["age_months"] + 1):
        by_month.append([])
    for i in range(0, max_dates["age_quarters"] + 1):
        by_quarter.append([])

    #
    # For each category we want the buckets to be such that the zeroth element
    # is the newest, and within each bucket we want the items to be sortest
    # oldest first.  This means we typically keep the first item from the top
    # N buckets.
    #
    # Using age to calculate the bucket, and then inserting items (with dates
    # already sorted oldest first) does this for us.
    #
    for date in dates:
        by_week[dates_d[date]["age_weeks"]].insert(0, date)
        by_month[dates_d[date]["age_months"]].insert(0, date)
        by_quarter[dates_d[date]["age_quarters"]].insert(0, date)

    #
    # Walk through the weekly buckets, decide what to keep..
    #
    found = 0
    for i in range(0, len(by_week)):
        if len(by_week[i]) == 0:
            debug("Week %s: skipping empty bucket" % (i))
        elif found < weekly_history:
            debug("Week %s: keeping %s, discarding %s" %
                  (i, by_week[i][0], by_week[i][1:]))
            keep.append(by_week[i][0])
            found += 1
        else:
            debug("Week %s: discarding all: %s" % (i, by_week[i]))
    delete = [x for x in dates if x not in keep]
    debug(" Keep: %s" % (keep))
    debug(" Delete: %s" % (delete))

    #
    # We now have our weekly backups.  When calculating the monthly backups
    # we want to include anything we already have marked for keeping..
    # 
    found = 0
    for i in range(0, len(by_month)):
        if len(by_month[i]) == 0:
            debug("Month %s: skipping empty bucket" % (i))
        elif found < monthly_history:
            #
            # First check if we've already got an item from this monthly
            # bucket. If so, we can discard all, if not we keep the
            # first (oldest).
            #
            found += 1
            overlap = [x for x in keep if x in by_month[i]]
            if overlap:
                debug("Month %s: skipping - some dates already marked to "
                      "keep: %s" % (i, ", ".join(overlap)))
            else:
                debug("Month %s: keeping %s, discarding %s" %
                      (i, by_month[i][0], by_month[i][1:]))
                keep.append(by_month[i][0])
        else:
            debug("Month %s: discarding all: %s" % (i, by_month[i]))
    delete = [x for x in dates if x not in keep]
    debug(" Keep: %s" % (keep))
    debug(" Delete: %s" % (delete))

    #
    # We now have our monthly backups too.  When calculating the quarterly
    # backups we want to include anything we already have marked for keeping.
    #
    for i in range(0, len(by_quarter)):
        if len(by_quarter[i]) == 0:
            debug("Quarter %s: skipping empty bucket" % (i))
        else:
            #
            # First check if we've already got an item from this quarterly
            # bucket.  If so, we can discard all, if not we keep the
            # first (oldest).
            #
            overlap = [x for x in keep if x in by_quarter[i]]
            if overlap:
                debug("Quarter %s: skipping - some dates already marked to "
                      "keep: %s" % (i, ", ".join(overlap)))
            else:
                debug("Quarter %s: keeping %s, discarding %s" %
                      (i, by_quarter[i][0], by_quarter[i][1:]))
                keep_optional.append(by_quarter[i][0])
    delete = [x for x in dates if x not in (keep + keep_optional)]
    debug(" RESULT: Keep: %s" % (keep))
    debug(" RESULT: Keep optional: %s" % (keep_optional))
    debug(" RESULT: Delete: %s" % (delete))
    debug("")
    return{"keep": keep, "keep_optional": keep_optional, "delete": delete}


def normalise_directory (dir):
    #
    # Normalise a directory by ensuring it has a trailing '/'.
    #
    if dir[-1] == '/':
        return dir
    else:
        return dir + '/'


def run_cmd(cmd):
    #
    # Spawn a subprocess (or not if we are in show-only mode), returning
    # [retcode, stderr, stdout]
    #
    debug("Running '%s'" % (" ".join(cmd)))
    subproc = subprocess.Popen(cmd,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = subproc.communicate()
    return([subproc.returncode, stderr, stdout])


def pipe(cmd1, cmd2):
    #
    # Pipe the output from cmd1 into cmd2 and return the output. We buffer the
    # output of the first process to allow it to complete before the second
    # process has necessarily dealt with all the output.
    #
    p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, bufsize=-1)
    p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    stdout, stderr = p2.communicate()
    if p2.returncode != 0:
        error("ERROR: failed to run the command '%s | %s': %s\n%s\n" %
              ((" ".join(cmd1)), (" ".join(cmd2)), p2.returncode, stderr))

    return p2.returncode, stdout


def calculate_total_size(cmd):
    #
    # Given a 'find' command, calculate the total size of all files found
    # by that command.
    #
    cmd.append("-print0")
    du_cmd = "xargs -0P2 -n1 du -s".split()
    rc, output = pipe(cmd, du_cmd)

    #
    # Split the output of 'du' on newlines. We don't check the last element
    # as that is a blank line.
    #
    total_size = 0
    if rc == 0:
        a = output.split("\n")
        for line in a[0:-1]:
            size = re.match('[0-9]+', line)
            total_size += int(size.group())

    return rc, total_size


def calculate_directory_size(dir):
    #
    # The directory size is calculated by:
    #  a) summing the size of directories 4 deep,
    #  b) summing the size of all files upto 4 deep,
    #  c) counting the directories upto 3 deep and allowing 4 bytes for each
    #     directory,
    #  d) adding all the above.
    #
    # This is not necessarily accurate as it assumes that the default block
    # size is 4k and that all directories are 4k.
    #
    dir = normalise_directory(dir)
    path_regexp = "%s*/*/*/*" % dir
    cmd = ["find", dir, "-maxdepth", "4", "-type", "d", "-path", path_regexp]
    rc, size1 = calculate_total_size(cmd)

    if rc == 0:
        cmd = ["find", dir, "-maxdepth", "4", "-type", "f"]
        rc, size2 = calculate_total_size(cmd)

    if rc == 0:
        path_regexp = "%s*" % dir
        cmd = ["find", dir, "-maxdepth", "3", "-type", "d",
               "-path", path_regexp]
        rc, err, out = run_cmd(cmd)
        size3 = 4 * (len(out.split("\n")) - 1)
    
    #
    # Return the size in Mb and round up.
    #
    if rc == 0:
        size = (size1 + size2 + size3 + 1023) / 1024
    else:
        size = 0

    return rc, size
    

def check_sufficient_free_space(dir, size_needed):
    rc, err, out = run_cmd(["df", "-m", "-P", dir])
    if rc != 0:
        error("ERROR: failed to determine available free space: %s\n%s\n" %
              (rc, err))
    free_space = int(out.split("\n")[1].split()[3])
    debug("Free space available on disk: %u Mb (%u Mb needed)" %
          (free_space, size_needed))
    return (free_space >= size_needed)


def remove_directory(dir):
    #
    # Removes a directory by spawning multiple copies of 'rm'.
    #
    dir = normalise_directory(dir)
    debug("Removing directory '%s'" % dir)
    path_regexp = "%s*/*/*/*" % dir
    find_cmd = ["find", dir, "-maxdepth", "4", "-type", "d",
                "-path", path_regexp, "-print0"]
    rm_cmd = "xargs -0P2 -n1 rm -rf".split()
    rc, stdout = pipe(find_cmd, rm_cmd)
    shutil.rmtree(dir)
    return rc


def device_is_usb(devname):
    #
    # Check whether the device is USB by using 'udevadm info' to check which
    # bus it's attached to.
    #
    is_usb = False

    rc, err, out = run_cmd(["/sbin/udevadm",
                            "info",
                            "-a",
                            "-n", "/dev/{0}".format(devname)])

    if rc != 0:
        error("ERROR: unable to check type of device {0}: {1}\n{2}\n".
            format(devname, rc, err))
    else:
        for line in out.rsplit("\n"):
            if re.match(r".*looking at device .*/usb[0-9]/.*", line):
                is_usb = True

    return is_usb


def main():
    #
    # This script can hammer our servers at a time when people notice.
    # Be nice. By default this process runs at a negative nice value,
    # presumably because udev starts us.
    #
    debug("Reniced to %u" % (os.nice(40)))

    #
    # First we need to know the device that has been inserted - this is
    # passed in env variable
    #
    if not os.environ.has_key("DEVPATH"):
        debug(" argv: %s" % (sys.argv))
        debug(" environ:")
        for item in os.environ:
            debug(" %s: %s" % (item, os.environ[item]))
        error("No 'DEVPATH' item - cannot mount harddisk")
    dev = os.environ["DEVPATH"]
    dev = dev.split("/")[-1]
    debug("Got device %s" % (dev))

    #
    # Would ideally identify the eSATA explicitly, but without plugging it in
    # I can't easily do that - so for now infer that were aren't eSATA if we
    # are USB :)
    #
    is_usb = device_is_usb(dev)
    if not is_usb:
        debug("Device is eSATA")
        esata_disk = True
    else:
        debug("Device is USB")
        esata_disk = False

    #
    # Need some locking here - ideally working with main backup script locking
    # to stop them interfering with each other.
    #  
    get_backup_locks()

    #
    # Next check that no other disk is already mounted, and mount this
    # disk if not
    #
    makedirs(mount_dir, logger, False)

    if os.path.ismount(mount_dir):
        error("Unexpected - disk is already mounted.  "
              "Give up to avoid clashing")
    rc, err, out = run_cmd(["/bin/mount",
                            "-text3",
                            "/dev/%s" % dev,
                            mount_dir])
    if rc != 0:
        error("ERROR: failed to mount device %s: %d\n%s\n" % (dev, rc, err))

    #
    # Need to know the size of the backup directory
    #
    start_time = time.time()
    rc, backup_size = calculate_directory_size(source_dir)
    elapsed_time = time.time() + 1 - start_time
    elapsed_time_str = str(datetime.timedelta(0, elapsed_time, 0)).split(".")[0]

    if rc != 0:
        error("ERROR: failed to get backup directory size %s: %d\n%s\n" %
              (source_dir, rc, err))

    debug("'du' completed in %s, backup directory size is %u Mb" %
          (elapsed_time_str, backup_size))

    #
    # We only keep a single mirror+increment - so move the old one to
    # mirror-only directory, and delete the increment part.
    #
    # Obviously exclude anything that isn't a backup (ie. doesn't fit
    # the YYYY-MM-DD format).
    #
    debug("Looking for mirror/increments to make into mirror-only")
    start_time = time.time()
    rdiff_to_mirror_cmd = os.path.join(script_dir,
                                       "enbackup-rdiff-to-mirror.py")

    backup_dir_full = os.path.join(mount_dir, target_dir)
    mirror_dir_full = os.path.join(backup_dir_full, mirror_dir)

    makedirs(backup_dir_full, logger, False)
    makedirs(mirror_dir_full, logger, False)

    to_move = os.listdir(backup_dir_full)
    for backup in to_move:
        if is_iso_date(backup):
            backup_old = os.path.join(backup_dir_full, backup)
            backup_new = os.path.join(mirror_dir_full, backup)
            rc, err, out = run_cmd([rdiff_to_mirror_cmd, backup_old])
            if rc != 0:
                error("ERROR: failed to shrink old backup %s: %d\n%s\n" %
                      (backup_old, rc, err))
            os.rename(backup_old, backup_new)
    elapsed_time = time.time() + 1 - start_time
    elapsed_time_str = str(datetime.timedelta(0, elapsed_time, 0)).split(".")[0]
    debug("Mirror/increment to mirror-only conversion completed in %s" % elapsed_time_str)

    #
    # Work out which backup sets we should keep and delete.
    #
    res = filter_dates(os.listdir(mirror_dir_full))
    
    #
    # Delete all backups marked for deletion, keep those for optional
    # deletion (ie. quarterlies).
    #
    debug("Deleting old backups")
    start_time = time.time()
    for directory in res["delete"]:
        full_directory = os.path.join(mirror_dir_full, directory)
        debug("Deleting old backup directory: %s" % (full_directory))
        rc = remove_directory(full_directory)
        if rc != 0:
            error("ERROR: failed to delete %s as part of the old backups" %
                  full_directory)

    #
    # See if we have enough free space without deleting quarterlies, if not
    # enough then delete quarterlies as long as we have a minimum number of
    # non-quarterly backup sets..  If even that doesn't give us enough space
    # then delete the older "keep" sets, as long as we keep a minimum of
    # three.
    #
    if check_sufficient_free_space(backup_dir_full, backup_size):
        debug("Got enough free space without deleting quarterlies")
    elif len(res["keep"]) < minimum_backup_sets_required:
        error("Insufficient disk space, and not enough 'keep' directories to "
              "zap quarterlies (have %s, need %s)" %
              (len(res["keep"]), minimum_backup_sets_required))
    else:
        possible_deletes = (res["keep"][minimum_backup_sets_required:] +
                            res["keep_optional"])
        while True:
            if len(possible_deletes) == 0:
                error("Not enough space for backup, and no (more) backups to "
                      "delete (needed %s Mb)" % (backup_size))
            directory = possible_deletes.pop()
            if directory in res["keep_optional"]:
                dir_type = "optional"
            else:
                dir_type = "weekly/monthly"
            full_directory = os.path.join(mirror_dir_full, directory)
            debug("Deleting old backup directory (%s): %s" %
                  (dir_type, full_directory))
            rc = remove_directory(full_directory)
            if rc != 0:
                error("ERROR: failed to delete %s as part of the old backups" %
                      full_directory)
            if check_sufficient_free_space(backup_dir_full, backup_size):
                debug("Deleting backup sets has created enough free space")
                break

    elapsed_time = time.time() + 1 - start_time
    elapsed_time_str = str(datetime.timedelta(0, elapsed_time, 0)).split(".")[0]
    debug("Deleting old backups completed in %s" % elapsed_time_str)

    #
    # OK - we have the space we need, so copy over the current
    # mirror+increment
    #
    output_dir = os.path.join(backup_dir_full,
                              datetime.date.today().isoformat())
    debug("Copying current backup mirror/increment %s to %s" %
          (source_dir, output_dir))
    start_time = time.time()
    rc, err, out = run_cmd(["cp", "-a", source_dir, output_dir])

    # 
    # Add one to elapsed time to avoid any change of divide by zero and
    # discard the sub-second part of the elapsed time
    # (eg. "0:00:08.839987" -> 0:00:08)
    #
    elapsed_time = time.time() + 1 - start_time
    elapsed_time_str = str(datetime.timedelta(0, elapsed_time, 0)).split(".")[0]
    if rc != 0:
        error("ERROR: failed to copy %s to %s (elapsed time %s): %d\n%s\n%s\n" %
              (source_dir, output_dir, elapsed_time_str, rc, out, err))
    else:
        debug("Copy completed: %u Mb, time taken %s, rate %u Mb/s" %
              (backup_size, elapsed_time_str,
               round(float(backup_size) / elapsed_time)))

    #
    # Unmount the disk
    #
    rc, err, out = run_cmd(["umount", mount_dir])
    if rc != 0:
        error("ERROR: failed to unmount disk: %d\n%s\n" % (rc, err))

    #
    # Remove the device using the appropriate helper script, depending on
    # whether it's USB or eSATA:
    #
    if esata_disk:
        remove_cmd = os.path.join(script_dir, "enbackup-sata-remove.sh")
    else:
        remove_cmd = os.path.join(script_dir, "enbackup-suspend-usb-device.sh")

    m = re.match(r"(sd[a-z])[0-9]", dev)
    if m == None:
        error("ERROR: failed to extract base device name from {0}".
              format(dev))

    basedev = m.group(1)
    remove_dev = "/dev/{0}".format(basedev)

    rc, err, out = run_cmd([remove_cmd, remove_dev])

    if rc != 0:
        error("ERROR: failed to remove device {0}: {1}\n{2}\n".format(
              remove_dev, rc, err))

    debug("Successfully completed!!")
    operation_success = True


if __name__ == "__main__":
    #
    # Check we are root - if not barf.  Should only hit this if being run
    # manually, as udev will always be run as root.
    #
    if (os.geteuid() != 0):
        print("ERROR: enbackup-archive.py must be run as root - aborting.  "
              "(EUID %s)" % (os.geteuid()))
        sys.exit(-1)

    #
    # Read the configuration from the file (or fall back to defaults if the
    # file does not exist, or the option isn't specified).
    #
    config = ConfigParser.ConfigParser(
                { "log_email": logfile_email_to_default,
                  "src_dir": source_dir_default,
                  "tgt_dir": target_dir_default,
                  "mount_point": mount_dir_default, })
    config.read(os.path.join(config_dir, "enbackup-archive.rc"))
    logfile_email_to = config.get("Logging", "log_email")
    source_dir = config.get("Paths", "src_dir")
    target_dir = config.get("Paths", "tgt_dir")
    mount_dir  = config.get("Paths", "mount_point")

    try:
        start_at_string = config.get("Options", "start_at")
    except ConfigParser.NoSectionError, ConfigParser.NoOptionError:
        #
        # Fall back to using the default start time.  Can't debug yet
        # because we don't initialize debug until after we've read options
        # and forked.
        #
        start_at_string = start_at_default

    try:
        start_at_ts = datetime.datetime.strptime(start_at_string,
                                                 start_at_fmt)
        start_at = [start_at_ts.hour, start_at_ts.minute]
    except:
        print("ERROR: start time is not in expected format, "
              "expect {1}, given {0} - aborting".format(start_at_fmt,
                                                        start_at_string))
        sys.exit(-1)

    #
    # This script takes far to long to run for the caller to be blocked,
    # so spawn a separate child process to do the work, with the parent
    # process just exiting leaving the udev script free to carry on.
    #
    # Fork call returns zero to the child process, and child's PID returned
    # to parent process
    #
    retval = os.fork()

    if retval == 0:
        #
        # Only open the log file in the child process!
        #
        debugfile = open(log_file_name, "w")

        try:
            #
            # We postpone all processing to save thrashing the disk with
            # "du" and "cp".
            #
            sleep_until_between_times(start_at, [23,59])

            main()
        except:
            #
            # Want to catch exceptions and print outputs, but not cause it to
            # stop cleanup!
            #
            debug("Exception caught - cleaning up")
            debug("".join(traceback.format_exc()))
        else:
            operation_success = True

        release_backup_locks()
        debugfile.close()

        #
        # Mail the logfile to the specified email address - appending
        # something more descriptive than just the raw log!
        #
        if logfile_email_to != None:
            if operation_success:
                prefix = "SUCCESS"
                body = "Good news!  Copying of backup to external disk has "\
                        "completed.\n\n"
                body += "Please disconnect the external disk, and if "\
                        "necessary connect the next disk.\n\n"
                body += "Full log is below - this is for information only\n\n"
                body += "\n\n\n\n\n\n\n\n"
                body += "%s\n" % ("#" * 79)
            else:
                prefix = "FAILURE"
                body = "INVESTIGATION REQUIRED!\n"
                body += "An error occured when copying backups to "\
                        "external disk.\n\n"
                body += "The full log is below, please investigate and "\
                        "fix this.\n\n"
                body += "Before re-running the backup archiving you will "\
                        "need to unmount the disk.\n\n"
                body += "\n\n%s\n" % ("#" * 79)

            subject = "{0}: Backup archiving to external disk complete".format(
                      prefix)
            tmpfile = open("/tmp/enbackup-archive.child", "w")
            tmpfile.write(body)
            for line in open(log_file_name).readlines():
                tmpfile.write(line)
            tmpfile.close()
        
            retval = os.popen('mail -s "%s" %s < %s' %
                              (subject, logfile_email_to, tmpfile.name)).close()
            if retval:
                print("Failed to email results to (%s): %d\n" %
                         (logfile_email_to, retval))
    else:
        #
        # Send a mail from the parent process just to say that the backup has
        # been kicked off successfully.
        #
        subject = "Backup archiving to external disk started"
        body = "Copying of backups to external disk has started - "
        body += "process %d has been spawned.\n\n" % (retval)
        body += "NOTE: if you do not get a mail tomorrow then something has "
        body += "probably gone wrong.  "
        body += "In that case check out the log file at {0}\n\n".format(
                 log_file_name)
        tmpfile = open("/tmp/enbackup-archive.parent", "w")
        tmpfile.write(body)
        tmpfile.close()
        retval = os.popen('mail -s "%s" %s < %s' %
                          (subject, logfile_email_to, tmpfile.name)).close()
        if retval:
            print("Failed to email results to (%s): %d\n" %
                     (logfile_email_to, retval))
