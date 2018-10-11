#!/usr/bin/python

import argparse
import datetime
import glob
import os
import subprocess

import enbackup.utils

def error(str):
    raise Exception(str)


def debug(str):
    print(str)


def run_cmd(cmd, debug_only=False, measure=False):
    #
    # Spawn a subprocess (or not if we are in show-only mode), returning
    # [retcode, stderr, stdout]
    #
    debug("Running '%s'" % (" ".join(cmd)))
    before = datetime.datetime.now()
    if not debug_only:
        subproc = subprocess.Popen(cmd,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = subproc.communicate()
        if subproc.returncode:
            error("Failed to run {}:\n"
                  "stdout: {}\n"
                  "stderr: {}".format(stdout, stderr))

    if measure:
        after = datetime.datetime.now()
        debug("  Done in {}".format(str(after - before)))
    else:
        debug("  Done")


def get_data_dir(dir):
    data_dir = os.path.join(dir, "rdiff-backup-data")
    # Check this looks like an rdiff-backup archive
    if not os.path.exists(data_dir):
        error("{} does not exist, this doesn't "
              "look like a backup dir".format(data_dir))

    return data_dir


def remove_suffix(s, suff):
    # remove '<suff>' or '<suff>.gz' from 's'
    # Complain if 's' doesn't end in '<suff>' or '<suff>.gz'
    rv = s

    if rv.endswith(".gz"):
        rv = rv[:-3]
    if rv.endswith(suff):
        rv = rv[:-len(suff)]
    else:
        error("{} did not end in {}".format(s, suff))

    return rv


def do_copy(backup_set, target_dir_parent):
    # Check the target directory doesn't already exist.
    if os.path.exists(target_dir):
        error("Target directory {} already exists.  Exiting "
              "rather than overwritring".format(target_dir))

    # Create a directory tree that matches the backup set
    # location, in the target area.
    os.makedirs(target_dir_parent, mode=0o700)

    print("Copying {} to {}".format(backup_set, target_dir_parent))
    run_cmd(["cp", "-a", backup_set, target_dir_parent], measure=True)


def restore_mdata_files(target_dir, data_dir, check_only=True):
    # Check for the metadata files we'll want to preserve.  We need
    #
    #   current_mirror.<DATE>.data
    #
    # (There should be one of these.)  And then we need each of
    # the following, for the same <DATE> value:
    #
    #   access_control_lists.<DATE>.snapshot
    #   current_mirror.<DATE>.data
    #   error_log.<DATE>.data
    #   extended_attributes.<DATE>.snapshot
    #   file_statistics.<DATE>.data.gz
    #   mirror_metadata.<DATE>.snapshot.gz
    #   session_statistics.<DATE>.data
    #
    # Each file may or may not be compressed (have a .gz suffix)
    target_dir_data = get_data_dir(target_dir)
    current_mirror_files = glob.glob(os.path.join(target_dir_data,
                                             "current_mirror.*"))
    if len(current_mirror_files) != 1:
        error("Expected exactly one mirror file under {} "
              "but found {}".format(target_dir_data,
                                    current_mirror_files))

    current_mirror_file = current_mirror_files[0]
    current_mirror_file = os.path.basename(current_mirror_file)
    start, end = current_mirror_file.split('.', 1)
    if start != "current_mirror":
        error("Current mirror file name looks wrong: "
              "{}".format(current_mirror_file))

    mirror_date = remove_suffix(end, ".data")

    mdata_files = glob.glob(os.path.join(target_dir_data,
                                     "*.{}.*".format(mirror_date)))
    expect_to_see = ["access_control_lists",
                     "error_log",
                     "extended_attributes",
                     "file_statistics",
                     "mirror_metadata",
                     "session_statistics",
                     "current_mirror"]
    to_keep = []
    seen = {}

    for expect in expect_to_see:
        seen[expect] = False

    for found in mdata_files:
        for expect in expect_to_see:
            if os.path.basename(found).startswith(expect):
                seen[expect] = True
                to_keep.append(found)

    print("Going to keep these metadata files:")
    for f in to_keep:
        print("  {}".format(f))

    if check_only:
        print(" *** DEBUG ONLY, NOT ACTUALLY RESTORING ***")
    else:
        print("Restoring metadata")

    for f in to_keep:
        run_cmd(["cp", "-a", f, data_dir], debug_only=check_only)


def delete_old_data(data_dir):
    # Clean up of old data.  Delete the rdiff-backup-data dir, and
    # then re-create it, owned by enbackup.
    print("Deleting old data")
    run_cmd(["rm", "-r", data_dir], measure=True)
    run_cmd(["mkdir", "-m", "700", data_dir])
    enbackup.utils.chown_enbackup(data_dir)


def lock_source_dir(source_dir):
    # Lock the source directory, and all parent directories, to make
    # sure no other enbackup jobs change it while we're running.
    locks = []
    while source_dir != '/':
        print("Locking directory {}".format(source_dir))
        lock_details = "enbackup-trim-old-data, PID {}\n".format(os.getpid())
        lock = enbackup.utils.DirLock(lock_details,
                                      source_dir)
        lock.lock()
        locks.append(lock)
        source_dir = os.path.dirname(source_dir)

    return locks


parser = argparse.ArgumentParser()
parser.add_argument("--backup-set")
parser.add_argument("--target-dir-ancestor")
parser.add_argument("--copy", action='store_true')
parser.add_argument("--delete", action='store_true')
parser.add_argument("--check-mdata", action='store_true')
parser.add_argument("--restore-mdata", action='store_true')
args = parser.parse_args()

# backup_set is what we are copying.
# target_dir_ancestor is the top-level directory we're storing all copies
# under.
# target_dir is the directory we will be copying into, underneath
# target_dir_ancestor.
# target_dir_parent is the directory immediately above target_dir.
#
# E.g. if
#  backup_set is           /enbackup/foo/bar
#  target_dir_ancestor is  /mnt/ext-disk/archive
#
# then target_dir is       /mnt/ext-disk/archive/enbackup/foo/bar
# and target_dir_parent is /mnt/ext-disk/archive/enbackup/foo
backup_set = args.backup_set
target_dir_ancestor = args.target_dir_ancestor

locks = lock_source_dir(backup_set)

if os.path.isabs(backup_set):
    rel_backup_set = backup_set[1:]
else:
    rel_backup_set = backup_set

target_dir = os.path.join(target_dir_ancestor, rel_backup_set)
target_dir_parent = os.path.dirname(target_dir)

data_dir = get_data_dir(backup_set)

if args.copy:
    do_copy(backup_set, target_dir_parent)
else:
    print("Skipping copy")

if args.check_mdata:
    restore_mdata_files(target_dir, data_dir, check_only=True)

if args.delete:
    delete_old_data(data_dir)
else:
    print("Skipping delete")

if args.delete or args.restore_mdata:
    restore_mdata_files(target_dir, data_dir, check_only=False)
else:
    print("Skipping restore")

print("Releasing locks")
for lock in locks:
    lock.unlock()
