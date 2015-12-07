#!/usr/bin/python

import os
import shutil
import sys

to_remove = []

if len(sys.argv) == 1:
    print("USAGE: %s {path to convert}" % (os.path.split(sys.argv[0]))[1])
    print("Converts an rdiff increment/mirror directory to mirror only")
    sys.exit(-1)

if len(sys.argv) != 2:
    print("Unexpected number of arguments - expect just path")
    print("USAGE: %s {path to convert}" % (os.path.split(sys.argv[0]))[1])
    sys.exit(-1)

rdiffdir = sys.argv[1]

if rdiffdir == "/":
    print("Blocking deletion on root directory path!!!")
    sys.exit(-1)
if rdiffdir[0:9] == "/enbackup":
    print("Blocking deletion on main rdiff-backup directory path!!!")
    sys.exit(-1)

print("Scanning for 'rdiff-backup-data' files in '%s'" % (rdiffdir))

for root, dirs, files in os.walk(rdiffdir):
    for dir in dirs:
        if dir == "rdiff-backup-data":
            full_dir = os.path.join(root, dir)
            #print("Found %s" % (full_dir))
            dirs.remove(dir)
            to_remove.append(full_dir)
        #else:
            #print("  skipping %s" % (os.path.join(root, dir)))

print("Have %d directories to remove" % len(to_remove))

prog_counter = 1

for dir in to_remove:
    print("[%d/%d] Removing %s" % (prog_counter, len(to_remove), dir))
    shutil.rmtree(dir)
    prog_counter += 1
