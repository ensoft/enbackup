###############################################################################
# status.py - Report current status of EnBackup operations
#
# September 2012, Jim M
#
# Copyright (c) 2012 by Ensoft Ltd. All right reserved
#
# Version 1.0 - Initial version
###############################################################################
#
# Overview: this script uses the debug log file maintained by enbackup tools
# to print a status summary of ongoing/completed backup operations.

import argparse
import datetime
import os.path
import re
import sys
from enbackup.utils import run_cmd_output
from collections import namedtuple


# Default log file to search in
DEBUG_LOG = "/var/log/enbackup/enbackup.debug"


# Format for printing timestamps
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


# Default number of debug entries to search, for working out status
# (The full log can be hundreds of MB, so we don't search it all unless
# explicitly requested).
DEFAULT_DEBUG_ENTRIES_TO_SEARCH = 250 * 1000


class DebugFormatError(Exception):
    """
    A debug message was not in the expected format.
    """
    def __init__(self, msg):
        super(DebugFormatError, self).__init__(msg)


class DebugFileAccessError(Exception):
    """
    An error occurred while accessing the debug log file
    """
    def __init__(self, msg):
        super(DebugFileAccessError, self).__init__(msg)


class UserVisibleError(Exception):
    """
    A message that should be reported to the user.
    """
    def __init__(self, user_msg):
        self.user_msg = user_msg
        super(UserVisibleError, self).__init__(user_msg)


class DebugNotFoundError(Exception):
    """
    Some debug messages needed to determin the current status were not found.
    """
    def __init__(self, debug_stmts, searched_event, extra_info=""):
        self.oldest_ts = debug_stmts[0].ts
        self.searched_event = searched_event
        self.extra_info = extra_info
        super(DebugNotFoundError, self).__init__("({},{})".format(
                                                            searched_event,
                                                            extra_info))


class DebugStmt(object):
    """
    A single message from the debug log file

    The following notable attributes are present:

    .. attribute:: ts
            Timestamp of the message.

    .. attribute:: level
            Debug severity level (LOG, DEBUG, ERROR, etc).

    .. attribute:: pid
            PID of the process that emitted the message

    .. attribute:: module
            The module that emitted the message

    .. attribute:: msg
            The text of the message.
    """
    def __init__(self, ts, level, pid, module, msg):
        self.ts = ts
        self.level = level
        self.pid = pid
        self.module = module
        self.msg = msg

    def __str__(self):
        return "DebugStmt({}, {}, {}, {}, {})".format(
                                        self.ts.strftime(TS_FORMAT),
                                        self.level,
                                        self.pid,
                                        self.module,
                                        self.msg)

    @classmethod
    def from_string(cls, string):
        """
        Parse a line of debug, and return a corresponding DebugStmt object.

        :param string:
            The debug string to parse.  This is expected to be of the form:

              2015-12-07 06:50:44,069 INFO    16551 [backup  ]: <message>
        """
        m = re.match(r"([0-9- :]+)"              # Timestamp (group 1)
                     r",[0-9]+\s"                # Timestamp ms (ignored)
                     r"([A-Z]+)\s+"              # Debug level (group 2)
                     r"(\d+)\s+"                 # PID (group 3)
                     r"\["
                         r"([^]]+)\s*"           # Module name (group 4)
                     r"\]:\s+"
                     r"(.*)",                    # Debug message (group 5)
                     string)
        if not m:
            raise DebugFormatError(
                "Failed to match {} against the expected format".format(
                    string))
        ts = datetime.datetime.strptime(m.group(1), TS_FORMAT)
        return DebugStmt(ts, m.group(2), int(m.group(3)),
                         m.group(4).strip(),
                         m.group(5))


def debug_msgs_from_lines(lines):
    """
    Create a list of `DebugStmt`s, given a list of lines of debug.

    :return:
        A list of `DebugStmt`s.

    :param lines:
        Lines of debug to contstruct debug statements from.  The function
        handles messages spanning multiple lines (i.e. it will join consecutive
        entries into multi-line debug statements, if needed).
    """
    # Read each line in the file.  If a line doesn't start with a
    # timestamp (YYYY-MM-DD), assume it's a continuation of the previous line.
    debug_msgs = []
    debug_re = re.compile(r"\d\d\d\d-\d\d-\d\d.*")
    for line in lines:
        if debug_re.match(line):
            debug_msgs.append(line)
        elif len(debug_msgs) > 0:
            debug_msgs[-1] += line

    return debug_msgs


def read_debug_msgs(debug_file=None,
                    whole_file=False):
    """
    Read a debug log, and return a list of `DebugStmt`s.

    The default behaviour is to only read the most recent portion of the
    file (the last `DEFAULT_DEBUG_ENTRIES_TO_SEARCH` lines) for speed,
    but if the `whole_file` flag is set then the full contents will be read.

    :param debug_file:
        Optional name of the debug file to read.  If not set, `DEBUG_LOG` is
        used.

    :param whole_file:
        Optional flag controlling whether to read the full file or not.
    """
    if debug_file is None:
        debug_file = DEBUG_LOG

    if whole_file:
        with open(debug_file, 'r') as f:
            debug_msgs = debug_msgs_from_lines(f)
    else:
        # Only read the last section of the file
        rc, output, err = run_cmd_output(["tail",
                                          "-n",
                                          str(DEFAULT_DEBUG_ENTRIES_TO_SEARCH),
                                          debug_file])
        if rc != 0:
            raise DebugFileAccessError("Failed to read {}: {}".format(
                                            debug_file, err))

        debug_lines = output.split('\n')
        debug_msgs = debug_msgs_from_lines(debug_lines)

    return debug_msgs


def archive_is_running():
    """
    Check whether an enbackup archive operation is running.
    """
    if get_matching_pids("enbackup-archive"):
        return True
    else:
        return False


def elapsed(t1, t2):
    """
    Return a printable string showing the time elapsed from t1 to t2.
    """
    # Use the timedelta's standard formatting, except discard microseconds.
    retval = str(t2 - t1)
    m = re.match(r"([^.]+)\.([0-9]+)", retval)
    if m:
        retval = m.group(1)
    return retval


def archive_summary(debug_stmts):
    """
    Print summary information for enbackup archive operations.

    :param debug_stmts:
        List of `DebugStmt`s to search.
    """
    archive_stmts = [x for x in debug_stmts if x.module == "archive"]
    if len(archive_stmts) == 0:
        oldest_ts = debug_stmts[0].ts
        raise DebugNotFoundError(debug_stmts,
                                 "archiving",
                                 "It's possible that archiving has not been "
                                 "running on this server")

    # Search backwards through the history to find the state of the current
    # operation.
    last_op_status = None
    last_end_stmt = None
    last_completed_start_stmt = None
    last_start_stmt = None
    lock_acquired_stmt = None

    for stmt in reversed(archive_stmts):
        if stmt.msg.startswith("Successfully completed!"):
            last_op_status = last_op_status or "Success"
            last_end_stmt = last_end_stmt or stmt

        elif stmt.msg.startswith("Exception caught - cleaning up"):
            last_op_status = last_op_status or "Failure"
            last_end_stmt = last_end_stmt or stmt

        elif stmt.msg.startswith("Acquired all locks"):
            lock_acquired_stmt = lock_acquired_stmt or stmt

        elif stmt.msg.startswith("Reniced to"):
            if last_end_stmt is not None:
                last_completed_start_stmt = last_completed_start_stmt or stmt
            last_start_stmt = last_start_stmt or stmt

    # Make sure we found everything we need:
    required_info = [ last_op_status,
                      last_end_stmt,
                      last_completed_start_stmt,
                      last_start_stmt,
                      lock_acquired_stmt ]
    if any(x is None for x in required_info):
        print(required_info)
        raise DebugNotFoundError(
                debug_stmts,
                "archiving (found: {})".format(required_info),
                "It's possible that an archiving operation terminated in "
                "an unexpected way, leading to incomplete logs")

    # Inspect what we found and print out the current status:
    if last_start_stmt != last_completed_start_stmt:
        # We found a job that started but didn't complete
        if archive_is_running():
            if lock_acquired_stmt.pid != last_start_stmt.pid:
                waiting_for_lock = "Yes"
            else:
                waiting_for_lock = "No"

            print(("Archiving is currently in progress:\n"
                  "  Job started at: {}\n"
                  "    (elapsed time: {})\n"
                  "  Waiting for lock: {}\n".format(
                            last_start_stmt.ts.strftime(TS_FORMAT),
                            elapsed(last_start_stmt.ts,
                                    datetime.datetime.now()),
                            waiting_for_lock)))
        else:
            print(("WARNING: Last archive operation (pid {}, started at {}) "
                  "did not exit successfully. Check {} for debugging "
                  "information".format(last_start_stmt.pid,
                                       last_start_stmt.ts.strftime(TS_FORMAT),
                                       DEBUG_LOG)))
    else:
        print("Archiving is not currently in progress\n")

    print(("Last completed archiving job:\n"
          "  Overall status: {}\n"
          "  Started at:  {}\n"
          "  Finished at: {}\n"
          "    (elapsed time: {})\n".format(
                last_op_status,
                last_completed_start_stmt.ts.strftime(TS_FORMAT),
                last_end_stmt.ts.strftime(TS_FORMAT),
                elapsed(last_completed_start_stmt.ts, last_end_stmt.ts))))


def get_matching_pids(pattern):
    """
    Get a list of Process IDs for processes matching some pattern.

    :return:
        A list of numerical PIDs

    :param pattern:
        The pattern to search for.  The search includes arguments for running
        processes, as well as process names.
    """
    cmd = ["pgrep", "-f", pattern]
    rc, output, err = run_cmd_output(cmd)
    if rc == 0:
        # One or more processes matched
        pids = [int(p) for p in output.split('\n') if p != ""]
    elif rc == 1:
        # No processes matched
        pids = []
    else:
        raise UserVisibleError("Failed to run {}".format(" ".join(cmd)))
    return pids


class OpTransitions(namedtuple('OpTransitions_',
                               ['start', 'wait', 'run', 'done'])):
    """
    Transition strings to match against debug messages.

    If a debug message is found matching one of these strings, an InProgressOp
    can transition to a new state.
    """
    pass


class OpState:
    """
    Possible states for an in-progress operation.
    """
    START = "Started"
    WAIT = "Waiting for lock"
    RUN = "Running"
    DONE = "Finished running"


class InProgressOp(object):
    """
    An operation that is currently in progress.

    The following notable attributes are present:

    .. attribute:: descr
            Description to identify the operation.

    .. attribute:: trans
            Strings prefixes to search for to identify debug messages that
            indicate the operation has transitioned to a new state.

    .. attribute:: state
            The current state of the operation.

    .. attribute:: ts
            Timestamp that the operation entered its current state.
    """
    def __init__(self, descr, transitions, debug_stmt):
        self.descr = descr
        self.trans = transitions
        self.state = OpState.START
        self.ts = debug_stmt.ts


class BackupOp(InProgressOp):
    """
    An in-progress backup operation.
    """
    trans = OpTransitions("Starting backup at",
                          "About to acquire global lock...",
                          "Acquired global lock",
                          "Lock released")

    def __init__(self, descr, debug_stmt):
        super(BackupOp, self).__init__(descr, self.trans, debug_stmt)


class LocalMirrorOp(InProgressOp):
    """
    An in-progress local mirroring operation.
    """
    trans = OpTransitions("Local mirror command:",
                          "About to lock",
                          "Running command",
                          "Local mirror done")

    def __init__(self, descr, debug_stmt):
        super(LocalMirrorOp, self).__init__(descr, self.trans, debug_stmt)


class RemoteMirrorOp(InProgressOp):
    """
    An in-progress remote mirroring operation.
    """
    trans = OpTransitions("Remote mirror command:",
                          "About to lock",
                          "Running command",
                          "Remote mirror done")

    def __init__(self, descr, debug_stmt):
        super(RemoteMirrorOp, self).__init__(descr, self.trans, debug_stmt)


def get_backup_args(pid):
    """
    Get the arguments that an instance of 'enbackup backup' was invoked with.

    :return:
        A string containing the process arguments (or an error message,
        if attempting to get the arguments fails).

    :param pid:
        The PID of the process to search for.
    """
    cmd = ["ps", "-p", str(pid), "-o", "args", "h"]
    rc, output, err = run_cmd_output(cmd)
    if rc == 0:
        # The rc file should be the first argument after the command
        (_, all_args) = output.split("enbackup backup")
        args = all_args.strip().split()[0]
    else:
        args = "<Cannot find args for PID {}".format(pid)
    return args


def print_header(title):
    """
    Print a title for a section of output, underlining with '=' characters.

    :param title:
        The title to print.
    """
    underline = "".join(['=' for char in title])
    print(title)
    print(underline)


def in_progress_status(debug_stmts):
    """
    Display the status of any currently-running operations.

    :param debug_stmts:
        List of `DebugStmt`s to search.
    """
    # First, work out which jobs we should be interested in
    interesting_pids = set(get_matching_pids("enbackup"))

    # Now, look through our list of debug statements relating to those pids,
    # and record the state of each operation, by matching the debug statements
    # that indicate interesting events for each task.
    ops = {}
    interesting_stmts = [s for s in debug_stmts if s.pid in interesting_pids]
    for stmt in interesting_stmts:
        op = ops.get(stmt.pid)

        if op is None:
            # Not known yet, see if it's the start of an operation we're
            # interested in.
            if stmt.msg.startswith(BackupOp.trans.start):
                ops[stmt.pid] = BackupOp("Backup, using config from {}".format(
                                            get_backup_args(stmt.pid)),
                                         stmt)
            elif stmt.msg.startswith(LocalMirrorOp.trans.start):
                # Source and target are the last two arguments, which we can
                # get by splitting up the debug message into tokens and
                # removing extra ',] characters.
                toks = stmt.msg.split(" ")
                src = toks[-2].strip(",'[]")
                dst = toks[-1].strip(",'[]")
                ops[stmt.pid] = LocalMirrorOp("Local mirror from "
                                              "{} to {}".format(src, dst),
                                              stmt)
            elif stmt.msg.startswith(RemoteMirrorOp.trans.start):
                # Data being sent is the last argument
                toks = stmt.msg.split(" ")
                src = toks[-1].strip(",'[]")
                ops[stmt.pid] = RemoteMirrorOp("Remote mirror, "
                                               "transmitting {}".format(src),
                                               stmt)
        else:
            # See if we can perform a transition.
            if stmt.msg.startswith(op.trans.wait):
                if op.state == OpState.START:
                    op.state = OpState.WAIT
                    op.ts = stmt.ts
            elif stmt.msg.startswith(op.trans.run):
                if op.state == OpState.WAIT:
                    op.state = OpState.RUN
                    op.ts = stmt.ts
            elif stmt.msg.startswith(op.trans.done):
                if op.state == OpState.RUN:
                    op.state = OpState.DONE
                    op.ts = stmt.ts

    # Now collect the operations into groups depending on their status.
    # Note that 'starting' and 'finishing' should be very brief transient
    # states while an operation is starting up or shutting down.  For
    # display purposes, we group them together under 'running', but print
    # an extra note to indicate the detailed state.
    starting_ops = []
    waiting_ops = []
    running_ops = []
    finishing_ops = []

    for pid, op in ops.items():
        if op.state == OpState.START:
            starting_ops.append(pid)
            running_ops.append(pid)
        elif op.state == OpState.WAIT:
            waiting_ops.append(pid)
        elif op.state == OpState.RUN:
            running_ops.append(pid)
        elif op.state == OpState.DONE:
            finishing_ops.append(pid)
            running_ops.append(pid)

    def _print_op(pid, op_descr, state_descr):
        print(("  {}: {}{}".format(pid, ops[pid].descr, extra)))
        indent_len = len(str(pid))
        indent = "".join([" " for i in range(0, indent_len)])
        print(("  {}  {} for {}".format(
                indent,
                state_descr,
                elapsed(ops[pid].ts, datetime.datetime.now()))))

    print("The following jobs are currently running:")
    for pid in running_ops:
        if pid in starting_ops:
            extra = " (starting up, not yet running)"
        elif pid in finishing_ops:
            extra = " (finished, should exit soon)"
        else:
            extra = ""
        _print_op(pid, "{}{}".format(ops[pid].descr, extra), "Running")
    print("")

    print("The following jobs are currently queued, waiting to start:")
    for pid in waiting_ops:
        _print_op(pid, ops[pid].descr, "Waiting")
    print("")


def overall_status(debug_stmts):
    """
    Display the overall status of all backup jobs.

    :param debug_stmts:
        List of `DebugStmt`s to search.
    """
    print_header("Running/queued jobs")
    in_progress_status(debug_stmts)

    # Try printing archiving status.  If we hit a DebugNotFoundError then
    # report it but keep going (archiving status wasn't explicitly requested,
    # so it's OK if it's missing -- it's not present on all servers).
    print_header("Archiving status")
    try:
        archive_summary(debug_stmts)
    except DebugNotFoundError as e:
        print("No archiving information found on this server\n")

    print_header("Backup set status")
    # Print the status of each backup set in turn
    for backup_set in get_backup_sets(debug_stmts):
        status_of_single_set(debug_stmts, backup_set)


def get_backup_sets(debug_stmts):
    """
    Return a list of all backup sets referenced in a collection of debug.

    :return:
        A list of strings, where each string is the name of a backup set.

    :param debug_stmts:
        List of `DebugStmt`s to search.
    """
    handle_set = re.compile(r"Handling set '([\w/]+)'")
    backup_sets = set([])
    for stmt in debug_stmts:
        if stmt.module == "backup":
            m = handle_set.match(stmt.msg)
            if m:
                backup_sets.add(m.group(1))
    return sorted(backup_sets)


def print_backup_sets(debug_stmts):
    """
    Display all backup sets found in the debug.

    :param debug_stmts:
        List of `DebugStmt`s to search.
    """
    backup_sets = get_backup_sets(debug_stmts)
    print("Found the following backup sets:")
    for s in backup_sets:
        print(("  {}".format(s)))


def status_of_single_set(debug_stmts, backup_set):
    """
    Display the status of a single backup set.

    :param debug_stmts:
        List of `DebugStmt`s to search.

    :param backup_set:
        The name of the backup set to search for (as reported by
        `get_backup_sets`).
    """
    # Search backwards through the log file to find the last time the
    # specified backup set was referenced.  Once we find it, look at
    # the previous (in reversed order, i.e. later in real time) message
    # reporting the status, to find out whether the backup command
    # was successful or not.
    start_of_set_msg = "Handling set '{}'".format(backup_set)
    backup_stmts = [x for x in debug_stmts if x.module == "backup"]
    found = False

    last_status = None
    last_status_msg = None

    for stmt in reversed(backup_stmts):
        if stmt.msg == "rdiff-backup success!":
            last_status = "Success"
            last_status_msg = stmt
        elif stmt.msg.startswith("ERROR: rdiff-backup command failed"):
            last_status = "Failure"
            last_status_msg = stmt

        if last_status is not None and stmt.msg == start_of_set_msg:
            found = True
            break

    if not found:
        raise DebugNotFoundError(debug_stmts,
                                 "backup set {}".format(backup_set))

    print(("Last result for backing up '{}': {}\n"
          "  Backup started at:  {}\n"
          "  Backup finished at: {}\n"
          "    (elapsed time: {})\n".format(
            backup_set,
            last_status,
            stmt.ts.strftime(TS_FORMAT),
            last_status_msg.ts.strftime(TS_FORMAT),
            elapsed(stmt.ts, last_status_msg.ts))))


def main(arguments):
    """
    Main function for this module.

    :param arguments:
        A list of the arguments for this module.
    """
    parser = argparse.ArgumentParser(
                        prog="enbackup status",
                        description="Display status on this server")
    parser.add_argument("-a", "--archive-status", action="store_true",
                        help="Print status of archiving operations.")
    parser.add_argument("--search-full-history", action="store_true",
                        help="Search the entire enbackup log file to "
                        "determine current status. This can be very slow. "
                        "The default behaviour is only to search the last "
                        "{} entries.".format(DEFAULT_DEBUG_ENTRIES_TO_SEARCH))
    parser.add_argument("--log-file", action="store",
                        help="Specify the log file to search for status "
                        "information.")
    parser.add_argument("--list-backup-sets", action="store_true",
                        help="Print possible backup sets that can be passed "
                        "to --backup-set.")
    parser.add_argument("--backup-set", action="store",
                        help="Display the status of a single backup set "
                        "originating on this server.")
    parser.add_argument("--running-jobs", action="store_true",
                        help="Display the status of running and queued backup "
                        "jobs on this server.")
    args = parser.parse_args(args=arguments)

    try:
        debug_msgs = read_debug_msgs(debug_file=args.log_file,
                                     whole_file=args.search_full_history)
        debug_stmts = [DebugStmt.from_string(x) for x in debug_msgs]

        if not (args.archive_status or
                args.list_backup_sets or
                args.backup_set or
                args.running_jobs):
            overall_status(debug_stmts)
        else:
            if args.running_jobs:
                in_progress_status(debug_stmts)

            if args.archive_status:
                archive_summary(debug_stmts)

            if args.list_backup_sets:
                print_backup_sets(debug_stmts)

            if args.backup_set:
                status_of_single_set(debug_stmts, args.backup_set)

    except UserVisibleError as e:
        print((e.user_msg))

    except DebugNotFoundError as e:
        print(("No information about {} found in recent logs (oldest "
                "date searched was {}). You may need to try the "
                "--search-full-history option. {}".format(
                    e.searched_event,
                    e.oldest_ts.strftime(TS_FORMAT),
                    e.extra_info)))


if __name__ == "__main__":
    # Invoke the main command with options passed by the user.
    main(sys.argv[1:])

