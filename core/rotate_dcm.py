# rotate-backups: Simple command line interface for backup rotation.
#
# Author: Peter Odding <peter@peterodding.com>
# Last Change: March 21, 2016 (original)
# URL: https://github.com/xolox/python-rotate-backups
# Modified by dacopanCM <dacopan.bsc@gmail.com>
# modifications:
#    - Python api class :class:`RotateBackupsCM`
#    - :class:`RotateBackupsCM` construct now has new param `gdrive` to manage remote actions on Google Drive
#    - rotate_backups modified to call `gdrive` to delete files on Google Drive
#    - custom_format_path instead of format_path to no format Google Drive files
#    - collect_backups to get files from Google Drive

"""
Simple to use Python API for rotation of backups.
The :mod:`rotate_backups` module contains the Python API of the
`rotate-backups` package. The core logic of the package is contained in the
:class:`RotateBackupsCM` class.
"""

# Standard library modules.
import collections
import datetime
import fnmatch
import functools
import logging
import os
import re

# External dependencies.
from dateutil.relativedelta import relativedelta
from executor import execute
from humanfriendly import format_path, Timer
from humanfriendly.text import concatenate
from natsort import natsort

# Semi-standard module versioning.
__version__ = '2.3'

# Initialize a logger for this module.
logger = logging.getLogger('dacopancm.' + __name__)

ORDERED_FREQUENCIES = (('hourly', relativedelta(hours=1)),
                       ('daily', relativedelta(days=1)),
                       ('weekly', relativedelta(weeks=1)),
                       ('monthly', relativedelta(months=1)),
                       ('yearly', relativedelta(years=1)))
"""
A list of tuples with two values each:
- The name of a rotation frequency (a string like 'hourly', 'daily', etc.).
- A :class:`~dateutil.relativedelta.relativedelta` object.
The tuples are sorted by increasing delta (intentionally).
"""

SUPPORTED_FREQUENCIES = dict(ORDERED_FREQUENCIES)
"""
A dictionary with rotation frequency names (strings) as keys and
:class:`~dateutil.relativedelta.relativedelta` objects as values. This
dictionary is generated based on the tuples in :data:`ORDERED_FREQUENCIES`.
"""
# (?P<year>\d{4})\D?(?P<month>\d{2}) \D?(?P<day>\d{2})\D?(?:(?P<hour>\d{2})\D?(?P<minute>\d{2}) \D?(?P<second>\d{2})?)?
TIMESTAMP_PATTERN = re.compile(r'''
    # Required components.
    (?P<year>\d{4} ) \D?
    (?P<month>\d{2}) \D?
    (?P<day>\d{2}  ) \D?
    (?:
        # Optional components.
        (?P<hour>\d{2}  ) \D?
        (?P<minute>\d{2}) \D?
        (?P<second>\d{2})?
    )?
''', re.VERBOSE)
"""
A compiled regular expression object used to match timestamps encoded in
filenames.
"""


def coerce_retention_period(value):
    """
    Coerce a retention period to a Python value.
    :param value: A string containing an integer number or the text 'always'.
    :returns: An integer number or the string 'always'.
    :raises: :exc:`~exceptions.ValueError` when the string can't be coerced.
    """
    value = value.strip()
    if value.lower() == 'always':
        return 'always'
    elif value.isdigit():
        return int(value)
    else:
        raise ValueError("Invalid retention period! (%s)" % value)


class RotateBackupsCM(object):
    """Python API for the ``rotate-backups`` program."""

    def __init__(self, rotation_scheme, include_list=None, exclude_list=None,
                 dry_run=False, io_scheduling_class=None, rotate_type='local', gdrivecm=None):
        """
        Construct a :class:`RotateBackupsCM` object.
        :param rotation_scheme: A dictionary with one or more of the keys 'hourly',
                                'daily', 'weekly', 'monthly', 'yearly'. Each key is
                                expected to have one of the following values:
                                - An integer gives the number of backups in the
                                  corresponding category to preserve, starting from
                                  the most recent backup and counting back in
                                  time.
                                - The string 'always' means all backups in the
                                  corresponding category are preserved (useful for
                                  the biggest time unit in the rotation scheme).
                                By default no backups are preserved for categories
                                (keys) not present in the dictionary.
        :param include_list: A list of strings with :mod:`fnmatch` patterns. If a
                             nonempty include list is specified each backup must
                             match a pattern in the include list, otherwise it
                             will be ignored.
        :param exclude_list: A list of strings with :mod:`fnmatch` patterns. If a
                             backup matches the exclude list it will be ignored,
                             *even if it also matched the include list* (it's the
                             only logical way to combine both lists).
        :param dry_run: If this is ``True`` then no changes will be made, which
                        provides a 'preview' of the effect of the rotation scheme
                        (the default is ``False``). Right now this is only useful
                        in the command line interface because there's no return
                        value.
        :param io_scheduling_class: Use ``ionice`` to set the I/O scheduling class
                                    (expected to be one of the strings 'idle',
                                    'best-effort' or 'realtime').
        """
        self.rotation_scheme = rotation_scheme
        self.include_list = include_list
        self.exclude_list = exclude_list
        self.dry_run = dry_run
        self.io_scheduling_class = io_scheduling_class
        self.rotate_type = rotate_type
        if rotate_type == 'remote':
            self.gdrivecm = gdrivecm

    def rotate_backups(self, directory):
        """
        Rotate the backups in a directory according to a flexible rotation scheme.
        :param directory: The pathname of a directory that contains backups to
                          rotate (a string).

        .. note:: This function binds the main methods of the
                  :class:`RotateBackups` class together to implement backup
                  rotation with an easy to use Python API. If you're using
                  `rotate-backups` as a Python API and the default behavior is
                  not satisfactory, consider writing your own
                  :func:`rotate_backups()` function based on the underlying
                  :func:`collect_backups()`, :func:`group_backups()`,
                  :func:`apply_rotation_scheme()` and
                  :func:`find_preservation_criteria()` methods.
        """
        # Load configuration overrides by user?

        # Collect the backups in the given directory. if rotate type is on local or on google drive
        sorted_backups = self.collect_backups(directory, self.rotate_type)
        if not sorted_backups:
            logger.info("No backups found in %s.", self.custom_format_path(directory))
            return
        most_recent_backup = sorted_backups[-1]
        # Group the backups by the rotation frequencies.
        backups_by_frequency = self.group_backups(sorted_backups)
        # Apply the user defined rotation scheme.
        self.apply_rotation_scheme(backups_by_frequency, most_recent_backup.datetime)
        # Find which backups to preserve and why.
        backups_to_preserve = self.find_preservation_criteria(backups_by_frequency)
        # Apply the calculated rotation scheme.
        for backup in sorted_backups:
            if backup in backups_to_preserve:
                matching_periods = backups_to_preserve[backup]
                logger.info("Preserving %s (matches %s retention %s) ..",
                            self.custom_format_path(backup.pathname),
                            concatenate(map(repr, matching_periods)),
                            "period" if len(matching_periods) == 1 else "periods")
            else:
                logger.info("Deleting %s %s ..", backup.type, self.custom_format_path(backup.pathname))
                if not self.dry_run:
                    timer = Timer()
                    if self.rotate_type == 'local':  # if rotate type is on local or on google drive
                        command = ['rm', '-Rf', backup.pathname]
                        if self.io_scheduling_class:
                            command = ['ionice', '--class', self.io_scheduling_class] + command

                        execute(*command, logger=logger)
                    else:
                        self.gdrivecm.delete_file(backup.pathname.split('_')[0])
                    logger.debug("Deleted %s in %s.", self.custom_format_path(backup.pathname), timer)
        if len(backups_to_preserve) == len(sorted_backups):
            logger.info("Nothing to do! (all backups preserved)")

    def collect_backups(self, directory, rotate_type):
        """
        Collect the backups in the given directory. on local or google drive
        :param directory: The pathname of an existing directory (a string).
        :param rotate_type: The rotate type if local o remote on Google Drive
        :returns: A sorted :class:`list` of :class:`Backup` objects (the
                  backups are sorted by their date).
        """
        backups = []
        # directory = os.path.abspath(directory)
        directory = os.path.abspath(directory) if not rotate_type == 'remote' else directory
        logger.info("Scanning %s directory for backups: %s", rotate_type, self.custom_format_path(directory))
        # get files from local if rotate_type is local else get files from GoogleDrive
        files = os.listdir(directory) if not rotate_type == 'remote' else self.gdrivecm.get_files(directory)

        for entry in natsort(files):
            # Check for a time stamp in the directory entry's name.
            match = TIMESTAMP_PATTERN.search(entry)
            if match:
                # Make sure the entry matches the given include/exclude patterns.
                if self.exclude_list and any(fnmatch.fnmatch(entry, p) for p in self.exclude_list):
                    logger.debug("Excluded %r (it matched the exclude list).", entry)
                elif self.include_list and not any(fnmatch.fnmatch(entry, p) for p in self.include_list):
                    logger.debug("Excluded %r (it didn't match the include list).", entry)
                else:
                    backups.append(Backup(
                        pathname=os.path.join(directory, entry) if not rotate_type == 'remote' else entry,
                        datetime=datetime.datetime(*(int(group, 10) for group in match.groups('0'))),
                    ))
            else:
                logger.debug("Failed to match time stamp in filename: %s", entry)
        if backups:
            logger.info("Found %i timestamped backups in %s.", len(backups), self.custom_format_path(directory))
        return sorted(backups)

    def group_backups(self, backups):
        """
        Group backups collected by :func:`collect_backups()` by rotation frequencies.
        :param backups: A :class:`set` of :class:`Backup` objects.
        :returns: A :class:`dict` whose keys are the names of rotation
                  frequencies ('hourly', 'daily', etc.) and whose values are
                  dictionaries. Each nested dictionary contains lists of
                  :class:`Backup` objects that are grouped together because
                  they belong into the same time unit for the corresponding
                  rotation frequency.
        """
        backups_by_frequency = dict((frequency, collections.defaultdict(list)) for frequency in SUPPORTED_FREQUENCIES)
        for b in backups:
            backups_by_frequency['hourly'][(b.year, b.month, b.day, b.hour)].append(b)
            backups_by_frequency['daily'][(b.year, b.month, b.day)].append(b)
            backups_by_frequency['weekly'][(b.year, b.week)].append(b)
            backups_by_frequency['monthly'][(b.year, b.month)].append(b)
            backups_by_frequency['yearly'][b.year].append(b)
        return backups_by_frequency

    def apply_rotation_scheme(self, backups_by_frequency, most_recent_backup):
        """
        Apply the user defined rotation scheme to the result of :func:`group_backups()`.
        :param backups_by_frequency: A :class:`dict` in the format generated by
                                     :func:`group_backups()`.
        :param most_recent_backup: The :class:`~datetime.datetime` of the most
                                   recent backup.
        :raises: :exc:`~exceptions.ValueError` when the rotation scheme
                 dictionary is empty (this would cause all backups to be
                 deleted).
        .. note:: This method mutates the given data structure by removing all
                  backups that should be removed to apply the user defined
                  rotation scheme.
        """
        if not self.rotation_scheme:
            raise ValueError("Refusing to use empty rotation scheme! (all backups would be deleted)")
        for frequency, backups in backups_by_frequency.items():
            # Ignore frequencies not specified by the user.
            if frequency not in self.rotation_scheme:
                backups.clear()
            else:
                # Reduce the number of backups in each period of this rotation
                # frequency to a single backup (the first in the period).
                for period, backups_in_period in backups.items():
                    first_backup = sorted(backups_in_period)[0]
                    backups[period] = [first_backup]
                # Check if we need to rotate away backups in old periods.
                retention_period = self.rotation_scheme[frequency]
                if retention_period != 'always':
                    # Remove backups created before the minimum date of this
                    # rotation frequency (relative to the most recent backup).
                    minimum_date = most_recent_backup - SUPPORTED_FREQUENCIES[frequency] * retention_period
                    for period, backups_in_period in list(backups.items()):
                        for backup in backups_in_period:
                            if backup.datetime < minimum_date:
                                backups_in_period.remove(backup)
                        if not backups_in_period:
                            backups.pop(period)
                    # If there are more periods remaining than the user
                    # requested to be preserved we delete the oldest one(s).
                    items_to_preserve = sorted(backups.items())[-retention_period:]
                    backups_by_frequency[frequency] = dict(items_to_preserve)

    def find_preservation_criteria(self, backups_by_frequency):
        """
        Collect the criteria used to decide which backups to preserve.
        :param backups_by_frequency: A :class:`dict` in the format generated by
                                     :func:`group_backups()` which has been
                                     processed by :func:`apply_rotation_scheme()`.
        :returns: A :class:`dict` with :class:`Backup` objects as keys and
                  :class:`list` objects containing strings (rotation
                  frequencies) as values.
        """
        backups_to_preserve = collections.defaultdict(list)
        for frequency, delta in ORDERED_FREQUENCIES:
            for period in backups_by_frequency[frequency].values():
                for backup in period:
                    backups_to_preserve[backup].append(frequency)
        return backups_to_preserve

    def custom_format_path(self, directory):
        return format_path(directory) if not self.rotate_type == 'remote' else directory


@functools.total_ordering
class Backup(object):
    """
    :py:class:`Backup` objects represent a rotation subject.
    In addition to the :attr:`type` and :attr:`week` properties :class:`Backup`
    objects support all of the attributes of :py:class:`~datetime.datetime`
    objects by deferring attribute access for unknown attributes to the
    :py:class:`~datetime.datetime` object given to the constructor.
    """

    def __init__(self, pathname, datetime):
        """
        Initialize a :py:class:`Backup` object.
        :param pathname: The filename of the backup (a string).
        :param datetime: The date/time when the backup was created (a
                         :py:class:`~datetime.datetime` object).
        """
        self.pathname = pathname
        self.datetime = datetime

    @property
    def type(self):
        """Get a string describing the type of backup (e.g. file, directory)."""
        if os.path.islink(self.pathname):
            return 'symbolic link'
        elif os.path.isdir(self.pathname):
            return 'directory'
        else:
            return 'file'

    @property
    def week(self):
        """Get the ISO week number."""
        return self.datetime.isocalendar()[1]

    def __getattr__(self, name):
        """Defer attribute access to the datetime object."""
        return getattr(self.datetime, name)

    def __repr__(self):
        """Enable pretty printing of :py:class:`Backup` objects."""
        return "Backup(pathname=%r, datetime=%r)" % (self.pathname, self.datetime)

    def __hash__(self):
        """Make it possible to use :py:class:`Backup` objects in sets and as dictionary keys."""
        return hash(self.pathname)

    def __eq__(self, other):
        """Make it possible to use :py:class:`Backup` objects in sets and as dictionary keys."""
        return type(self) == type(other) and self.datetime == other.datetime

    def __lt__(self, other):
        """Enable proper sorting of backups."""
        return self.datetime < other.datetime
