/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.je.cleaner;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;

/**
 * Iterator over files that should be migrated by cleaning them, even if
 * they don't need to be cleaned for other reasons.
 *
 * Files are migrated either because they are named in the
 * CLEANER_FORCE_CLEAN_FILES parameter or their log version is prior to the
 * CLEANER_UPGRADE_TO_LOG_VERSION parameter.
 *
 * An iterator is used rather than finding the entire set at startup to
 * avoid opening a large number of files to examine their log version.  For
 * example, if all files are being migrated in a very large data set, this
 * would involve opening a very large number of files in order to read
 * their header.  This could significantly delay application startup.
 *
 * Because we don't have the entire set at startup, we can't select the
 * lowest utilization file from the set to clean next.  Inteaad we iterate
 * in file number order to increase the odds of cleaning lower utilization
 * files first.
 */
class FilesToMigrate {

    private final EnvironmentImpl env;

    /**
     * An array of pairs of file numbers, where each pair is a range of
     * files to be force cleaned.  Index i is the from value and i+1 is the
     * to value, both inclusive.
     */
    private long[] forceCleanFiles;

    /** Log version to upgrade to, or zero if none. */
    private int upgradeToVersion;

    /** Whether to continue checking the log version. */
    private boolean checkLogVersion;

    /** Whether hasNext() has prepared a valid nextFile. */
    private boolean nextAvailable;

    /** File to return; set by hasNext() and returned by next(). */
    private long nextFile;

    FilesToMigrate(EnvironmentImpl env) {
        this.env = env;
        String forceCleanProp = env.getConfigManager().get
            (EnvironmentParams.CLEANER_FORCE_CLEAN_FILES);
        parseForceCleanFiles(forceCleanProp);

        upgradeToVersion = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_UPGRADE_TO_LOG_VERSION);
        if (upgradeToVersion == -1) {
            upgradeToVersion = LogEntryType.LOG_VERSION;
        }

        checkLogVersion = (upgradeToVersion != 0);
        nextAvailable = false;
        nextFile = -1;
    }

    /**
     * Returns whether there are more files to be migrated.  Must be called
     * while synchronized on the UtilizationProfile.
     */
    boolean hasNext(SortedMap<Long, FileSummary> fileSummaryMap,
                    Set<Long> inProgressFiles) {
        if (nextAvailable) {
            /* hasNext() has returned true since the last next(). */
            return true;
        }
        long foundFile = -1;
        for (long file :
             fileSummaryMap.tailMap(nextFile + 1).keySet()) {
            if (inProgressFiles.contains(file)) {
                continue;
            }
            if (isForceCleanFile(file)) {
                /* Found a file to force clean. */
                foundFile = file;
                break;
            } else if (checkLogVersion) {
                try {
                    int logVersion =
                        env.getFileManager().getFileLogVersion(file);
                    if (logVersion < upgradeToVersion) {
                        /* Found a file to migrate. */
                        foundFile = file;
                        break;
                    } else {

                        /*
                         * All following files have a log version greater
                         * or equal to this one; stop checking.
                         */
                        checkLogVersion = false;
                    }
                } catch (RuntimeException e) {
                    /* Throw exception but allow iterator to continue. */
                    nextFile = file;
                    throw e;
                }
            }
        }
        if (foundFile != -1) {
            nextFile = foundFile;
            nextAvailable = true;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the next file file to be migrated.  Must be called while
     * synchronized on the UtilizationProfile.
     */
    long next(SortedMap<Long, FileSummary> fileSummaryMap,
              Set<Long> inProgressFiles)
        throws NoSuchElementException {

        if (hasNext(fileSummaryMap, inProgressFiles)) {
            nextAvailable = false;
            return nextFile;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns whether the given file is in the forceCleanFiles set.
     */
    private boolean isForceCleanFile(long file) {

        if (forceCleanFiles != null) {
            for (int i = 0; i < forceCleanFiles.length; i += 2) {
                long from = forceCleanFiles[i];
                long to = forceCleanFiles[i + 1];
                if (file >= from && file <= to) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Parses the je.cleaner.forceCleanFiles property value and initializes
     * the forceCleanFiles field.
     *
     * @throws IllegalArgumentException via Environment ctor and
     * setMutableConfig.
     */
    private void parseForceCleanFiles(String propValue)
        throws IllegalArgumentException {

        if (propValue == null || propValue.length() == 0) {
            forceCleanFiles = null;
        } else {
            String errPrefix = "Error in " +
                EnvironmentParams.CLEANER_FORCE_CLEAN_FILES.getName() +
                "=" + propValue + ": ";

            StringTokenizer tokens = new StringTokenizer
                (propValue, ",-", true /*returnDelims*/);

            /* Resulting list of Long file numbers. */
            List<Long> list = new ArrayList<Long>();

            while (tokens.hasMoreTokens()) {

                /* Get "from" file number. */
                String fromStr = tokens.nextToken();
                long fromNum;
                try {
                    fromNum = Long.parseLong(fromStr, 16);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException
                        (errPrefix + "Invalid hex file number: " +
                         fromStr);
                }

                long toNum = -1;
                if (tokens.hasMoreTokens()) {

                    /* Get delimiter. */
                    String delim = tokens.nextToken();
                    if (",".equals(delim)) {
                        toNum = fromNum;
                    } else if ("-".equals(delim)) {

                        /* Get "to" file number." */
                        if (tokens.hasMoreTokens()) {
                            String toStr = tokens.nextToken();
                            try {
                                toNum = Long.parseLong(toStr, 16);
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException
                                    (errPrefix +
                                     "Invalid hex file number: " +
                                     toStr);
                            }
                        } else {
                            throw new IllegalArgumentException
                                (errPrefix + "Expected file number: " +
                                 delim);
                        }
                    } else {
                        throw new IllegalArgumentException
                            (errPrefix + "Expected '-' or ',': " + delim);
                    }
                } else {
                    toNum = fromNum;
                }

                assert toNum != -1;
                list.add(Long.valueOf(fromNum));
                list.add(Long.valueOf(toNum));
            }

            forceCleanFiles = new long[list.size()];
            for (int i = 0; i < forceCleanFiles.length; i += 1) {
                forceCleanFiles[i] = list.get(i).longValue();
            }
        }
    }
}
