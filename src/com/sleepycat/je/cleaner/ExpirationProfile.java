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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.bind.tuple.SortedPackedLongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.Put;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.StartupTracker;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;

/**
 * A cache of the histograms for all files, except for the last file. Also
 * caches the number of bytes expired in the current interval.
 *
 * No memory budgeting is performed because the size of these data structures
 * is small compared to the Btree they represent. The serialized form of the
 * histogram is cached, which is a small number of bytes per file. If no data
 * in a file expires, it will not have a cache entry.
 *
 * Possible future optimization: If there is contention on this data structure,
 * the refresh method could create a read-only map containing the current number
 * of expired bytes, for access by getExpiredBytes without synchronization.
 */
public class ExpirationProfile {

    private static Pair<Integer, Integer> PAIR_OF_ZEROS = new Pair<>(0, 0);

    private final EnvironmentImpl env;

    /*
     * The 'map' of file number to Histogram, protected by its own mutex.
     *
     * Note that if the map and completedTrackers mutexes are both held, they
     * must be acquired in that order.
     */
    private final Map<Long, ExpInfo> map;

    /*
     * The the expiration times in days and hours of last refresh, i.e, the
     * cached bytes in each Histogram are those that were expired on this
     * day/hour. Both fields are protected by the 'map' mutex.
     */
    private int lastRefreshHour = -1;
    private int lastRefreshDay = -1;

    /*
     * Whether any expiration times are in hours. If false, all intervals are
     * in days. Protected by the 'map' mutex.
     */
    private boolean anyExpirationInHours;

    /*
     * Map of file number to completed tracker. Protected by its own mutex.
     *
     * Note that if the map and completedTrackers mutexes are both held, they
     * must be acquired in that order.
     */
    private final Map<Long, ExpirationTracker> completedTrackers;

    /*
     * The expiration summary DB. Its key is the file number, a long. Its data
     * is the serialized form of the histogram, or is empty (zero length) if
     * the file has no expired data. The latter case includes files created
     * before the TTL feature was added.
     */
    private DatabaseImpl db;

    public ExpirationProfile(final EnvironmentImpl env) {
        this.env = env;
        map = new HashMap<>();
        completedTrackers = new HashMap<>();
    }

    /** Make a copy for used in utilities, etc. */
    public ExpirationProfile(final ExpirationProfile other) {
        env = other.env;
        db = other.db;
        synchronized (other.map) {
            map = new HashMap<>(other.map);
        }
        completedTrackers = Collections.emptyMap();
    }

    /**
     * Called at the end of recovery to open the expiration DB, and cache its
     * records in the profile's map.
     *
     * Also collects expiration info for any complete file having expiration
     * info that was not written to the DB earlier, due to a crash for example.
     *
     * Also initializes the tracker for the current file in the log manager,
     * reading/tracking the existing entries in that file.
     */
    public void populateCache(
        final StartupTracker.Counter counter,
        final ProgressListener<RecoveryProgress> listener) {

        synchronized (map) {

            assert db == null;
            assert completedTrackers.isEmpty();

            db = env.getDbTree().openNonRepInternalDB(DbType.EXPIRATION);

            if (db == null) {
                /* Read-only env with no expiration summary DB. */
                return;
            }

            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            /* Ordered array of file numbers. */
            final Long[] existingFiles =
                env.getFileManager().getAllFileNumbers();

            /* Parallel array to existingFiles. */
            final boolean[] filesHaveRecords =
                new boolean[existingFiles.length];

            /*
             * For the last file we must always get its expiration info and
             * then initialize the log manager's tracker. Note that its DB
             * record, if any, is deleted below.
             */
            final FileProcessor processor = env.getCleaner().createProcessor();
            final long lastFileNum = env.getFileManager().getCurrentFileNum();

            if (!env.isReadOnly()) {
                if (existingFiles.length > 0) {

                    /* Flush to ensure the cleaner can read all entries. */
                    env.flushLog(false /*fsync*/);

                    final ExpirationTracker tracker =
                        processor.countExpiration(lastFileNum);

                    env.getLogManager().initExpirationTracker(tracker);
                } else {
                    env.getLogManager().initExpirationTracker(
                        new ExpirationTracker(0));
                }
            }

            /*
             * Populate map with existing records in the DB that correspond to
             * existing files. Delete records in the DB that do not correspond
             * to existing files, to clean-up past errors. Also delete the
             * record for the last file, if it exists.
             */
            final Locker locker = BasicLocker.createBasicLocker(
                env, false /*noWait*/);

            try (final Cursor cursor =
                 DbInternal.makeCursor(db, locker, null)) {

                while (cursor.get(key, data, Get.NEXT, null) != null) {

                    counter.incNumRead();

                    final long fileNum =
                        SortedPackedLongBinding.entryToLong(key);

                    final int i = Arrays.binarySearch(existingFiles, fileNum);

                    if (i >= 0 && existingFiles[i] < lastFileNum) {

                        filesHaveRecords[i] = true;

                        final byte[] serializedForm = data.getData();

                        if (serializedForm.length > 0) {
                            counter.incNumProcessed();
                            map.put(
                                fileNum,
                                new ExpInfo(serializedForm, 0));
                        }
                    } else if (!env.isReadOnly()) {
                        counter.incNumDeleted();
                        cursor.delete();
                    }
                }
            } finally {
                locker.operationEnd();
            }

            /*
             * If a record is missing for an existing file, use the cleaner to
             * get the expiration info for the file, and then add a record to
             * the DB and to the map. Note that the last file is not processed.
             */
            for (int i = 0;
                 i < existingFiles.length && existingFiles[i] < lastFileNum;
                 i += 1) {

                if (filesHaveRecords[i]) {
                    continue;
                }

                final long fileNum = existingFiles[i];

                counter.incNumAux();

                if (listener != null) {
                    listener.progress(
                        RecoveryProgress.POPULATE_EXPIRATION_PROFILE,
                        1, -1);
                }

                final ExpirationTracker tracker =
                    processor.countExpiration(fileNum);

                putFile(tracker, 0);

                LoggerUtils.info(
                    env.getLogger(), env,
                    "Loaded missing expiration data from file 0x" +
                        Long.toHexString(fileNum));
            }
        }
    }

    /**
     * Writes a record in the expiration summary DB for the given tracker,
     * and (if there is any data with an expiration time) adds it to the map.
     *
     * Because this method and {@link #removeFile} perform Btree operations
     * while synchronized on the 'map', it is important that an IN is not
     * latched when calling these methods, which could cause a deadlock. Also,
     * an IN latch should not be held while calling a method that synchronizes
     * on {@link FileSelector}, since methods in this class are called while
     * synchronized on FileSelector, and this could cause a 3-way deadlock
     * [#25613].
     */
    void putFile(final ExpirationTracker tracker, final int expiredSize) {

        final long fileNum = tracker.getFileNum();
        final byte[] serializedForm = tracker.serialize();

        synchronized (map) {

            if (db != null && !env.isReadOnly()) {
                final DatabaseEntry key = new DatabaseEntry();
                final DatabaseEntry data = new DatabaseEntry();

                SortedPackedLongBinding.longToEntry(fileNum, key);

                data.setData(serializedForm);

                final Locker locker = BasicLocker.createBasicLocker(
                    env, false /*noWait*/);

                try (final Cursor cursor =
                         DbInternal.makeCursor(db, locker, null)) {

                    cursor.put(key, data, Put.OVERWRITE, null);

                } finally {
                    locker.operationEnd();
                }
            }

            if (serializedForm.length > 0) {
                map.put(
                    fileNum,
                    new ExpInfo(serializedForm, expiredSize));
            }
        }
    }

    /**
     * Remove entry for a file from the map and DB, when the file is deleted.
     */
    void removeFile(final long fileNum) {

        if (db == null || env.isReadOnly()) {
            return;
        }

        synchronized (map) {

            map.remove(fileNum);

            final DatabaseEntry key = new DatabaseEntry();
            SortedPackedLongBinding.longToEntry(fileNum, key);

            final Locker locker = BasicLocker.createBasicLocker(
                env, false /*noWait*/);

            try (final Cursor cursor =
                 DbInternal.makeCursor(db, locker, null)) {

                if (cursor.get(
                    key, null, Get.SEARCH,
                    LockMode.RMW.toReadOptions()) != null) {

                    cursor.delete(null);
                }
            } finally {
                locker.operationEnd();
            }
        }
    }

    /**
     * Called after a file flip. The tracker is completed in the sense that the
     * file is completely written, but there may be pending calls to
     * {@link ExpirationTracker#track(int, boolean, int)} for some writing
     * threads. This is because track is not called under the LWL.
     */
    public void addCompletedTracker(final ExpirationTracker tracker) {

        if (db == null) {
            return;
        }

        final long fileNum = tracker.getFileNum();

        synchronized (completedTrackers) {
            assert !completedTrackers.containsKey(fileNum);

            completedTrackers.put(fileNum, tracker);
        }
    }

    /**
     * Periodically, and when refreshing the profile, we process completed
     * trackers that were added to the completedTrackers queue at the time of a
     * file flip. If truly complete, they are added to the profile DB and map.
     */
    void processCompletedTrackers() {

        /* Only one thread at a time should process them. */
        synchronized (map) {

            /*
             * Make a copy in order to process them without holding their
             * mutex, to avoid blocking addCompletedTracker, which is in the
             * main write path.
             */
            final List<ExpirationTracker> trackers;

            synchronized (completedTrackers) {
                trackers = new ArrayList<>(completedTrackers.values());
            }

            for (final ExpirationTracker tracker : trackers) {

                if (tracker.hasPendingTrackCalls()) {
                    /* Not quite completed. */
                    continue;
                }

                putFile(tracker, 0);

                synchronized (completedTrackers) {
                    completedTrackers.remove(tracker.getFileNum());
                }
            }
        }
    }

    /**
     * Updates the expired bytes in the expiration profile according to the
     * data that has expired at the given time. Should be called periodically,
     * and before calling {@link #getExpiredBytes}.
     *
     * Also processes any completed trackers by adding them to the DB and to
     * the histogram map.
     *
     * This method only does any real work once per hour, on hour boundaries,
     * since data expires on (at most) hour boundaries.
     */
    public void refresh(final long time) {

        if (db == null) {
            return;
        }

        /* Synchronize to protect map and the lastRefreshXxx fields. */
        synchronized (map) {

            /*
             * Get last hour boundary, rounding down to the closest hour. If
             * an hour has not passed (and this is not the first time called),
             * then return and expect that we'll try again later.
             */
            final int hourLimit =
                (int) (time / TTL.MILLIS_PER_HOUR);

            if (hourLimit == lastRefreshHour) {
                return;
            }

            final int dayLimit = hourLimit / 24;
            final boolean newDayLimit = (dayLimit != lastRefreshDay);

            processCompletedTrackers();

            lastRefreshHour = hourLimit;
            lastRefreshDay = dayLimit;
            anyExpirationInHours = false;

            for (final ExpInfo info : map.values()) {
                if (ExpirationTracker.isExpirationInHours(
                    info.serializedForm)) {
                    anyExpirationInHours = true;
                    break;
                }
            }

            /*
             * If all expiration times are on day boundaries, and we have not
             * started a new day, there is nothing more to do.
             */
            if (!newDayLimit && !anyExpirationInHours) {
                return;
            }

            /*
             * Recalculate expired bytes for the current day/hour, saving the
             * previous value.
             */
            for (final ExpInfo info : map.values()) {

                info.previousExpiredBytes = info.currentExpiredBytes;

                info.currentExpiredBytes = ExpirationTracker.getExpiredBytes(
                    info.serializedForm, dayLimit, hourLimit);
            }
        }
    }

    /**
     * Returns the number of expired bytes for the given file. Uses the value
     * calculated by the last call to {@link #refresh}.
     */
    public int getExpiredBytes(final long fileNum) {
        synchronized (map) {
            final ExpInfo info = map.get(fileNum);
            return (info != null) ? info.currentExpiredBytes : 0;
        }
    }

    /**
     * Returns the number of expired bytes for the given file. Two values are
     * returned: the total expired at the given time, and a potentially smaller
     * amount that gradually expires over the current hour or day interval.
     * Uses the values calculated by the last call to {@link #refresh}.
     *
     * The amount that gradually expires is the amount that expires in the
     * current time interval, which is one day if all data in the profile
     * expires on day boundaries, and otherwise is one hour. If this is the
     * first interval for the file (after a restart or a revisal run), all
     * expired bytes are considered expired in the current interval and expire
     * gradually.
     *
     * @return pair of {allExpiredBytes, gradualExpiredBytes}.
     */
    public Pair<Integer, Integer> getExpiredBytes(
        final long fileNum,
        final long time) {

        synchronized (map) {

            final ExpInfo info = map.get(fileNum);

            if (info == null) {
                return PAIR_OF_ZEROS;
            }

            final int newlyExpiredBytes =
                info.currentExpiredBytes - info.previousExpiredBytes;

            if (newlyExpiredBytes == 0) {
                return new Pair<>(
                    info.currentExpiredBytes, info.currentExpiredBytes);
            }

            final long intervalMs = anyExpirationInHours ?
                TTL.MILLIS_PER_HOUR : TTL.MILLIS_PER_DAY;

            final long currentMs = time % intervalMs;

            final int gradualBytes = info.previousExpiredBytes +
                ((int) ((newlyExpiredBytes * currentMs) / intervalMs));

            return new Pair<>(info.currentExpiredBytes, gradualBytes);
        }
    }

    public String toString(final long fileNum) {
        synchronized (map) {
            final ExpInfo info = map.get(fileNum);
            return (info != null) ? info.toString() : "NoExpInfo";
        }
    }

    /**
     * Contains cached information about expiration for a data file.
     */
    private static class ExpInfo {

        /**
         * Cached serialized form, use to recompute the current expired bytes
         * for a given expiration time.
         */
        final byte[] serializedForm;

        /**
         * The number of expired bytes for the given file. This is the value
         * calculated by the last call to {@link ExpirationProfile#refresh}.
         */
        int currentExpiredBytes = 0;

        /**
         * The number of bytes that expired prior to the current interval.
         * Calculated by the previous refresh for which the interval changed.
         */
        int previousExpiredBytes = 0;

        ExpInfo(final byte[] serializedForm,
                final int currentExpiredBytes) {

            this.serializedForm = serializedForm;
            this.currentExpiredBytes = currentExpiredBytes;
        }

        @Override
        public String toString() {
            return "{ExpInfo currentBytes = " + currentExpiredBytes +
                " " + ExpirationTracker.toString(serializedForm) + '}';
        }
    }
}
