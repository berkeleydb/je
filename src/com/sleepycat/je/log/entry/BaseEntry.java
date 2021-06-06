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

package com.sleepycat.je.log.entry;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.VLSN;

/**
 * A Log entry allows you to read, write and dump a database log entry.  Each
 * entry may be made up of one or more loggable items.
 *
 * The log entry on disk consists of
 *  a. a log header defined by LogManager
 *  b. a VLSN, if this entry type requires it, and replication is on.
 *  c. the specific contents of the log entry.
 *
 * This class encompasses (b and c).
 *
 * @param <T> the type of the loggable items in this entry
 */
abstract class BaseEntry<T extends Loggable> implements LogEntry {

    /*
     * These fields are transient and are not persisted to the log
     */

    /*
     * Constructor used to create log entries when reading.
     */
    private final Constructor<T> noArgsConstructor;

    /*
     * Attributes of the entry type may be used to conditionalizing the reading
     * and writing of the entry.
     */
    LogEntryType entryType;

    /**
     * Constructor to read an entry. The logEntryType must be set later,
     * through setLogType().
     *
     * @param logClass the class for the contained loggable item or items
     */
    BaseEntry(Class<T> logClass) {
        noArgsConstructor = getNoArgsConstructor(logClass);
    }

    static <T extends Loggable> Constructor<T> getNoArgsConstructor(
        final Class<T> logClass) {
        try {
            return logClass.getConstructor((Class<T>[]) null);
        } catch (SecurityException | NoSuchMethodException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * @return a new instance of the class used to create the log entry.
     */
    T newInstanceOfType() {
        return newInstanceOfType(noArgsConstructor);
    }

    static <T extends Loggable> T newInstanceOfType(
        final Constructor<T> noArgsConstructor) {
        try {
            return noArgsConstructor.newInstance((Object[]) null);
        } catch (IllegalAccessException | InstantiationException |
                IllegalArgumentException | InvocationTargetException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Constructor to write an entry.
     */
    BaseEntry() {
        noArgsConstructor = null;
    }

    /**
     * Returns the class of the contained loggable item or items, or null if
     * the instance was created to write an entry.
     *
     * @return the loggable class or null
     */
    public Class<T> getLogClass() {
        return (noArgsConstructor != null) ?
            noArgsConstructor.getDeclaringClass() :
            null;
    }

    /**
     * Inform a BaseEntry instance of its corresponding LogEntryType.
     */
    @Override
    public void setLogType(LogEntryType entryType) {
        this.entryType = entryType;
    }

    @Override
    public LogEntryType getLogType() {
        return entryType;
    }

    /**
     * By default, this log entry is complete and does not require fetching
     * additional entries.  This method is overridden by BINDeltaLogEntry.
     */
    @Override
    public Object getResolvedItem(DatabaseImpl dbImpl) {
        return getMainItem();
    }

    @Override
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl) {
        return false;
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     * @throws DatabaseException from subclasses.
     */
    @Override
    public void postLogWork(@SuppressWarnings("unused") LogEntryHeader header,
                            @SuppressWarnings("unused") long justLoggedLsn,
                            @SuppressWarnings("unused") VLSN vlsn) {

        /* by default, do nothing. */
    }

    public void postFetchInit(@SuppressWarnings("unused")
                              DatabaseImpl dbImpl) {
    }

    @Override
    public LogEntry clone() {

        try {
            return (LogEntry) super.clone();
        } catch (CloneNotSupportedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        dumpEntry(sb, true);
        return sb.toString();
    }
}
