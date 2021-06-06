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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * - INLogEntry is used to read/write full-version IN logrecs.
 *
 * - BINDeltaLogEntry subclasses INLogEntry and is used to read/write
 *   BIN-delta logrecs for log versions 9 or later.
 *
 * - OldBINDeltaLogEntry is used to read/write BIN-delta logrecs for
 *   log versions earlier than 9. OldBINDeltaLogEntry is not a subclass
 *   of INLogEntry.
 *
 * On disk, a full IN logrec contains:
 * 
 * <pre>
 * (3 <= version < 6)
 *        IN
 *        database id
 *        prevFullLsn  -- in version 2
 *
 * (6 <= version < 8)
 *        database id
 *        prevFullLsn
 *        IN
 *
 * (8 <= version)
 *        database id
 *        prevFullLsn
 *        prevDeltaLsn
 *        IN
 * </pre>
 *
 *  On disk, a BIN-delta logrec written via the BINDeltaLogEntry contains:
 *
 * <pre>
 * (version == 9)
 *        database id
 *        prevFullLsn  -- always NULL
 *        prevDeltaLsn
 *        BIN (dirty slots only)
 *        prevFullLsn
 *
 * (version >= 10)
 *        database id
 *        prevFullLsn
 *        prevDeltaLsn
 *        BIN (dirty slots only and including the new fullBinNEntries and
 *             fullBinMaxEntries fields) 
 * </pre>
 *
 */
public class INLogEntry<T extends IN> extends BaseEntry<T>
    implements LogEntry, INContainingEntry {

    /*
     * Persistent fields in an IN entry.
     */

    private DatabaseId dbId;

    /*
     * this.in may be a (a) UIN, (b) full BIN, or (c) BIN delta.
     * In case (a), "this" is a INLogEntry
     * In case (c), "this" is a BINDeltaLogEntry instance.
     * In case (b), "this" may be either a INLogEntry or a BINDeltaLogEntry
     * instance. It will be a BINDeltaLogEntry instance, if "this" is used
     * to log a full in-memory BIN as a BIN-delta.
     */
    private T in;

    /**
     * If non-null, used to write a pre-serialized log entry. In this case the
     * 'in' field is null.
     */
    private ByteBuffer inBytes;

    /*
     * The lsn of the previous full-version logrec for the same IN.
     *
     * See comment above about the evolution of this field.
     */
    private long prevFullLsn;

    /*
     * If this is a BIN logrec and the previous logrec for the same BIN was
     * a BIN-delta, prevDeltaLsn is the lsn of that previous logrec. Otherwise,
     * prevDeltaLsn is NULL.
     *
     * See comment above about the evolution of this field.
     */
    private long prevDeltaLsn;

    /**
     * Construct a log entry for reading.
     */
    public static <T extends IN> INLogEntry<T> create(final Class<T> INClass) {
        return new INLogEntry<T>(INClass);
    }

    INLogEntry(Class<T> INClass) {
        super(INClass);
    }

    /**
     * Construct an INLogEntry for writing to the log.
     */
    public INLogEntry(T in) {
        this(in, false /*isBINDelta*/);
    }

    /*
     * Used by both INLogEntry and BINDeltaLogEntry for writing to the log.
     */
    INLogEntry(T in, boolean isBINDelta) {

        setLogType(isBINDelta ? LogEntryType.LOG_BIN_DELTA : in.getLogType());

        dbId = in.getDatabase().getId();

        this.in = in;
        inBytes = null;

        prevFullLsn = in.getLastFullLsn();
        prevDeltaLsn = in.getLastDeltaLsn();
    }

    /**
     * Used to write a pre-serialized log entry.
     */
    public INLogEntry(final ByteBuffer bytes,
                      final long lastFullLsn,
                      final long lastDeltaLsn,
                      final LogEntryType logEntryType,
                      final IN parent) {

        setLogType(logEntryType);

        dbId = parent.getDatabase().getId();

        in = null;
        inBytes = bytes;

        prevFullLsn = lastFullLsn;
        prevDeltaLsn = lastDeltaLsn;
    }

    /*
     * Whether this LogEntry reads/writes a BIN-Delta logrec.
     * Overriden by the BINDeltaLogEntry subclass.
     */
    @Override
    public boolean isBINDelta() {
        return false;
    }

    @Override
    public DatabaseId getDbId() {
        return dbId;
    }

    @Override
    public long getPrevFullLsn() {
        return prevFullLsn;
    }

    @Override
    public long getPrevDeltaLsn() {
        return prevDeltaLsn;
    }

    @Override
    public T getMainItem() {
        assert inBytes == null;

        return in;
    }

    @Override
    public IN getIN(DatabaseImpl dbImpl) {
        assert inBytes == null;

        return in;
    }

    public long getNodeId() {
        assert inBytes == null;

        return in.getNodeId();
    }

    public boolean isPreSerialized() {
        return inBytes != null;
    }

    /**
     * Returns the main item BIN if it has any slots with expiration times.
     * Must only be called if this entry's type is BIN or BIN_DELTA.
     *
     * This method is called for expiration tracking because getMainItem and
     * getIN cannot be called on an INLogEntry logging parameter, since it may
     * be in pre-serialize form when it appears in the off-heap cache.
     */
    public BIN getBINWithExpiration() {

        if (inBytes != null) {
            final BIN bin = new BIN();
            if (!bin.mayHaveExpirationValues(
                inBytes, LogEntryType.LOG_VERSION)) {
                return null;
            }
            inBytes.mark();
            readMainItem((T) bin, inBytes, LogEntryType.LOG_VERSION);
            inBytes.reset();
            return bin.hasExpirationValues() ? bin : null;
        }

        assert in.isBIN();
        final BIN bin = (BIN) in;
        return bin.hasExpirationValues() ? bin : null;
    }

    /*
     * Read support
     */

    @Override
    public void readEntry(
        EnvironmentImpl envImpl,
        LogEntryHeader header,
        ByteBuffer entryBuffer) {

        assert inBytes == null;

        int logVersion = header.getVersion();
        boolean version6OrLater = (logVersion >= 6);

        if (logVersion < 2) {
            throw unexpectedState(
                "Attempt to read from log file with version " +
                logVersion + ", which is not supported any more");
        }

        if (version6OrLater) {
            dbId = new DatabaseId();
            dbId.readFromLog(entryBuffer, logVersion);

            prevFullLsn = LogUtils.readLong(entryBuffer, false/*unpacked*/);
            if (logVersion >= 8) {
                prevDeltaLsn = LogUtils.readPackedLong(entryBuffer);
            } else {
                prevDeltaLsn = DbLsn.NULL_LSN;
            }
        }

        /* Read IN. */
        in = newInstanceOfType();
        readMainItem(in, entryBuffer, logVersion);

        if (!version6OrLater) {
            dbId = new DatabaseId();
            dbId.readFromLog(entryBuffer, logVersion);

            prevFullLsn = LogUtils.readLong(entryBuffer, true/*unpacked*/);
            prevDeltaLsn = DbLsn.NULL_LSN;
        }
    }

    private void readMainItem(T in, ByteBuffer entryBuffer, int logVersion) {

        if (isBINDelta()) {
            assert(logVersion >= 9);

            in.readFromLog(
                entryBuffer, logVersion, true /*deltasOnly*/);

            if (logVersion == 9) {
                prevFullLsn = LogUtils.readPackedLong(entryBuffer);
            }

            in.setLastFullLsn(prevFullLsn);

        } else {
            in.readFromLog(entryBuffer, logVersion);
        }
    }

    /*
     * Writing support
     */
    @Override
    public int getSize() {

        final int inSize;

        if (inBytes != null) {
            inSize = inBytes.remaining();
        } else {
            inSize = in.getLogSize(isBINDelta());
        }

        return (inSize +
                dbId.getLogSize() +
                LogUtils.getPackedLongLogSize(prevFullLsn) +
                LogUtils.getPackedLongLogSize(prevDeltaLsn));
    }

    @Override
    public void writeEntry(ByteBuffer destBuffer) {

        dbId.writeToLog(destBuffer);

        LogUtils.writePackedLong(destBuffer, prevFullLsn);
        LogUtils.writePackedLong(destBuffer, prevDeltaLsn);

        if (inBytes != null) {
            final int pos = inBytes.position();
            destBuffer.put(inBytes);
            inBytes.position(pos);
        } else {
            in.writeToLog(destBuffer, isBINDelta());
        }
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    /**
     * INs from two different environments are never considered equal,
     * because they have lsns that are environment-specific.
     */
    @Override
    public boolean logicalEquals(@SuppressWarnings("unused") LogEntry other) {
        return false;
    }

    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {

        dbId.dumpLog(sb, verbose);

        if (inBytes != null) {
            sb.append("<INBytes len=\"");
            sb.append(inBytes.remaining());
            sb.append("\"/>");
        } else {
            in.dumpLog(sb, verbose);
        }

        if (prevFullLsn != DbLsn.NULL_LSN) {
            sb.append("<prevFullLsn>");
            sb.append(DbLsn.getNoFormatString(prevFullLsn));
            sb.append("</prevFullLsn>");
        }
        if (prevDeltaLsn != DbLsn.NULL_LSN) {
            sb.append("<prevDeltaLsn>");
            sb.append(DbLsn.getNoFormatString(prevDeltaLsn));
            sb.append("</prevDeltaLsn>");
        }
        return sb;
    }

    /** Never replicated. */
    public void dumpRep(@SuppressWarnings("unused") StringBuilder sb) {
    }
}
