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

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
 * An OldBINDelta contains the information needed to create a partial (delta)
 * BIN log entry. It also knows how to combine a full BIN log entry and a delta
 * to generate a new BIN.
 *
 * An OldBINDelta is no longer written by this version of JE, but it may be
 * read from a log file written by earlier versions.
 */
public class OldBINDelta implements Loggable {

    private final DatabaseId dbId;    // owning db for this bin.
    private long lastFullLsn;   // location of last full version
    private long prevDeltaLsn;  // location of previous delta version
    private final ArrayList<DeltaInfo> deltas;  // list of key/action changes

    /**
     * For instantiating from the log.
     */
    public OldBINDelta() {
        dbId = new DatabaseId();
        lastFullLsn = DbLsn.NULL_LSN;
        prevDeltaLsn = DbLsn.NULL_LSN;
        deltas = new ArrayList<DeltaInfo>();
    }

    /**
     * For Sizeof.
     */
    public OldBINDelta(@SuppressWarnings("unused") SizeofMarker marker) {
        dbId = new DatabaseId();
        lastFullLsn = DbLsn.NULL_LSN;
        prevDeltaLsn = DbLsn.NULL_LSN;
        deltas = null; /* Computed separately. */
    }

    public DatabaseId getDbId() {
        return dbId;
    }

    public long getLastFullLsn() {
        return lastFullLsn;
    }

    /**
     * @return the prior delta version of this BIN, or NULL_LSN if the prior
     * version is a full BIN.  The returned value is the LSN that is obsoleted
     * by this delta.
     */
    public long getPrevDeltaLsn() {
        return prevDeltaLsn;
    }

    /**
     * Returns a key that can be used to find the BIN associated with this
     * delta.  The key of any slot will do.
     */
    public byte[] getSearchKey() {
        assert (deltas.size() > 0);
        return deltas.get(0).getKey();
    }

    /**
     * Create a BIN by fetching the full version and applying the deltas.
     */
    public BIN reconstituteBIN(DatabaseImpl dbImpl) {

        final EnvironmentImpl envImpl = dbImpl.getEnv();

        final BIN fullBIN = (BIN)
            envImpl.getLogManager().getEntryHandleFileNotFound(lastFullLsn);

        reconstituteBIN(dbImpl, fullBIN);

        return fullBIN;
    }

    /**
     * Given a full version BIN, apply the deltas.
     */
    public void reconstituteBIN(DatabaseImpl dbImpl, BIN fullBIN) {

        fullBIN.setDatabase(dbImpl);
        fullBIN.latch(CacheMode.UNCHANGED);
        try {

            /*
             * The BIN's lastFullLsn is set here, while its lastLoggedLsn is
             * set by postFetchInit or postRecoveryInit.
             */
            fullBIN.setLastFullLsn(lastFullLsn);

            /* Process each delta. */
            for (int i = 0; i < deltas.size(); i++) {
                final DeltaInfo info = deltas.get(i);
                fullBIN.applyDelta(
                    info.getKey(), null/*data*/, info.getLsn(),
                    info.getState(), 0 /*lastLoggedSize*/, 0 /*memId*/,
                    VLSN.NULL_VLSN_SEQUENCE, null /*child*/,
                    0 /*expiration*/, false /*expirationInHours*/);
            }

            /*
             * The applied deltas will leave some slots dirty, which is
             * necessary as a record of changes that will be included in the
             * next delta.  However, the BIN itself should not be dirty,
             * because this delta is a persistent record of those changes.
             */
            fullBIN.setDirty(false);
        } finally {
            fullBIN.releaseLatch();
        }
    }

    /*
     * Logging support
     */

    @Override
    public int getLogSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        throw new UnsupportedOperationException();
    }

    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        dbId.readFromLog(itemBuffer, entryVersion);
        lastFullLsn = LogUtils.readLong(itemBuffer, (entryVersion < 6));
        if (entryVersion >= 8) {
            prevDeltaLsn = LogUtils.readPackedLong(itemBuffer);
        }
        int numDeltas = LogUtils.readInt(itemBuffer, (entryVersion < 6));

        for (int i=0; i < numDeltas; i++) {
            DeltaInfo info = new DeltaInfo();
            info.readFromLog(itemBuffer, entryVersion);
            deltas.add(info);
        }

        /* Use minimum memory. */
        deltas.trimToSize();
    }

    public void dumpLog(StringBuilder sb, boolean verbose) {
        dbId.dumpLog(sb, verbose);
        sb.append("<lastFullLsn>");
        sb.append(DbLsn.getNoFormatString(lastFullLsn));
        sb.append("</lastFullLsn>");
        sb.append("<prevDeltaLsn>");
        sb.append(DbLsn.getNoFormatString(prevDeltaLsn));
        sb.append("</prevDeltaLsn>");
        sb.append("<deltas size=\"").append(deltas.size()).append("\"/>");
        for (int i = 0; i < deltas.size(); i++) {
            DeltaInfo info = deltas.get(i);
            info.dumpLog(sb, verbose);
        }
    }

    public long getTransactionId() {
        return 0;
    }

    /**
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * Returns the number of bytes occupied by this object.  Deltas are not
     * stored in the Btree, but they are budgeted during a SortedLSNTreeWalker
     * run.
     */
    public long getMemorySize() {
        long size = MemoryBudget.BINDELTA_OVERHEAD +
                    MemoryBudget.ARRAYLIST_OVERHEAD +
                    MemoryBudget.objectArraySize(deltas.size());
        for (DeltaInfo info : deltas) {
            size += info.getMemorySize();
        }
        return size;
    }
}
