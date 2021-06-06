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

package com.sleepycat.je.txn;

import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.utilint.DbLsn;

/**
 * LockResult is the return type of Locker.lock(). It encapsulates a
 * LockGrantType (the return type of LockManager.lock()) and a WriteLockInfo.
 * 
 * The WriteLockInfo field is non-null if (a) the locker is transactional, and
 * (b) the request was for a WRITE or WRITE_RANGE lock, and (c) the request was
 * not a non-blocking request that got denied. If so, the WriteLockInfo is
 * either a newly created one or a pre-existing one if the same locker had
 * write-locked the same LSN before. 
 */
public class LockResult {
    private LockGrantType grant;
    private WriteLockInfo wli;

    /* Made public for unittests */
    public LockResult(LockGrantType grant, WriteLockInfo wli) {
        this.grant = grant;
        this.wli = wli;
    }

    public LockGrantType getLockGrant() {
        return grant;
    }

    public WriteLockInfo getWriteLockInfo() {
        return wli;
    }

    /*
     * Method called from CursorImpl.LockStanding.prepareForUpdate()
     */
    public void setAbortInfo(
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        DatabaseImpl db) {

        /*
         * Do not overwrite abort info if this locker has logged the
         * associated record before.
         */
        if (wli != null && wli.getNeverLocked()) {
            if (abortLsn != DbLsn.NULL_LSN) {
                wli.setAbortLsn(abortLsn);
                wli.setAbortKnownDeleted(abortKD);
                wli.setAbortKey(abortKey);
                wli.setAbortData(abortData);
                wli.setAbortVLSN(abortVLSN);
                wli.setAbortExpiration(abortExpiration, abortExpirationInHours);
                wli.setDb(db);
            }
            wli.setNeverLocked(false);
        }
    }

    /**
     * Used to copy write lock info when an LSN is changed.
     */
    public void copyWriteLockInfo(WriteLockInfo fromInfo) {
        if (fromInfo != null && wli != null) {
            wli.copyAllInfo(fromInfo);
            wli.setNeverLocked(false);
        }
    }
}
