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

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.tree.OldBINDelta;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Before log version 9, this was used to hold a OldBINDelta that can be combined
 * with a BIN when fetched from the log; see getResolvedItem.  This class was
 * replaced by BINDeltaLogEntry in log version 9, which can be used to
 * create a live (but incomplete) BIN in the Btree.
 */
public class OldBINDeltaLogEntry extends SingleItemEntry<OldBINDelta>
    implements INContainingEntry {

    public OldBINDeltaLogEntry(Class<OldBINDelta> logClass) {
        super(logClass);
    }

    /*
     * Whether this LogEntry reads/writes a BIN-Delta logrec.
     */
    @Override
    public boolean isBINDelta() {
        return true;
    }

    /**
     * Resolve a BIN-delta item by fetching the full BIN and merging the delta.
     */
    @Override
    public Object getResolvedItem(DatabaseImpl dbImpl) {
        return getIN(dbImpl);
    }

    @Override
    public IN getIN(DatabaseImpl dbImpl) {
        OldBINDelta delta = getMainItem();
        return delta.reconstituteBIN(dbImpl);
    }

    @Override
    public DatabaseId getDbId() {
        OldBINDelta delta = getMainItem();
        return delta.getDbId();        
    }

    @Override
    public long getPrevFullLsn() {
        OldBINDelta delta = getMainItem();
        return delta.getLastFullLsn();
    }

    @Override
    public long getPrevDeltaLsn() {
        OldBINDelta delta = getMainItem();
        return delta.getPrevDeltaLsn();        
    }
}
