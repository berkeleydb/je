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

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
  * VersionedLN is used to  provide an in-memory representation of an LN that
  * makes its VLSN available through btree access.
  *
  * On disk, each log entry is composed of a header (je.log.LogEntryHeader) and
  * a body (je.log.entry.LogEntry). When an LN is materialized in the Btree, it
  * usually holds only the body, and does not have access to information in the
  * log entry header, such as the VLSN. Since version based API operations need
  * access to the VLSN, environments which are configured with
  * je.rep.preserveRecordVersion=true instantiate VersionedLNs instead of LNs,
  * in order to cache the VLSN with the LN, and make it cheaply available to
  * Btree operations. 
  */
public class VersionedLN extends LN {

    private long vlsnSequence = VLSN.NULL_VLSN_SEQUENCE;

    public VersionedLN() {
    }

    VersionedLN(byte[] data) {
        super(data);
    }

    VersionedLN(DatabaseEntry dbt) {
        super(dbt);
    }

    /** For Sizeof. */
    public VersionedLN(SizeofMarker marker, DatabaseEntry dbt) {
        super(dbt);
    }

    @Override
    public long getVLSNSequence() {
        return vlsnSequence;
    }

    @Override
    public void setVLSNSequence(long seq) {
        vlsnSequence = seq;
    }

    /**
     * Add additional size taken by this LN subclass.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        return super.getMemorySizeIncludedByParent() +
               MemoryBudget.VERSIONEDLN_OVERHEAD;
    }
}
