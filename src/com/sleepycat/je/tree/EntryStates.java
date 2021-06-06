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

public class EntryStates {

    /*
     * If we run out of bits, the two OFFHEAP bits could re-use any of the bit
     * values, since the former only appear on level 2 INs and the latter only
     * on BINs.
     */
    static final byte KNOWN_DELETED_BIT = 0x1;
    static final byte CLEAR_KNOWN_DELETED_BIT = ~0x1;
    static final byte DIRTY_BIT = 0x2;
    static final byte CLEAR_DIRTY_BIT = ~0x2;
    static final byte OFFHEAP_DIRTY_BIT = 0x4;
    static final byte CLEAR_OFFHEAP_DIRTY_BIT = ~0x4;
    static final byte PENDING_DELETED_BIT = 0x8;
    static final byte CLEAR_PENDING_DELETED_BIT = ~0x8;
    static final byte EMBEDDED_LN_BIT = 0x10;
    static final byte CLEAR_EMBEDDED_LN_BIT = ~0x10;
    static final byte NO_DATA_LN_BIT = 0x20;
    static final byte CLEAR_NO_DATA_LN_BIT = ~0x20;
    static final byte OFFHEAP_PRI2_BIT = 0x40;
    static final byte CLEAR_OFFHEAP_PRI2_BIT = ~0x40;

    static final byte TRANSIENT_BITS = OFFHEAP_DIRTY_BIT | OFFHEAP_PRI2_BIT;
    static final byte CLEAR_TRANSIENT_BITS = ~TRANSIENT_BITS;
}
