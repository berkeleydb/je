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

/**
 * Contains static methods for estimating record storage size.
 *
 * Currently this only applies to KVS because we assume that VLSNs are
 * preserved.
 */
public class StorageSize {

    /*
     * Maximum size of the per-LN overhead.
     *
     * The overhead is variable and depends on several factors, see
     * LNLogEntry.getSize(). The following cases are considered:
     *
     *  25: cleaned and migrated LN (no txn info), no TTL:
     *      22: header (type, checksum, flags, prevOffset, size, vlsn)
     *      2: data length
     *      1: flags
     *
     *  43: insertion, with TTL:
     *      25: same as above
     *      2: expiration
     *      8: txnId
     *      8: lastLoggedLsn
     *
     *  53: update, with TTL:
     *      43: same as above
     *      8: abortLsn
     *      2: abortExpiration
     *
     * 50 is used as a conservative estimate for LN_OVERHEAD. Updates will be
     * relatively infrequent.
     */
    private final static int LN_OVERHEAD = 50;

    /*
     * Maximum size of the per-slot overhead.
     *
     * The overhead is variable and depends on several factors, see
     * IN.getLogSize. The following cases are considered:
     *
     *  11: Minimum for all cases
     *      8: lsn
     *      1: keySize
     *      1: state
     *      1: expiration
     *
     *  12: Secondary DB, with TTL
     *      11: minimum above
     *      1: data size
     *
     *  13: Separate LN in primary DB, with TTL
     *      11: minimum above
     *      2: lastLoggedSize
     *
     *  20: Embedded LN in primary DB, with TTL
     *      11: minimum above
     *      1: data size
     *      8: vlsn
     *
     * 12 is used for SEC_SLOT_OVERHEAD as a conservative estimate.
     *
     * 14 is used for PRI_SLOT_OVERHEAD and in the customer formula for both
     * the separate LN and embedded LN cases. The slot overhead for the
     * embedded case will be larger, but in that case there are significant
     * savings because the primary key is not duplicated.
     */
    private final static int SEC_SLOT_OVERHEAD = 12;
    private final static int PRI_SLOT_OVERHEAD = 14;
    private final static int PRI_EMBEDDED_LN_SLOT_OVERHEAD = 20;

    /* Static methods only. */
    private StorageSize() {}

    /**
     * Returns the estimated disk storage size for the record in the given BIN
     * slot. This method does not fetch the LN.
     * <p>
     * For KVS, a formula that customers will use to predict the storage for a
     * given set of records, not including obsolete size (size available for
     * reclamation by the cleaner), is as follows.
     * <p>
     * The storage overhead for a single Row (JE primary record) is:
     * <pre>
     *  Serialized size of the Row, all fields (JE key + data size)
     *    +
     *  Serialized size of the PrimaryKey fields (JE key size)
     *    +
     *  Fixed per-Row internal overhead (64: LN_OVERHEAD + PRI_SLOT_OVERHEAD)
     * </pre>
     *
     * The storage overhead for an Index record is:
     * <pre>
     *  Serialized size of the IndexKey fields (JE key size)
     *    +
     *  Serialized size of the PrimaryKey fields (JE data size)
     *    +
     *  Fixed per-IndexKey internal overhead (12: SEC_SLOT_OVERHEAD)
     * </pre>
     *
     * This method returns the size estimate for an actual record based on the
     * use of that formula, getting the key and data size (or lastLoggedSize)
     * from the BIN. The amount calculated using the formula above will
     * normally be larger than the size returned by this method, for several
     * reasons:
     * <ul>
     *   <li>
     *   This method uses the key size after it is reduced by prefix
     *   compression.
     *   </li>
     *   <li>
     *   For a separate (non-embedded) LN, this method uses the lastLoggedSize
     *   rather than adding LN_OVERHEAD to the data size (this is why
     *   LN_OVERHEAD is not referenced in code here). This is more accurate
     *   since the actual LN overhead is reduced due to integer packing, etc.
     *   Also, this method cannot fetch the LN, so the data size is unknown.
     *   </li>
     *   <li>
     *   For an embedded LN in a primary DB, the returned  size does not
     *   include the LN size, since the LN is always obsolete. This means the
     *   primary key size is not counted redundantly and the LN_OVERHEAD is not
     *   included in the return value, as they are in the formula. These are
     *   significant differences, but since embedded LNs require a data size
     *   LTE 16, this is not expected to be a common use case. If it becomes
     *   common, we should add a new case for this to the customer formula.
     *   </li>
     * </ul>
     *
     * In addition, the size returned by this method will normally be larger
     * than the actual storage size on disk. This is because this method uses
     * PRI_SLOT_OVERHEAD and SEC_SLOT_OVERHEAD to calculate the Btree slot
     * space, rather than using the serialized size of the slot. These constant
     * values are somewhat larger than the actual overheads, since they do not
     * take into account integer packing, etc. See the comments above these
     * constants. The serialized slot size was not used here for simplicity and
     * speed, plus this additional size compensates for uncounted sizes such as
     * per-BIN and UIN overhead.
     *
     * @return the estimated storage size, or zero when the size is unknown
     * because a non-embedded LN is not resident and the LN was logged with a
     * JE version prior to 6.0.
     */
    public static int getStorageSize(final BIN bin, final int idx) {

        final int storedKeySize = bin.getStoredKeySize(idx);

        /*
         * For a JE secondary DB record (KVS Index record), return:
         *
         *   data-size + key-size + SEC_SLOT_OVERHEAD
         *
         *    where data-size is serialized IndexKey size
         *    and key-size is serialized PrimaryKey size.
         *
         * The storedKeySize includes key-size, data-size, and one extra byte
         * for data (primary key) size. We subtract it here because it is
         * included in SEC_SLOT_OVERHEAD.
         */
        if (bin.getDatabase().getSortedDuplicates()) {
            return storedKeySize - 1 + SEC_SLOT_OVERHEAD;
        }

        /*
         * For an embedded-LN JE primary DB record (KVS Row):
         *
         *   Return data-size + key-size + PRI_SLOT_OVERHEAD
         *
         *    where (data-size + key-size) is serialized Row size
         *    and key-size is serialized PrimaryKey size
         *
         * The storedKeySize includes key-size, data-size, and one extra byte
         * for data (primary key) size. We subtract it here because it is
         * included in PRI_EMBEDDED_LN_SLOT_OVERHEAD.
         */
        if (bin.isEmbeddedLN(idx)) {
            return storedKeySize - 1 + PRI_EMBEDDED_LN_SLOT_OVERHEAD;
        }

        /*
         * For a separate (non-embedded) JE primary DB record (KVS Row):
         *
         *   Return LN-log-size + key-size + PRI_SLOT_OVERHEAD
         *
         *    where LN-log-size is LN_OVERHEAD (or less) + data-size + key-size
         *    and (data-size + key-size) is serialized Row size
         *    and key-size is serialized PrimaryKey size
         *
         * The storedKeySize is the key-size alone.
         */
        final int lastLoggedSize = bin.getLastLoggedSize(idx);
        if (lastLoggedSize == 0) {
            /* Size is unknown. */
            return 0;
        }
        return lastLoggedSize + storedKeySize + PRI_SLOT_OVERHEAD;
    }
}
