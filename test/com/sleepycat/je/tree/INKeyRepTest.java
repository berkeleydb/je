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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.PartialComparator;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.INKeyRep.Default;
import com.sleepycat.je.tree.INKeyRep.MaxKeySize;
import com.sleepycat.je.tree.INKeyRep.Type;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.VLSN;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class INKeyRepTest extends INEntryTestBase {

    /**
     * Permute tests over various compactMaxKeyLength sizes.
     */
    @Parameters
    public static List<Object[]> genParams() {
        
        return Arrays.asList(
            new Object[][]{{(short)5}, {(short)16}, {(short)202}});
    }
    
    public INKeyRepTest(short keyLength) {
        compactMaxKeyLength = keyLength;
        customName = ":maxLen=" + compactMaxKeyLength;
    }
    
    /**
     * Test use of the representations at the IN level. Checks memory
     * bookkeeping after each operation.
     */
    @Test
    public void testINBasic() {

        int keySize = compactMaxKeyLength / 2;

        Database db = createDb(DB_NAME, keySize, nodeMaxEntries);

        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        EnvironmentImpl env = dbImpl.getEnv();

        boolean embeddedLNs = (env.getMaxEmbeddedLN() >= keySize);

        if (embeddedLNs) {
            verifyAcrossINEvict(db, Type.DEFAULT, Type.DEFAULT);
        } else {
            verifyAcrossINEvict(db, Type.DEFAULT, Type.MAX_KEY_SIZE);
        }

        db.close();

        /* Ensure that default value constants are kept in sync. */
        assertEquals
            (String.valueOf(INKeyRep.MaxKeySize.DEFAULT_MAX_KEY_LENGTH),
             EnvironmentParams.TREE_COMPACT_MAX_KEY_LENGTH.getDefault());
    }

    @Test
    public void testDINEvict() {

        int keySize = compactMaxKeyLength / 2;

        Database db = createDupDb(DB_NAME, keySize, nodeMaxEntries);

        verifyAcrossINEvict(db, Type.MAX_KEY_SIZE, Type.MAX_KEY_SIZE);

        db.close();
    }

    private BIN verifyAcrossINEvict(Database db,
                                    Type pre,
                                    Type post) {

        DatabaseImpl dbImpl = DbInternal.getDbImpl(db);

        BIN firstBin = (BIN)(dbImpl.getTree().getFirstNode(cacheMode));
        assertEquals(pre, firstBin.getKeyVals().getType());

        firstBin.evictLNs();
        firstBin.releaseLatch();
        assertEquals(post, firstBin.getKeyVals().getType());

        verifyINMemorySize(dbImpl);
        return firstBin;
    }

    @Test
    public void testINMutate() {

       commonINMutate(false);
    }

    @Test
    public void testINMutatePrefix() {
        commonINMutate(true);
    }

    public void commonINMutate(boolean prefixKeys) {

        final int keySize = compactMaxKeyLength / 2;

        final Database db = createDb(DB_NAME, keySize, nodeMaxEntries-1,
                                     prefixKeys);
        final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
        EnvironmentImpl env = dbImpl.getEnv();

        boolean embeddedLNs = (env.getMaxEmbeddedLN() >= keySize);

        BIN bin = (BIN)(dbImpl.getTree().getFirstNode(cacheMode));
        bin.evictLNs();

        if (embeddedLNs) {
            assertEquals(Type.DEFAULT, bin.getKeyVals().getType());
        } else {
            assertEquals(Type.MAX_KEY_SIZE, bin.getKeyVals().getType());
        }

        bin.releaseLatch();

        DatabaseEntry key = new DatabaseEntry();
        key.setData(createByteVal(nodeMaxEntries, keySize+1));
        db.put(null, key, key);

        verifyINMemorySize(dbImpl);
        assertEquals(Type.DEFAULT, bin.getKeyVals().getType());

        db.close();
    }

    @Test
    public void testBasic() {
        final int size = 32;
        final IN parent = new TestIN(size);
        commonTest(parent, new Default(size));
        commonTest(parent, new MaxKeySize(size,
                            (short) Math.max(1, (compactMaxKeyLength - 9))));
    }

    public void commonTest(IN parent, INKeyRep targets) {
        targets = targets.set(1, new byte[]{1}, parent);
        assertEquals(1, targets.get(1)[0]);

        targets.copy(0, 5, 1, parent);
        assertEquals(1, targets.get(1)[0]);

        targets.copy(0, 5, 2, parent);
        assertEquals(1, targets.get(6)[0]);

        targets.set(1, null, parent);

        assertEquals(null, targets.get(1));

        targets.copy(5, 0, 2, null);
        assertEquals(1, targets.get(1)[0]);
    }

    @Test
    public void testDefaultKeyVals() {
        final int size = 128;
        final IN parent = new TestIN(size);
        Default defrep = new Default(size);
        byte[][] refEntries = initRep(parent, defrep);
        checkEquals(refEntries, defrep);
    }

    @Test
    public void testMaxKeyVals() {
        final int size = 128;
        final IN parent = new TestIN(size);
        MaxKeySize defrep = new MaxKeySize(size, compactMaxKeyLength);
        byte[][] refEntries = initRep(parent, defrep);
        checkEquals(refEntries, defrep);
    }

    @Test
    public void testMaxKeyMutation() {
        final int size = 32;
        final IN parent = new TestIN(size);
        INKeyRep defrep = new MaxKeySize(size, compactMaxKeyLength);
        initRep(parent, defrep);

        /* No mutation on null. */
        defrep = defrep.set(0, null, parent);
        assertEquals(Type.MAX_KEY_SIZE, defrep.getType());

        /* No mutation on change */
        defrep = defrep.set(0, new byte[0], parent);
        assertEquals(Type.MAX_KEY_SIZE, defrep.getType());

        /* Mutate on large key. */
        defrep = defrep.set(0, new byte[compactMaxKeyLength+1], parent);
        assertEquals(Type.DEFAULT, defrep.getType());
    }

    @Test
    public void testRampUp() {

        /* The sizes here only work for CompressedOops */
        Assume.assumeTrue(!JVMSystemUtils.ZING_JVM);

        final int size = 128;
        final IN parent = new TestIN(size);
        byte refEntries[][] = new byte[size][];
        INKeyRep defrep = new Default(size);

        for (int i=0; i < defrep.length(); i++) {

            int keyLength = Math.max(4, i % compactMaxKeyLength);

            ByteBuffer byteBuffer = ByteBuffer.allocate(keyLength);
            byteBuffer.putInt(i);

            defrep.set(i, byteBuffer.array(), parent);

            refEntries[i] = byteBuffer.array();

            checkEquals(refEntries, defrep);

            defrep = defrep.compact(parent);

            checkEquals(refEntries, defrep);
        }

        if (compactMaxKeyLength < 20) {
            /* Should have transitioned as a result of the compaction. */
            assertEquals(Type.MAX_KEY_SIZE, defrep.getType());
        } else {
            /*
             * With compactMaxKeyLength == 202, the java mem overhead of
             * storing each key as a separate byte array is smaller than
             * using 202 bytes for all 128 keys.
             */
            assertEquals(Type.DEFAULT, defrep.getType());
        }
    }

    @Test
    public void testShiftEntries() {
        int size = 128;
        final IN parent = new TestIN(size);
        commonShiftEntries(parent, new Default(size));
        commonShiftEntries(parent, new MaxKeySize(size, (short)8));
    }

    public void commonShiftEntries(IN parent, INKeyRep entries) {
        int size = entries.length();
        byte refEntries[][] = new byte[size][];

        Random rand = new Random();

        for (int i = 0; i < 10000; i++) {
            int slot = rand.nextInt(size);
            byte[] n = (i % 10) == 0 ? null : createByteVal(slot, 8);
            refEntries[slot] = n;
            entries = entries.set(slot, n, parent);
            checkEquals(refEntries, entries);

            /* Simulate an insertion */
            entries = entries.copy(slot, slot + 1, size - (slot + 1), parent);
            System.arraycopy(refEntries, slot, refEntries, slot + 1,
                             size - (slot + 1));
            checkEquals(refEntries, entries);

            /* Simulate a deletion. */
            entries = entries.copy(slot + 1, slot, size - (slot + 1), parent);
            entries = entries.set(size-1, null, parent);
            System.arraycopy(refEntries, slot + 1, refEntries,
                             slot, size - (slot + 1));
            refEntries[size-1] = null;
            checkEquals(refEntries, entries);
        }
    }

    @Test
    public void testKeySizeChange_IN_updateEntry() {

        commonKeySizeChange(
            new ChangeKey() {
                public void changeKey(final BIN bin,
                                      final int index,
                                      byte[] newKey) {
                    bin.insertRecord(
                        index, (LN)bin.getTarget(index), bin.getLsn(index),
                        bin.getLastLoggedSize(index), newKey, null,
                        0 /*expiration*/, false /*expirationInHours*/);
                }
            });
    }

    @Test
    public void testKeySizeChange_IN_updateNode1() {

        commonKeySizeChange(
            new ChangeKey() {
                public void changeKey(final BIN bin,
                                      final int index,
                                      byte[] newKey) {
                long lnMemSize = 0;
                if (!bin.isEmbeddedLN(index)) {
                    LN ln = bin.fetchLN(index, CacheMode.UNCHANGED);
                    lnMemSize = ln.getMemorySizeIncludedByParent();
                    assertEquals(Type.MAX_KEY_SIZE, bin.getKeyVals().getType());
                } else {
                    assertEquals(Type.DEFAULT, bin.getKeyVals().getType());
                }

                bin.updateRecord(
                    index, lnMemSize, bin.getLsn(index),
                    VLSN.NULL_VLSN_SEQUENCE,
                    bin.getLastLoggedSize(index), newKey, null,
                    0 /*expiration*/, false /*expirationInHours*/);
            }
        });
    }

    @Test
    public void testKeySizeChange_IN_updateNode2() {

        commonKeySizeChange(
            new ChangeKey() {
                public void changeKey(final BIN bin,
                                      final int index,
                                      byte[] newKey) {

                if (!bin.isEmbeddedLN(index)) {
                    LN ln = bin.fetchLN(index, CacheMode.UNCHANGED);
                    bin.detachNode(index, false, -1);
                    assertEquals(Type.MAX_KEY_SIZE, bin.getKeyVals().getType());
                    bin.attachNode(index, ln, newKey);
                } else {
                    bin.convertKey(index, newKey);
                }
            }
        });
    }

    @Test
    public void testKeySizeChange_IN_updateNode3() {

        commonKeySizeChange(
            new ChangeKey() {
                public void changeKey(final BIN bin,
                                      final int index,
                                      byte[] newKey) {
                    LN ln = null;
                    if (!bin.isEmbeddedLN(index)) {
                        ln = bin.fetchLN(index, CacheMode.UNCHANGED);
                        assertEquals(Type.MAX_KEY_SIZE, bin.getKeyVals().getType());
                    }

                    bin.insertRecord(
                        index, ln, bin.getLsn(index),
                        bin.getLastLoggedSize(index), newKey, null,
                        0 /*expiration*/, false /*expirationInHours*/);
                }
            });
    }

    interface ChangeKey {
        void changeKey(final BIN bin, final int index, byte[] newKey);
    }

    /**
     * Force key size changes using internal IN methods, to ensure that memory
     * budgeting is correct.  [#19295]
     *
     * Although the key size for an existing IN slot normally doesn't change,
     * it can change when a "partial key comparator" is used, which compares
     * a subset of the complete key.  In addition, conversion to the new dup
     * format requires changing keys arbitrarily.
     */
    private void commonKeySizeChange(final ChangeKey changeKey) {

        final int keySize = compactMaxKeyLength / 2;

        for (int testCase = 0; testCase < 1; testCase += 1) {

            final Database db = createDb(DB_NAME, keySize, nodeMaxEntries);
            final DatabaseImpl dbImpl = DbInternal.getDbImpl(db);
            EnvironmentImpl env = dbImpl.getEnv();
            
            boolean embeddedLNs = (env.getMaxEmbeddedLN() >= keySize);

            /*
             * A non-null custom comparator is necessary to test the IN.setNode
             * family of methods, which update the key only if a partial
             * comparator may be configured.
             */
            dbImpl.setBtreeComparator(new StandardComparator(), true);

            /* Mutate BIN to MAX_KEY_SIZE rep using eviction. */
            if (embeddedLNs) {
                verifyAcrossINEvict(db, Type.DEFAULT, Type.DEFAULT);
            } else {
                verifyAcrossINEvict(db, Type.DEFAULT, Type.MAX_KEY_SIZE);
            }

            /* Manufacture new key with one extra byte. */
            final BIN bin = (BIN)(dbImpl.getTree().getFirstNode(cacheMode));
            final int index = nodeMaxEntries / 2;
            final byte[] oldKey = bin.getKey(index);
            final byte[] newKey = new byte[oldKey.length + 1];
            System.arraycopy(oldKey, 0, newKey, 0, oldKey.length);

            /*
             * Test changing size of BIN slot key using various IN methods.
             * The rep should mutate to DEFAULT because the key size increased.
             */
            changeKey.changeKey(bin, index, newKey);
            assertEquals(Type.DEFAULT, bin.getKeyVals().getType());
            bin.releaseLatch();

            /* Prior to the fix for [#19295], the memory check failed. */
            verifyINMemorySize(dbImpl);

            db.close();
        }
    }

    @Test
    public void testCompareKeys() {
        final int size = 32;
        final IN parent = new TestIN(size);

        for (final boolean embeddedData : new boolean[] {false, true}) {

            for (int cmp = 0; cmp < 2; cmp += 1) {

                final Comparator<byte[]> comparator =
                    (cmp == 0) ? null : new StandardComparator();

                for (final INKeyRep targets :
                     new INKeyRep[] {new Default(size),
                                     new MaxKeySize(size, (short) 8)}) {

                    compareKeysWithoutPrefixes(
                        parent, targets, embeddedData, comparator);

                    compareKeysWithPrefixes(
                        parent, targets, embeddedData, comparator);

                }
            }
        }
    }

    public void compareKeysWithoutPrefixes(IN parent,
                                           INKeyRep targets,
                                           boolean embeddedData,
                                           Comparator<byte[]> comparator) {

        final byte[] data = embeddedData ? new byte[] {1, 2} : null;

        final byte[] k12 = new byte[]{1, 2};
        final byte[] k123 = new byte[]{1, 2, 3};
        final byte[] k34 = new byte[]{3, 4};
        final byte[] k345 = new byte[]{3, 4, 5};

        final int i123 = 0;
        final int i345 = 1;

        targets = targets.set(i123, k123, data, parent);
        targets = targets.set(i345, k345, data, parent);

        int cmp;

        cmp = targets.compareKeys(k12, null, i123, embeddedData, comparator);
        assertTrue(cmp < 0);
        cmp = targets.compareKeys(k12, null, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k123, null, i123, embeddedData, comparator);
        assertTrue(cmp == 0);
        cmp = targets.compareKeys(k123, null, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k34, null, i123, embeddedData, comparator);
        assertTrue(cmp > 0);
        cmp = targets.compareKeys(k34, null, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k345, null, i123, embeddedData, comparator);
        assertTrue(cmp > 0);
        cmp = targets.compareKeys(k345, null, i345, embeddedData, comparator);
        assertTrue(cmp == 0);
    }

    public void compareKeysWithPrefixes(IN parent,
                                        INKeyRep targets,
                                        boolean embeddedData,
                                        Comparator<byte[]> comparator) {

        final byte[] data = embeddedData ? new byte[] {1, 2} : null;

        final byte[] k0 = new byte[]{0};
        final byte[] k00 = new byte[]{0, 0};
        final byte[] k0012 = new byte[]{0, 0, 1, 2};
        final byte[] k00123 = new byte[]{0, 0, 1, 2, 3};
        final byte[] k0034 = new byte[]{0, 0, 3, 4};
        final byte[] k00345 = new byte[]{0, 0, 3, 4, 5};

        final byte[] k123 = new byte[]{1, 2, 3};
        final byte[] k345 = new byte[]{3, 4, 5};

        final int i123 = 0;
        final int i345 = 1;

        targets = targets.set(i123, k123, data, parent);
        targets = targets.set(i345, k345, data, parent);

        int cmp;

        cmp = targets.compareKeys(k0012, k00, i123, embeddedData, comparator);
        assertTrue(cmp < 0);
        cmp = targets.compareKeys(k0012, k00, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k00123, k00, i123, embeddedData, comparator);
        assertTrue(cmp == 0);
        cmp = targets.compareKeys(k00123, k00, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k0034, k00, i123, embeddedData, comparator);
        assertTrue(cmp > 0);
        cmp = targets.compareKeys(k0034, k00, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k00345, k00, i123, embeddedData, comparator);
        assertTrue(cmp > 0);
        cmp = targets.compareKeys(k00345, k00, i345, embeddedData, comparator);
        assertTrue(cmp == 0);

        /*
         * Special case where search key is smaller than full key in the slot,
         * and the search key is contained in the prefix. This failed from JE
         * 6.2.41 to 6.4.11 [#24583].
         */
        cmp = targets.compareKeys(k0, k00, i123, embeddedData, comparator);
        assertTrue(cmp < 0);
        cmp = targets.compareKeys(k0, k00, i345, embeddedData, comparator);
        assertTrue(cmp < 0);

        cmp = targets.compareKeys(k00, k00, i123, embeddedData, comparator);
        assertTrue(cmp < 0);
        cmp = targets.compareKeys(k00, k00, i345, embeddedData, comparator);
        assertTrue(cmp < 0);
    }

    private class StandardComparator
        implements Comparator<byte[]>, PartialComparator, Serializable {

        public int compare(final byte[] k1, final byte[] k2) {
            return Key.compareKeys(k1, k2, null);
        }
    }

    private void checkEquals(byte[][] refEntries, INKeyRep entries) {
        for (int i=0; i < refEntries.length; i++) {
            Arrays.equals(refEntries[i], entries.get(i));
        }
    }

    private byte[][] initRep(IN parent, INKeyRep rep) {
        int size = rep.length();
        byte[][] refEntries = new byte[size][];
        for (int i = 0; i < rep.length(); i++) {
            int keyLength = Math.max(4, i % compactMaxKeyLength);
            ByteBuffer byteBuffer = ByteBuffer.allocate(keyLength);
            byteBuffer.putInt(i);
            INKeyRep nrep = rep.set(i, byteBuffer.array(), parent);
            assertTrue(rep == nrep);
            refEntries[i] = byteBuffer.array();
            checkEquals(refEntries, rep);
        }
        return refEntries;
    }
}
