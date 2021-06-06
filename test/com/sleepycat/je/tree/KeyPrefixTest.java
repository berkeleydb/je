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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.utilint.StringUtils;

public class KeyPrefixTest extends DualTestCase {

    private final File envHome;
    private Environment env;
    private Database db;

    public KeyPrefixTest() {
        System.setProperty("longAckTimeout", "true");
        envHome = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        db = null;
        env = null;
    }

    private void initEnv(int nodeMax)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        if (nodeMax > 0) {
            envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
                                     Integer.toString(nodeMax));
        }
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setTxnNoSync(true);
        env = create(envHome, envConfig);

        String databaseName = "testDb";
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setKeyPrefixing(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, databaseName, dbConfig);
    }

    private void closeEnv() {
        try {
            db.close();
            close(env);
            db = null;
            env = null;
        } catch (DatabaseException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static final String[] keys = {
        "aaa", "aab", "aac", "aae",                // BIN1
        "aaf", "aag", "aah", "aaj",                // BIN2
        "aak", "aala", "aalb", "aam",              // BIN3
        "aan", "aao", "aap", "aas",                // BIN4
        "aat", "aau", "aav", "aaz",                // BIN5
        "baa", "bab", "bac", "bam",                // BIN6
        "ban", "bax", "bay", "baz",                // BIN7
        "caa", "cab", "cay", "caz",                // BIN8
        "daa", "eaa", "faa", "fzz",                // BIN10
        "Aaza", "Aazb", "aal", "aama"
    };

    @Test
    public void testPrefixBasic()
        throws Exception {

        initEnv(5);
        Key.DUMP_TYPE = DumpType.TEXT;
        try {

            /* Build up a tree. */
            for (String key : keys) {
                assertEquals(OperationStatus.SUCCESS,
                             db.put(null,
                                    new DatabaseEntry
                                    (StringUtils.toUTF8(key)),
                                    new DatabaseEntry(new byte[] { 1 })));
            }

            String[] sortedKeys = new String[keys.length];
            System.arraycopy(keys, 0, sortedKeys, 0, keys.length);
            Arrays.sort(sortedKeys);

            Transaction txn = env.beginTransaction(null, null);
            Cursor cursor = null;
            int i = 0;
            try {
                cursor = db.openCursor(txn, CursorConfig.READ_UNCOMMITTED);
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();

                boolean somePrefixSeen = false;
                while (cursor.getNext(key, data, LockMode.DEFAULT) ==
                       OperationStatus.SUCCESS) {
                    assertEquals(StringUtils.fromUTF8(key.getData()),
                                 sortedKeys[i++]);
                    byte[] prefix =
                        DbInternal.getCursorImpl(cursor).getBIN().
                        getKeyPrefix();
                    if (prefix != null) {
                        somePrefixSeen = true;
                    }
                }
                assertTrue(somePrefixSeen);
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
                txn.commit();
            }

            if (false) {
                System.out.println("<dump>");
                DbInternal.getDbImpl(db).getTree().dump();
            }

            closeEnv();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }

    @Test
    public void testPrefixManyRandom()
        throws Exception {

        doTestPrefixMany(true);
    }

    @Test
    public void testPrefixManySequential()
        throws Exception {

        doTestPrefixMany(false);
    }

    private void doTestPrefixMany(boolean random)
        throws Exception {

        initEnv(0);
        final int N_EXTRA_ENTRIES = 1000;
        Key.DUMP_TYPE = DumpType.BINARY;
        try {

            /* 2008-02-28 11:06:50.009 */
            long start = 1204214810009L;

            /* 2 years after start. Prefixes will be 3 and 4 bytes long. */
            long end = start + (2L * 365L * 24L * 60L * 60L * 1000L);

            /* This will yield 94,608 entries. */
            long inc = 1000000L;
            int nEntries = insertTimestamps(start, end, inc, random);

            /*
             * This will force some splits on the left side of the tree which
             * will force recalculating the suffix on the leg after the initial
             * prefix/suffix calculation.
             */
            insertExtraTimestamps(0, N_EXTRA_ENTRIES);

            /* Do the same on the right side of the tree. */
            insertExtraTimestamps(end, N_EXTRA_ENTRIES);
            assertEquals((nEntries + 2 * N_EXTRA_ENTRIES), db.count());

            Transaction txn = env.beginTransaction(null, null);
            Cursor cursor = null;
            try {
                cursor = db.openCursor(txn, CursorConfig.READ_UNCOMMITTED);

                verifyEntries(0, N_EXTRA_ENTRIES, cursor, 1);
                verifyEntries(start, nEntries, cursor, inc);
                verifyEntries(end, N_EXTRA_ENTRIES, cursor, 1);

                deleteEntries(txn, start, nEntries);
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                cursor.close();
                cursor = db.openCursor(txn, CursorConfig.READ_UNCOMMITTED);
                verifyEntries(0, N_EXTRA_ENTRIES, cursor, 1);
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getNext(key, data, LockMode.DEFAULT));
                assertEquals(end, LongBinding.entryToLong(key));
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
                txn.commit();
            }

            if (false) {
                System.out.println("<dump>");
                DbInternal.getDbImpl(db).getTree().dump();
            }

            closeEnv();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new Exception(t);
        }
    }

    private int insertTimestamps(long start,
                                 long end,
                                 long inc,
                                 boolean random)
        throws DatabaseException {

        int nEntries = (int) ((end - start) / inc);
        List<Long> keyList = new ArrayList<Long>(nEntries);
        long[] keys = null;
        if (random) {
            for (long i = start; i < end; i += inc) {
                keyList.add(i);
            }
            keys = new long[keyList.size()];
            Random rnd = new Random(10); // fixed seed
            int nextKeyIdx = 0;
            while (keyList.size() > 0) {
                int idx = rnd.nextInt(keyList.size());
                keys[nextKeyIdx++] = keyList.get(idx);
                keyList.remove(idx);
            }
        }

        /* Build up a tree. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        data.setData(new byte[1]);
        int j = 0;
        for (long i = start; i < end; i += inc) {
            if (random) {
                LongBinding.longToEntry(keys[j], key);
            } else {
                LongBinding.longToEntry(i, key);
            }
            j++;
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, key, data));
        }
        return j;
    }

    private void insertExtraTimestamps(long start, int nExtraEntries)
        throws DatabaseException {

        /* Add (more than one node's worth) to the left side of the tree.*/
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry(new byte[] { 0 });
        long next = start;
        for (int i = 0; i < nExtraEntries; i++) {
            LongBinding.longToEntry(next, key);
            assertEquals(OperationStatus.SUCCESS,
                         db.put(null, key, data));
            next++;
        }
    }

    private void deleteEntries(Transaction txn, long start, int nEntries)
        throws DatabaseException {

        /*
         * READ_UNCOMMITTED is used here as a trick to reduce the amount of
         * memory taken by locks.  Because we don't lock the record before
         * deleting it, we don't lock the LSN, which reduces the number of
         * locks by half.
         */
        Cursor cursor = db.openCursor(txn, null);
        try {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            LongBinding.longToEntry(start, key);
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getSearchKey(key, data,
                                             LockMode.READ_UNCOMMITTED));
            for (int i = 0; i < nEntries; i++) {
                assertEquals(OperationStatus.SUCCESS, cursor.delete());
                assertEquals(OperationStatus.SUCCESS,
                             cursor.getNext(key, data,
                                            LockMode.READ_UNCOMMITTED));
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    private void verifyEntries(long start,
                               int nEntries,
                               Cursor cursor,
                               long inc)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        long check = start;
        for (int i = 0; i < nEntries; i++) {
            assertEquals(OperationStatus.SUCCESS,
                         cursor.getNext(key, data, LockMode.DEFAULT));
            long keyInfo = LongBinding.entryToLong(key);
            assertTrue(keyInfo == check);
            check += inc;
        }
    }

    /**
     * Tests use of an RLE based comparator function
     */
    @Test
    public void testRLEComparator() {
        initEnv(0);
        db.close();
        db =  env.openDatabase
            (null, "testKeyComparator",
             configDatabaseWithComparator(new RLEKeyComparator()));
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        final int keyCount = 1000;

        /* seed the database. */
        Random rand = new Random(0);

        for (int i=0; i < keyCount; i++) {
            String str = Long.toString(rand.nextLong());
            final byte[] keyBytes = strToRLEbytes(str);

            key.setData(keyBytes);
            data.setData(key.getData());
            OperationStatus status = db.put(null, key, data);
            assertSame(OperationStatus.SUCCESS, status);

            status = db.get(null, key, data, LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
        }

        /* Ensure bin prefixes are computed. */
        Transaction txn = env.beginTransaction(null, null);
        Cursor cursor = null;
        try {
            cursor = db.openCursor(txn, CursorConfig.READ_UNCOMMITTED);
            BIN pbin = null;
            while (cursor.getNext(key, data, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {

                final BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
                if (bin != pbin) {
                    bin.latch();
                    bin.recalcKeyPrefix();
                    bin.releaseLatch();
                    pbin = bin;
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            txn.commit();
        }

        /* Verify that expected keys are all present in the database. */
        rand = new Random(0);
        for (int i=0; i < keyCount; i++) {
            String str = Long.toString(rand.nextLong());

            final byte[] keyBytes = strToRLEbytes(str);

            key.setData(keyBytes);
            OperationStatus status = db.get(null, key, data, LockMode.DEFAULT);
            assertSame(OperationStatus.SUCCESS, status);
        }

        closeEnv();
    }

    static public String rleBytesToStr(byte encodedBytes[]) {
        ByteBuffer bb = ByteBuffer.wrap(encodedBytes);
        StringBuffer sb = new StringBuffer();

        while (bb.remaining() > 0){
            int length = bb.getInt();
            final char c = bb.getChar();
            while (length-- > 0) {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    static public byte[] strToRLEbytes(String str) {
        ByteBuffer bb = ByteBuffer.allocate(1000);
        for (int i = 0; i < str.length(); i++) {
            int length = 1;
            while ((i+1) < str.length() && str.charAt(i) == str.charAt(i+1)) {
                length++;
                i++;
            }
            bb.putInt(length);
            bb.putChar(str.charAt(i));
        }
        byte encodedBytes[] = new byte[bb.position()];
        bb.rewind();
        bb.get(encodedBytes);
        return encodedBytes;
    }

    static class RLEKeyComparator
        implements Comparator<byte[]>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(byte[] k1, byte[] k2) {
            String s1 = rleBytesToStr(k1);
            String s2 = rleBytesToStr(k2);
            return s1.compareTo(s2);
        }
    }

    private DatabaseConfig configDatabaseWithComparator
        (Comparator<byte[]> testKeyComparator) {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setKeyPrefixing(true);
        dbConfig.setBtreeComparator(testKeyComparator);
        return dbConfig;
    }

    /**
     * Tests key prefixing with a key comparator that causes keys to be sorted
     * such that the first and last keys in a range do not represent the first
     * last values if instead they were sorted byte-by-byte.  For example,
     * string keys might be stored with a preceding 1 byte integer length:
     * <pre>
     *  2, "aa"
     *  2, "bb"
     *  4, "cccc"
     *  2, "dd"
     * </pre>
     * If prefixing were performed by only considering the first and last key
     * in the range, the first byte would be in common and used as the prefix.
     * But this prefix would be incorrect for the middle key.
     * <p>
     * The test uses these four keys, rather than just three, because the old
     * (incorrect) code checked the first two keys and the last keys to
     * determine the prefix.  In this test case, the first byte has the same
     * value (2) for these three keys.
     * <p>
     * [#21405]
     */
    @Test
    public void testLeadingLengthKeys() {

        initEnv(0);
        db.close();
        db = null;

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setKeyPrefixing(true);
        dbConfig.setBtreeComparator(new LeadingLengthKeyComparator());
        db = env.openDatabase(null, "testKeyComparator", dbConfig);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Insert leading-length keys. */
        for (String s : new String[] { "aa", "bb", "cccc", "dd" }) {
            key.setData(getLeadingLengthKeyBytes(s));
            data.setData(key.getData());
            OperationStatus status = db.put(null, key, data);
            assertSame(OperationStatus.SUCCESS, status);
        }

        /* Force prefixing. */
        Cursor cursor = db.openCursor(null, null);
        OperationStatus status = cursor.getFirst(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);

        BIN bin = DbInternal.getCursorImpl(cursor).getBIN();
        bin.latch();
        bin.recalcKeyPrefix();
        bin.releaseLatch();

        /* Check keys. */
        status = cursor.getFirst(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals("aa", getLeadingLengthKeyString(key));
        status = cursor.getNext(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals("bb", getLeadingLengthKeyString(key));
        status = cursor.getNext(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals("cccc", getLeadingLengthKeyString(key));
        status = cursor.getNext(key, data, null);
        assertSame(OperationStatus.SUCCESS, status);
        assertEquals("dd", getLeadingLengthKeyString(key));
        status = cursor.getNext(key, data, null);
        assertSame(OperationStatus.NOTFOUND, status);

        cursor.close();
        closeEnv();
    }

    static byte[] getLeadingLengthKeyBytes(String s) {
        assertTrue(s.length() < Byte.MAX_VALUE);
        byte[] b = new byte[1 + s.length()];
        b[0] = (byte) s.length();
        try {
            System.arraycopy(s.getBytes("US-ASCII"), 0, b, 1, s.length());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return b;
    }

    static String getLeadingLengthKeyString(DatabaseEntry entry) {
        return getLeadingLengthKeyString(entry.getData());
    }

    static String getLeadingLengthKeyString(byte[] b) {
        int len = b[0];
        try {
            return new String(b, 1, len, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    static class LeadingLengthKeyComparator
        implements Comparator<byte[]>, Serializable {

        public int compare(byte[] k1, byte[] k2) {
            String s1 = getLeadingLengthKeyString(k1);
            String s2 = getLeadingLengthKeyString(k2);
            return s1.compareTo(s2);
        }
    }
}
