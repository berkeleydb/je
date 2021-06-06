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

package com.sleepycat.je.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Put;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.util.test.TxnTestCase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests multi-key secondary operations.  Exhaustive API testing of multi-key
 * secondaries is part of SecondaryTest and ForeignKeyTest, which test the use
 * of a single key with SecondaryMultiKeyCreator.  This class adds tests for
 * multiple keys per record.
 */
@RunWith(Parameterized.class)
public class ToManyTest extends TxnTestCase {

    private static final Function<Byte, Set<Byte>> NEW_SET =
        (k -> new HashSet<>());

    /*
     * The primary database has a single byte key and byte[] array data.  Each
     * byte of the data array is a secondary key in the to-many index.
     *
     * The primary map mirrors the primary database and contains Byte keys and
     * a set of Byte objects for each map entry value.  The secondary map
     * mirrors the secondary database, and for every secondary key (Byte)
     * contains a set of primary keys (set of Byte).
     */
    private Map<Byte, Set<Byte>> priMap0 = new HashMap<>();
    private Map<Byte, Set<Byte>> secMap0 = new HashMap<>();
    private Database priDb;
    private SecondaryDatabase secDb;

    @Parameters
    public static List<Object[]> genParams() {
       
        /*
         * This test does not work with TXN_NULL because with transactions we
         * cannot abort the update in a one-to-many test when secondary key
         * already exists in another primary record.
         */
        return getTxnParams(
            new String[] {TxnTestCase.TXN_USER, TxnTestCase.TXN_AUTO}, false);
    }
    
    public ToManyTest(String type){
        initEnvConfig();
        txnType = type;
        isTransactional = (txnType != TXN_NULL);
        customName = txnType;
    }

    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        priMap0 = null;
        secMap0 = null;
        priDb = null;
        secDb = null;
    }

    @Test
    public void testManyToMany()
        throws DatabaseException {

        priDb = openPrimary("pri");
        secDb = openSecondary(priDb, "sec", true /*dups*/);

        writeAndVerify((byte) 0, new byte[] {});
        writeAndVerify((byte) 0, null);
        writeAndVerify((byte) 0, new byte[] {0, 1, 2});
        writeAndVerify((byte) 0, null);
        writeAndVerify((byte) 0, new byte[] {});
        writeAndVerify((byte) 0, new byte[] {0});
        writeAndVerify((byte) 0, new byte[] {0, 1});
        writeAndVerify((byte) 0, new byte[] {0, 1, 2});
        writeAndVerify((byte) 0, new byte[] {1, 2});
        writeAndVerify((byte) 0, new byte[] {2});
        writeAndVerify((byte) 0, new byte[] {});
        writeAndVerify((byte) 0, null);

        writeAndVerify((byte) 0, new byte[] {0, 1, 2});
        writeAndVerify((byte) 1, new byte[] {1, 2, 3});
        writeAndVerify((byte) 0, null);
        writeAndVerify((byte) 1, null);
        writeAndVerify((byte) 0, new byte[] {0, 1, 2});
        writeAndVerify((byte) 1, new byte[] {1, 2, 3});
        writeAndVerify((byte) 0, new byte[] {0});
        writeAndVerify((byte) 1, new byte[] {3});
        writeAndVerify((byte) 0, null);
        writeAndVerify((byte) 1, null);

        secDb.close();
        priDb.close();
    }

    @Test
    public void testOneToMany()
        throws DatabaseException {

        priDb = openPrimary("pri");
        secDb = openSecondary(priDb, "sec", false /*dups*/);

        writeAndVerify((byte) 0, new byte[] {1, 5});
        writeAndVerify((byte) 1, new byte[] {2, 4});
        writeAndVerify((byte) 0, new byte[] {0, 1, 5, 6});
        writeAndVerify((byte) 1, new byte[] {2, 3, 4});
        write((byte) 0, new byte[] {3}, true /*expectException*/);
        writeAndVerify((byte) 1, new byte[] {});
        writeAndVerify((byte) 0, new byte[] {0, 1, 2, 3, 4, 5, 6});
        writeAndVerify((byte) 0, null);
        writeAndVerify((byte) 1, new byte[] {0, 1, 2, 3, 4, 5, 6});
        writeAndVerify((byte) 1, null);

        secDb.close();
        priDb.close();
    }

    /**
     * Puts or deletes a single primary record, updates the maps, and verifies
     * that the maps match the databases.
     */
    private void writeAndVerify(byte priKey, byte[] priData)
        throws DatabaseException {

        int nWrites = write(priKey, priData, false /*expectException*/);
        updateMaps(priKey, bytesToSet(priData), nWrites);
        verify();
    }

    /**
     * Puts or deletes a single primary record.
     *
     * @return n of secondary writes.
     */
    private int write(byte priKey, byte[] priData, boolean expectException)
        throws DatabaseException {

        DatabaseEntry keyEntry = new DatabaseEntry(new byte[] { priKey });
        DatabaseEntry dataEntry = new DatabaseEntry(priData);

        Transaction txn = txnBeginCursor();
        try {
            int nWrites;
            try (Cursor c = priDb.openCursor(txn, null)) {
                OperationResult r;
                if (priData != null) {
                    r = c.put(keyEntry, dataEntry, Put.OVERWRITE, null);
                } else {
                    r = c.get(keyEntry, null, Get.SEARCH, null);
                    assertNotNull(r);
                    r = c.delete(null);
                }
                assertNotNull(r);
                nWrites = DbInternal.getCursorImpl(c).getNSecondaryWrites();
            }
            txnCommit(txn);
            assertTrue(!expectException);
            return nWrites;
        } catch (Exception e) {
            txnAbort(txn);
            assertTrue(e.toString(), expectException);
            return 0;
        }
    }

    /**
     * Updates map 0 to reflect a record added to the primary database.
     *
     * @param nWrites number of secondary records inserted/deleted, which
     * should equal the number of map changes.
     */
    private void updateMaps(Byte priKey, Set<Byte> newPriData, int nWrites) {

        int nChanges = 0;

        /* Remove old secondary keys. */
        Set<Byte> oldPriData = priMap0.get(priKey);
        if (oldPriData != null) {
            for (Byte secKey : oldPriData) {
                if (newPriData != null && newPriData.contains(secKey))  {
                    continue;
                }
                Set<Byte> priKeySet = secMap0.get(secKey);
                assertNotNull(priKeySet);
                assertTrue(priKeySet.remove(priKey));
                nChanges += 1;
                if (priKeySet.isEmpty()) {
                    secMap0.remove(secKey);
                }
            }
        }

        if (newPriData != null) {
            /* Put primary entry. */
            priMap0.put(priKey, newPriData);
            /* Add new secondary keys. */
            for (Byte secKey : newPriData) {
                if (oldPriData != null && oldPriData.contains(secKey))  {
                    continue;
                }
                Set<Byte> priKeySet =
                    secMap0.computeIfAbsent(secKey, NEW_SET);
                assertTrue(priKeySet.add(priKey));
                nChanges += 1;
            }
        } else {
            /* Remove primary entry. */
            priMap0.remove(priKey);
        }

        assertEquals(nChanges, nWrites);
    }

    /**
     * Verifies that the maps match the databases.
     */
    private void verify()
        throws DatabaseException {

        Transaction txn = txnBeginCursor();

        DatabaseEntry priKeyEntry = new DatabaseEntry();
        DatabaseEntry secKeyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        Map<Byte, Set<Byte>> priMap1 = new HashMap<>();
        Map<Byte, Set<Byte>> priMap2 = new HashMap<>();
        Map<Byte, Set<Byte>> secMap1 = new HashMap<>();
        Map<Byte, Set<Byte>> secMap2 = new HashMap<>();

        /* Build map 1 from the primary database. */
        Cursor priCursor = priDb.openCursor(txn, null);

        while (priCursor.getNext(priKeyEntry, dataEntry, null) ==
               OperationStatus.SUCCESS) {

            Byte priKey = priKeyEntry.getData()[0];
            Set<Byte> priData = bytesToSet(dataEntry.getData());

            /* Update primary map. */
            priMap1.put(priKey, priData);

            /* Update secondary map. */
            for (Byte secKey : priData) {

                Set<Byte> priKeySet =
                    secMap1.computeIfAbsent(secKey, NEW_SET);

                assertTrue(priKeySet.add(priKey));
            }

            /*
             * Add empty primary records to priMap2 while we're here, since
             * they cannot be built from the secondary database.
             */
            if (priData.isEmpty()) {
                priMap2.put(priKey, priData);
            }
        }
        priCursor.close();

        /* Build map 2 from the secondary database. */
        SecondaryCursor secCursor = secDb.openSecondaryCursor(txn, null);

        while (secCursor.getNext(secKeyEntry, priKeyEntry, dataEntry, null) ==
               OperationStatus.SUCCESS) {

            Byte priKey = priKeyEntry.getData()[0];
            Byte secKey = secKeyEntry.getData()[0];

            /* Update primary map. */
            Set<Byte> priData = priMap2.computeIfAbsent(priKey, NEW_SET);
            priData.add(secKey);

            /* Update secondary map. */
            Set<Byte> secData = secMap2.computeIfAbsent(secKey, NEW_SET);
            secData.add(priKey);
        }

        secCursor.close();

        /* Compare. */
        assertEquals(priMap0, priMap1);
        assertEquals(priMap1, priMap2);
        assertEquals(secMap0, secMap1);
        assertEquals(secMap1, secMap2);

        txnCommit(txn);
    }

    private Set<Byte> bytesToSet(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        Set<Byte> set = new HashSet<>();
        for (byte b : bytes) {
            set.add(b);
        }
        return set;
    }

    private Database openPrimary(String name)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);

        Transaction txn = txnBegin();
        try {
            return env.openDatabase(txn, name, dbConfig);
        } finally {
            txnCommit(txn);
        }
    }

    private SecondaryDatabase openSecondary(Database priDb,
                                            String dbName,
                                            boolean dups)
        throws DatabaseException {

        SecondaryConfig dbConfig = new SecondaryConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(dups);
        dbConfig.setMultiKeyCreator(new MyKeyCreator());

        Transaction txn = txnBegin();
        try {
            return env.openSecondaryDatabase(txn, dbName, priDb, dbConfig);
        } finally {
            txnCommit(txn);
        }
    }

    private static class MyKeyCreator implements SecondaryMultiKeyCreator {

        public void createSecondaryKeys(SecondaryDatabase secondary,
                                        DatabaseEntry key,
                                        DatabaseEntry data,
                                        Set<DatabaseEntry> results) {
            for (int i = 0; i < data.getSize(); i+= 1) {
                results.add(new DatabaseEntry(data.getData(), i, 1));
            }
        }
    }
}
