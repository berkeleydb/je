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

package com.sleepycat.je.rep.util.ldiff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class LDiffTest extends TestBase {
    private final File envRoot;
    private final File envDir1;
    private final File envDir2;
    private static final String dbName = "ldiff.db";

    public LDiffTest() {
        envRoot = SharedTestUtils.getTestDir();
        envDir1 = new File(envRoot, "env1");
        envDir2 = new File(envRoot, "env2");
    }

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        LDiffTestUtils.deleteDir(envDir1);
        envDir1.mkdir();

        LDiffTestUtils.deleteDir(envDir2);
        envDir2.mkdir();
    }

    @After
    public void tearDown() {
        LDiffTestUtils.deleteDir(envDir1);
        LDiffTestUtils.deleteDir(envDir2);
    }

    /**
     * Two environments with identical dbs should be equal.
     */
    @Test
    public void testEnvSameDb() 
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, dbName);

        LDiffTestUtils.insertRecords(db1, 60000);
        LDiffTestUtils.insertRecords(db2, 60000);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        assertTrue(ldf.diff(env1, env2));
        assertEquals(ldf.getDiffRegions().size(), 0);

        db1.close();
        db2.close();
        env1.close();
        env2.close();
    }

    /**
     * Two environments with identical dbs with different names should not be
     * equal.
     */
    @Test
    public void testEnvDifferentName() 
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, "_" + dbName);

        LDiffTestUtils.insertRecords(db1, 60000);
        LDiffTestUtils.insertRecords(db2, 60000);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        assertFalse(ldf.diff(env1, env2));
        assertTrue(ldf.getDiffRegions() == null);

        db1.close();
        db2.close();
        env1.close();
        env2.close();
    }

    /**
     * Two environments with different dbs should not be equal.
     */
    @Test
    public void testEnvDifferentDb() 
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, dbName);

        byte[] keyarr2 =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        LDiffTestUtils.insertRecords(db1, 60000);
        LDiffTestUtils.insertWithDiffKeys(db2, 60000, keyarr2);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        assertFalse(ldf.diff(env1, env2));
        List<MismatchedRegion> regions = ldf.getDiffRegions();
        assertEquals(regions.size(), 1);
        MismatchedRegion region = regions.get(0);
        assertEquals(region.getLocalDiffSize(), -1);
        assertEquals(region.getRemoteDiffSize(), -1);

        db1.close();
        db2.close();
        env1.close();
        env2.close();
    }

    /**
     * Two environments with different numbers of dbs should not be equal.
     */
    @Test
    public void testEnvDifferentNumDbs() 
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, dbName); 
        Database db3 = LDiffTestUtils.createDatabase(env2, "_" + dbName);

        LDiffTestUtils.insertRecords(db1, 60000);
        LDiffTestUtils.insertRecords(db2, 60000);
        LDiffTestUtils.insertRecords(db3, 60000);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        assertFalse(ldf.diff(env1, env2));
        assertEquals(ldf.getDiffRegions().size(), 0);

        db1.close();
        db2.close();
        db3.close();
        env1.close();
        env2.close();
    }

    /**
     * Two environments with multiple identical dbs should be equal.
     */
    @Test
    public void testEnvMultipleDbs() 
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, dbName);
        Database db3 = LDiffTestUtils.createDatabase(env1, "_" + dbName);
        Database db4 = LDiffTestUtils.createDatabase(env2, "_" + dbName);;

        byte[] keyarr2 =
            { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        LDiffTestUtils.insertRecords(db1, 60000);
        LDiffTestUtils.insertRecords(db2, 60000);
        LDiffTestUtils.insertWithDiffKeys(db3, 60000, keyarr2);
        LDiffTestUtils.insertWithDiffKeys(db4, 60000, keyarr2);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        assertTrue(ldf.diff(env1, env2));
        assertEquals(ldf.getDiffRegions().size(), 0);

        db1.close();
        db2.close();
        db3.close();
        db4.close();
        env1.close();
        env2.close();
    }

    /**
     * LDiff of two empty databases should succeed.
     */
    @Test
    public void testEmptyDbs() 
        throws Exception {

        Environment env = LDiffTestUtils.createEnvironment(envDir1);
        Database db1 = LDiffTestUtils.createDatabase(env, "ldiff.a.db");
        Database db2 = LDiffTestUtils.createDatabase(env, "ldiff.b.db");
        
        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        assertTrue(ldf.diff(db1, db2));
        assertEquals(ldf.getDiffRegions().size(), 0);

        db1.close();
        db2.close();
        env.close();
    }
    
    /**
     * LDiff of a database against itself should succeed.
     */
    @Test
    public void testSameDb() 
        throws Exception {

        Environment env = LDiffTestUtils.createEnvironment(envDir1);
        Database db = LDiffTestUtils.createDatabase(env, dbName);

        LDiffTestUtils.insertRecords(db, 60000);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        BlockBag bag = ldf.createBlockBag(db);
        assertTrue(ldf.diff(db, bag));
        assertEquals(ldf.getDiffRegions().size(), 0);

        db.close();
        env.close();
    }

    /**
     * Get a bag of blocks from a database, update the database and then diff
     * it against the bag of blocks.
     */
    @Test
    public void testUpdatedDbs() 
        throws Exception {

        Environment env = LDiffTestUtils.createEnvironment(envDir1);
        Database db = LDiffTestUtils.createDatabase(env, dbName);

        byte[] newKeys = LDiffTestUtils.insertRecords(db, 60000);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        BlockBag bag = ldf.createBlockBag(db);

        LDiffTestUtils.insertRecords(db, 40000, newKeys, true);
        assertFalse(ldf.diff(db, bag));
        List<MismatchedRegion> regions = ldf.getDiffRegions();
        assertEquals(regions.size(), 1);
        MismatchedRegion region = regions.get(0);
        assertEquals(region.getLocalDiffSize(), -1);
        assertEquals(region.getRemoteDiffSize(), -1);

        db.close();
        env.close();
    }

    /**
     * Create two identical databases, remove and then reinsert all the records
     * from one, creating two databases with the same data but different
     * structure.
     */
    @Test
    public void testDifferentDbs() 
        throws Exception {

        Environment env = LDiffTestUtils.createEnvironment(envDir1);
        Database db1 = LDiffTestUtils.createDatabase(env, "ldiff.a.db");
        Database db2 = LDiffTestUtils.createDatabase(env, "ldiff.b.db");

        LDiffTestUtils.insertRecords(db1, 60000);
        LDiffTestUtils.insertRecords(db2, 60000);

        LDiffConfig cfg = new LDiffConfig();
        LDiff ldf = new LDiff(cfg);
        BlockBag bag= ldf.createBlockBag(db2);
        assertTrue(ldf.diff(db1, bag));
        assertEquals(ldf.getDiffRegions().size(), 0);

        /* Remove all of dbA's data. */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor curs = db1.openCursor(null, null);
        while (curs.getNext(key, data, LockMode.DEFAULT) ==
               OperationStatus.SUCCESS) {
            curs.delete();
        }
        curs.close();

        bag = ldf.createBlockBag(db2);
        assertFalse(ldf.diff(db1, bag));
        assertEquals(ldf.getDiffRegions().size(), 1);
        assertTrue(ldf.getDiffRegions().get(0).isRemoteAdditional());

        /* Re-populate dbA with the original data. */
        LDiffTestUtils.insertRecords(db1, 60000);

        bag = ldf.createBlockBag(db2);
        assertTrue(ldf.diff(db1, bag));
        assertEquals(ldf.getDiffRegions().size(), 0);

        db1.close();
        db2.close();
        env.close();
    }

    /* Test the database has additonal blocks. */
    @Test
    public void testAdditional()
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, dbName);

        LDiffTestUtils.insertAdditionalRecords(db1, db2);

        LDiffConfig config = new LDiffConfig();
        config.setBlockSize(10);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        /* Test Block additional. */
        LDiff ldiff = new LDiff(config);
        assertFalse(ldiff.diff(env1, env2));

        List<MismatchedRegion> regions = ldiff.getDiffRegions();

        assertEquals(regions.size(), 3);

        MismatchedRegion region = regions.get(0);
        assertTrue(region.isRemoteAdditional());
        assertEquals(region.getRemoteDiffSize(), 100);
        IntegerBinding.intToEntry(1, key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        StringBinding.stringToEntry("herococo", data);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        region = regions.get(1);
        assertTrue(region.isRemoteAdditional());
        assertEquals(region.getRemoteDiffSize(), 20);
        IntegerBinding.intToEntry(151, key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        region = regions.get(2);
        assertTrue(region.isRemoteAdditional());
        assertEquals(region.getRemoteDiffSize(), -1);
        IntegerBinding.intToEntry(201, key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        /* Test Window additional. */
        assertFalse(ldiff.diff(env2, env1));
        regions = ldiff.getDiffRegions();

        assertEquals(regions.size(), 3);
        region = regions.get(0);
        assertTrue(region.isLocalAdditional());
        assertEquals(region.getLocalDiffSize(), 100);
        IntegerBinding.intToEntry(1, key);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getLocalBeginData(), data);

        region = regions.get(1);
        assertTrue(region.isLocalAdditional());
        assertEquals(region.getLocalDiffSize(), 20);
        IntegerBinding.intToEntry(151, key);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getLocalBeginData(), data);

        region = regions.get(2);
        assertTrue(region.isLocalAdditional());
        assertEquals(region.getLocalDiffSize(), -1);
        IntegerBinding.intToEntry(201, key);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getLocalBeginData(), data);

        db1.close();
        db2.close();
        env1.close();
        env2.close(); 
    }

    /* Test two database have different blocks. */
    @Test
    public void testDifferentArea()
        throws Exception {

        Environment env1 = LDiffTestUtils.createEnvironment(envDir1);
        Environment env2 = LDiffTestUtils.createEnvironment(envDir2);
        Database db1 = LDiffTestUtils.createDatabase(env1, dbName);
        Database db2 = LDiffTestUtils.createDatabase(env2, dbName);

        LDiffTestUtils.insertDifferentRecords(db1, db2);

        LDiffConfig config = new LDiffConfig();
        config.setBlockSize(10);

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        LDiff ldiff = new LDiff(config);
        assertFalse(ldiff.diff(env1, env2));
        List<MismatchedRegion> regions = ldiff.getDiffRegions();
        assertEquals(regions.size(), 4);

        MismatchedRegion region = regions.get(0);
        assertEquals(region.getLocalDiffSize(), 10);
        assertEquals(region.getRemoteDiffSize(), 10);
        IntegerBinding.intToEntry(1, key);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        StringBinding.stringToEntry("herococo", data);
        checkByteArrayEquality(region.getLocalBeginData(), data);
        StringBinding.stringToEntry("hero-coco", data);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        region = regions.get(1);
        assertEquals(region.getLocalDiffSize(), 30);
        assertEquals(region.getRemoteDiffSize(), 30);
        IntegerBinding.intToEntry(51, key);
        StringBinding.stringToEntry("herococo", data);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        checkByteArrayEquality(region.getLocalBeginData(), data);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        region = regions.get(2);
        assertEquals(region.getLocalDiffSize(), 10);
        assertEquals(region.getRemoteDiffSize(), 10);
        IntegerBinding.intToEntry(91, key);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        checkByteArrayEquality(region.getLocalBeginData(), data);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        region = regions.get(3);
        assertEquals(region.getLocalDiffSize(), -1);
        assertEquals(region.getRemoteDiffSize(), -1);
        IntegerBinding.intToEntry(271, key);
        checkByteArrayEquality(region.getLocalBeginKey(), key);
        checkByteArrayEquality(region.getRemoteBeginKey(), key);
        checkByteArrayEquality(region.getLocalBeginData(), data);
        StringBinding.stringToEntry("hero-coco", data);
        checkByteArrayEquality(region.getRemoteBeginData(), data);

        db1.close();
        db2.close();
        env1.close();
        env2.close();
    }

    private void checkByteArrayEquality(byte[] arr1, DatabaseEntry entry) {
        assertEquals(arr1.length, entry.getData().length);

        for (int i = 0; i < arr1.length; i++) {
            assertEquals(arr1[i], entry.getData()[i]);
        }
    }
}
