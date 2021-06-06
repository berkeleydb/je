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
package com.sleepycat.je.trigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.TriggerManager.MapOver;

/**
 * This set of unit tests exercises all standalone trigger invocations.
 */

public class InvokeTest extends TestBase {

    Environment env;
    Database db1 = null;
    int triggerCount = -1;
    /* The number of nodes. it's > 1 if this is a replicated environment. */
    protected int nNodes = 1;

    protected List<Trigger> getTriggers() {
        return new LinkedList<Trigger>(Arrays.asList((Trigger) new DBT("t1"),
                             (Trigger) new DBT("t2")));
    }

    protected List<Trigger> getTransientTriggers() {
        return new LinkedList<Trigger>(Arrays.asList((Trigger) new TDBT("tt1"),
                             (Trigger) new TDBT("tt2")));
    }

    protected List<Trigger> getTriggersPlusOne() {
        List<Trigger> triggers = getTriggers();
        triggers.add(new InvokeTest.DBT("t3"));
        return triggers;
    }

    protected TransactionConfig getTransactionConfig() {
        return null;
    }

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        List<Trigger> triggers = getTriggers();
        triggerCount = triggers.size();
        dbConfig.setTriggers(triggers);

        dbConfig.setOverrideTriggers(true);

        env = create(envRoot, envConfig);
        Transaction transaction =
            env.beginTransaction(null, getTransactionConfig());
        db1 = env.openDatabase(transaction, "db1", dbConfig);
        transaction.commit();
        dbConfig.setOverrideTriggers(false);
        resetTriggers();
    }

    @After
    public void tearDown() 
        throws Exception {
        
        db1.close();
        close(env);
        super.tearDown();
    }

    @Test
    public void testAddRemoveTriggerExistindDbTrans() {
        Transaction transaction =
            env.beginTransaction(null, getTransactionConfig());
        addRemoveTriggerExistingDb(transaction);
        transaction.commit();
    }

    @Test
    public void testAddRemoveTriggerExistindDbAuto() {
        addRemoveTriggerExistingDb(null);
    }

    void addRemoveTriggerExistingDb(Transaction transaction) {
        db1.close();
        resetTriggers();

        /* read/write open. */
        db1 = env.openDatabase(transaction, "db1", dbConfig);
        verifyAddTrigger(0);
        verifyRemoveTrigger(0);
        checkNullOpenTriggerCount(1);
        db1.close();
        resetTriggers();

        dbConfig.setOverrideTriggers(true);
        dbConfig.setTriggers(getTriggersPlusOne());
        db1 = env.openDatabase(transaction, "db1", dbConfig);
        DatabaseImpl db1Impl = DbInternal.getDbImpl(db1);
        DBT t3 = (DBT) db1Impl.getTriggers().get(2);
        assertEquals("t3", t3.getName());
        assertEquals(1, t3.ts.nAddTrigger);
        assertEquals(0, t3.ts.nRemoveTrigger);
        db1.close();
        resetTriggers();

        dbConfig.setTriggers(getTriggers());
        db1 = env.openDatabase(transaction, "db1", dbConfig);
        db1Impl = DbInternal.getDbImpl(db1);
        assertEquals("t3", t3.getName());
        assertEquals(0, t3.ts.nAddTrigger);
        assertEquals(1, t3.ts.nRemoveTrigger);
    }

    /**
     * Simply verifies that transient triggers are indeed transient, i.e., not
     * stored in the DatabaseImpl.  Also checks that a transient trigger can be
     * added when setOverrideTriggers(true) is not called.
     */
    @Test
    public void testBasicTransientTrigger() {

        /* Set two transient triggers in new DB. */
        List<Trigger> tList = getTransientTriggers();
        DatabaseConfig tdbConfig = getDBConfig();
        tdbConfig.setOverrideTriggers(false);
        tdbConfig.setTriggers(tList);
        Database dbc = env.openDatabase(null, "dbc", tdbConfig);
        assertEquals(tList, dbc.getConfig().getTriggers());
        for (Trigger trigger : dbc.getConfig().getTriggers()) {
            assertEquals("dbc", trigger.getDatabaseName());
        }
        verifyAddTrigger(1);
        checkTransientTriggerCount(1);
        resetTriggers();

        /* Test simple transctional put(). */
        Transaction transaction = env.beginTransaction(null, null);
        DatabaseEntry key = new DatabaseEntry(new byte[] {1});
        DatabaseEntry data = new DatabaseEntry(new byte[] {2});
        dbc.put(transaction, key, data);
        verifyPut(1, key, data, null);
        transaction.commit();
        verifyCommit(1);
        checkTransientTriggerCount(1);
        resetTriggers();

        /* Close DB and reopen -- there should be no triggers. */
        dbc.close();
        verifyRemoveTrigger(1);
        checkTransientTriggerCount(1);
        resetTriggers();
        tdbConfig = getDBConfig();
        tdbConfig.setOverrideTriggers(false);
        tdbConfig.setAllowCreate(false);
        dbc = env.openDatabase(null, "dbc", tdbConfig);
        assertNull(dbc.getConfig().getTriggers());
        verifyAddTrigger(0);
        dbc.close();
        verifyRemoveTrigger(0);
        checkTransientTriggerCount(0);

        /* Remove DB and recreate -- there should be no triggers. */
        env.removeDatabase(null, "dbc");
        tdbConfig = getDBConfig();
        tdbConfig.setOverrideTriggers(false);
        dbc = env.openDatabase(null, "dbc", tdbConfig);
        assertNull(dbc.getConfig().getTriggers());
        verifyAddTrigger(0);
        dbc.close();
        verifyRemoveTrigger(0);
        checkTransientTriggerCount(0);

        /* Add triggers to existing DB without overriding config. */
        tdbConfig = getDBConfig();
        tdbConfig.setOverrideTriggers(false);
        tdbConfig.setTriggers(tList);
        tdbConfig.setAllowCreate(false);
        dbc = env.openDatabase(null, "dbc", tdbConfig);
        assertEquals(tList, dbc.getConfig().getTriggers());
        for (Trigger trigger : dbc.getConfig().getTriggers()) {
            assertEquals("dbc", trigger.getDatabaseName());
        }
        verifyAddTrigger(1);
        dbc.close();
        verifyRemoveTrigger(1);
        checkTransientTriggerCount(1);
        resetTriggers();

        /* Clean up. */
        env.removeDatabase(null, "dbc");
    }

    private void create(Transaction transaction) {
        List<Trigger> tgs = getTriggers();
        DatabaseConfig tdbConfig = getDBConfig();
        dbConfig.setOverrideTriggers(true);
        tdbConfig.setTriggers(tgs);
        verifyOpen(0, 0);
        Database dbc = env.openDatabase(transaction, "dbc", tdbConfig );

        for (Trigger trigger : dbc.getConfig().getTriggers()) {
            assertEquals("dbc", trigger.getDatabaseName());
        }
        verifyOpen(1, 1);
        verifyAddTrigger(1);

        if (transaction == null) {
            verifyCommit(1);
        }
        dbc.close();
        verifyClose(1);
        /* Read triggers from the existing database. */
        checkTriggerCount(1);
        resetTriggers();
        dbc = env.openDatabase(transaction, "dbc", dbConfig);
        verifyOpen(0,1);
        verifyAddTrigger(0);
        /* Not a new database no create and consequently commit triggers. */
        verifyCommit(0);
        checkNullOpenTriggerCount(1);
        assertEquals(2, db1.getConfig().getTriggers().size());
        dbc.close();
        env.removeDatabase(transaction, "dbc");
    }

    @Test
    public void testCreateAuto() {
        create(null);
    }

    @Test
    public void testCreateTrans() {
        Transaction transaction =
            env.beginTransaction(null, getTransactionConfig());
        create(transaction);
        transaction.commit();
    }

    @Test
    public void testOpenAuto() {
        open(null);
    }

    @Test
    public void testOpenTrans() {
        Transaction transaction =
            env.beginTransaction(null, getTransactionConfig());
        open(transaction);
        transaction.commit();
    }

    private void open(Transaction transaction) {
        db1.close();
        resetTriggers();
        /* read/write open. */
        db1 = env.openDatabase(transaction, "db1", dbConfig);
        verifyOpen(0,1);
        checkNullOpenTriggerCount(1);
        db1.close();
        resetTriggers();

        DatabaseConfig config = getDBConfig();
        config.setReadOnly(true);
        db1 = env.openDatabase(transaction, "db1", config);
        verifyOpen(0,0);
        checkTriggerCount(0);
        resetTriggers();

        config.setReadOnly(false);
        Database db11 = env.openDatabase(transaction, "db1", config);
        verifyOpen(0,1);
        checkNullOpenTriggerCount(1);
        db11.close();
        resetTriggers();
    }

    @Test
    public void testClose() {
        closeDb();
    }

    private void closeDb() {
        resetTriggers();
        db1.close();
        verifyClose(1);
        checkNullOpenTriggerCount(1);
    }

    private void rename(Transaction transaction) {
        db1.close();
        env.renameDatabase(transaction, "db1", "dbr1");
        verifyRename("dbr1", 1);
        checkTriggerCount(1);
    }

    @Test
    public void testRenameAuto() {
        rename(null);
    }

    @Test
    public void testRenameTrans() {
        Transaction transaction = env.beginTransaction(null, null);
        rename(transaction);
        checkTriggerCount(1);
        transaction.commit();
    }

    @Test
    public void testRenameAbort() {
        Transaction transaction = env.beginTransaction(null, null);
        rename(transaction);
        checkTriggerCount(1);
        transaction.abort();
        db1 = env.openDatabase(null, "db1", dbConfig);
        verifyDB1Triggers();
        verifyAbort(1);
    }

    private void truncate(Transaction transaction) {
        db1.close();
        env.truncateDatabase(transaction, "db1", false);
        verifyTruncate(1);
        checkTriggerCount(1);
    }

    @Test
    public void testTruncateAuto() {
        truncate(null);
    }

    @Test
    public void testTruncateTrans() {
        Transaction transaction = env.beginTransaction(null, null);
        truncate(transaction);
        transaction.commit();

        /*
         * Truncate does a rename under the covers so make sure the triggers
         * are present on the new empty database.
         */
        db1 = env.openDatabase(null, "db1", dbConfig);
        verifyDB1Triggers();
    }

    private void verifyDB1Triggers() {
        assertEquals("db1", db1.getDatabaseName());
        assertEquals(triggerCount, db1.getConfig().getTriggers().size());
        for (Trigger t : db1.getConfig().getTriggers()) {
            assertEquals("db1", t.getDatabaseName());
        }
    }

    private void remove(Transaction transaction) {
        db1.close();
        env.removeDatabase(transaction, "db1");
        verifyRemove(1);
        verifyRemoveTrigger(1);
        checkTriggerCount(1);
    }

    @Test
    public void testRemoveAuto() {
        remove(null);
    }

    @Test
    public void testRemoveTrans() {
        Transaction transaction = env.beginTransaction(null, null);
        remove(transaction);
        transaction.commit();
    }

    @Test
    public void testKVOpsAuto() {
        KVOps(null);
    }

    @Test
    public void testKVOpsTrans() {
        Transaction transaction = env.beginTransaction(null, null);
        KVOps(transaction);
        transaction.commit();
    }

    @Test
    public void testKVOpsAbort() {
        Transaction transaction = env.beginTransaction(null, null);
        KVOps(transaction);
        transaction.abort();
        verifyAbort(1);
    }

    private void KVOps(Transaction transaction) {

        DatabaseEntry key = new DatabaseEntry();
        key.setData(new byte[]{1});
        DatabaseEntry data1 = new DatabaseEntry();
        data1.setData(new byte[]{2});
        verifyPut(0, null, null, null);

        db1.put(transaction, key, data1);
        verifyPut(1, key, data1, null);
        checkTriggerCount(1);
        resetTriggers();
        DatabaseEntry data2 = new DatabaseEntry();
        data2.setData(new byte[]{3});
        db1.put(transaction, key, data2);
        verifyPut(1, key, data2, data1);
        checkTriggerCount(1);
        resetTriggers();

        OperationStatus status = db1.delete(transaction, key);
        assertEquals(OperationStatus.SUCCESS, status);
        verifyDelete(1, key, data2);
        checkTriggerCount(1);
        resetTriggers();

        status = db1.delete(transaction, key);
        assertEquals(OperationStatus.NOTFOUND, status);
        verifyDelete(0, null, null);
        checkTriggerCount(0);

        db1.close();
    }

    /**
     * Ensure recovery replay of MapLNs executes properly.  This tests a fix
     * for an NPE that occurred during DatabaseImpl.readFromLog, which was
     * calling DatabaseImpl.getName prior to instantiation of DbTree.
     */
    @Test
    public void testBasicRecovery() {
        db1.close();
        close(env);
        resetTriggers();
        env = create(envRoot, envConfig);
        db1 = env.openDatabase(null, "db1", dbConfig);
        checkNullOpenTriggerCount(1);
        verifyAddTrigger(0);
        verifyRemoveTrigger(0);
    }

    private void resetTriggers() {
        for (Trigger t : TestBase.invokedTriggers) {
            ((TDBT)t).clear();
        }
        TestBase.invokedTriggers.clear();
    }

    protected void verifyDelete(final int nDelete,
                                final DatabaseEntry key,
                                final DatabaseEntry oldData) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                final TDBT dbt = (TDBT)e;
                assertEquals(nDelete, dbt.ts.nDelete);
                assertEquals(key, dbt.ts.key);
                assertEquals(oldData, dbt.ts.oldData);
                dbt.ts.nDelete = 0;
                return e;
            }
        }.run();
    }

    protected void verifyPut(final int nPut,
                             final DatabaseEntry key,
                             final DatabaseEntry newData,
                             final DatabaseEntry oldData) {;

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                final TDBT dbt = (TDBT)e;
                assertEquals(nPut, dbt.ts.nPut);
                dbt.ts.nPut = 0;
                assertEquals(key, dbt.ts.key);
                assertEquals(newData, dbt.ts.newData);
                assertEquals(oldData, dbt.ts.oldData);
                return e;
            }
        }.run();
    }

    protected void verifyOpen(final int nCreate, final int nOpen) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nOpen, ((TDBT)e).ts.nOpen);
                assertEquals(nCreate, ((TDBT)e).ts.nCreate);
                ((TDBT)e).ts.nCreate = 0;
                ((TDBT)e).ts.nOpen = 0;
                return e;
            }
        }.run();
    }

    protected void verifyClose(final int nClose) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nClose, ((TDBT)e).ts.nClose);
                ((TDBT)e).ts.nClose = 0;
                return e;
            }
        }.run();
    }

    /* The triggers should have been executed on all nodes. */
    protected void checkTriggerCount(final int count) {
        assertEquals((count * nNodes * triggerCount),
                     TestBase.invokedTriggers.size());
    }

    /* Transient triggers are only executed on the node they're configured. */
    protected void checkTransientTriggerCount(final int count) {
        assertEquals((count * triggerCount),
                     TestBase.invokedTriggers.size());
    }

    /*
     * Null open triggers, ones where the db is opened for writes,
     * but no writes are actually done, will not fire on replica nodes.
     */
    protected void checkNullOpenTriggerCount(final int count) {
        assertEquals((count * triggerCount),
                     TestBase.invokedTriggers.size());
    }

    protected void verifyRemove(final int nRemove) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nRemove, ((DBT)e).ts.nRemove);
                ((DBT)e).ts.nRemove = 0;
                return e;
            }
        }.run();
    }

    protected void verifyTruncate(final int nTruncate) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nTruncate, ((DBT)e).ts.nTruncate);
                ((DBT)e).ts.nTruncate = 0;
                return e;
            }
        }.run();
    }

    protected void verifyRename(final String newName,
                                final int nRename) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                final DBT dbt = (DBT)e;
                assertEquals(nRename, dbt.ts.nRename);
                newName.equals(dbt.ts.newName);
                dbt.ts.nRename = 0;
                assertEquals(newName, dbt.getDatabaseName());
                return e;
            }
        }.run();
    }

    protected void verifyCommit(final int nCommit) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nCommit, ((TDBT)e).ts.nCommit);
                ((TDBT)e).ts.nCommit = 0;
                return e;
            }
        }.run();
    }

    protected void verifyAbort(final int nAbort) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nAbort, ((TDBT)e).ts.nAbort);
                ((TDBT)e).ts.nAbort = 0;
                return e;
            }
        }.run();
    }

    protected void verifyRemoveTrigger(final int nRemoveTrigger) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nRemoveTrigger, ((TDBT)e).ts.nRemoveTrigger);
                ((TDBT)e).ts.nRemoveTrigger = 0;
                return e;
            }
        }.run();
    }

    protected void verifyAddTrigger(final int nAddTrigger) {

        new MapOver<Trigger,Trigger>(TestBase.invokedTriggers) {

            @Override
            protected Trigger fun(Trigger e) {
                assertEquals(nAddTrigger, ((TDBT)e).ts.nAddTrigger);
                ((TDBT)e).ts.nAddTrigger = 0;
                return e;
            }
        }.run();
    }
}
