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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.util.test.SharedTestUtils;

public class TestBase extends DualTestCase {

    static class TestState {

        Transaction transaction = null;
        DatabaseEntry key = null;
        DatabaseEntry oldData = null;
        DatabaseEntry newData = null;

        String newName = null;

        int nAddTrigger = 0;
        int nRemoveTrigger = 0;

        int nCreate = 0;
        int nClose = 0;
        int nOpen = 0;
        int nRemove = 0;
        int nTruncate = 0;
        int nRename = 0;

        int nPut = 0;
        int nDelete = 0;

        int nCommit = 0;
        int nAbort = 0;
    }

    /*
     * Synchronized since multiple replicas may insert entries at the same time.
     */
    public static Set<Trigger> invokedTriggers =
        Collections.synchronizedSet(new HashSet<Trigger>());

    /**
     * Transient DBT class. Does not implement PersistentTrigger, for minimal
     * testing of transient triggers, but must implement Serializable since it
     * is the superclass of a serializable class (DBT).
     */
    public static class TDBT
        implements Trigger, TransactionTrigger, Serializable {

        transient TestState ts = new TestState();

        private static final long serialVersionUID = 1L;
        final String name;
        transient String databaseName = null;

        public TDBT(String name) {
            super();
            this.name = name;
        }

        public Trigger setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            if (ts == null) {
               ts = new TestState();
            }
            return this;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public void delete(Transaction txn,
                           DatabaseEntry key,
                           DatabaseEntry oldData) {
            assertTrue(key != null);
            invokedTriggers.add(this);
            ts.transaction = txn;
            ts.key = key;
            ts.oldData = oldData;
            ts.nDelete++;
        }

        public void put(Transaction txn,
                        DatabaseEntry key,
                        DatabaseEntry oldData,
                        DatabaseEntry newData) {
            invokedTriggers.add(this);
            ts.transaction = txn;
            ts.key = key;
            ts.oldData = oldData;
            ts.newData = newData;
            ts.nPut++;
        }

        public String getName() {
            return name;
        }

        public void abort(Transaction txn) {
           invokedTriggers.add(this);
           ts.transaction = txn;
           ts.nAbort++;
        }

        public void commit(Transaction txn) {
           invokedTriggers.add(this);
           ts.transaction = txn;
           ts.nCommit++;
        }

        public void clear() {
            ts = new TestState();
        }

        public void addTrigger(Transaction txn) {
            invokedTriggers.add(this);
            ts.transaction = txn;
            ts.nAddTrigger++;
        }

        public void removeTrigger(Transaction txn) {
            invokedTriggers.add(this);
            ts.transaction = txn;
            ts.nRemoveTrigger++;
        }
    }

    /**
     * Regular/persistent trigger class.
     */
    public static class DBT extends TDBT implements PersistentTrigger {

        public DBT(String name) {
            super(name);
        }

        public void open(Transaction txn, Environment env, boolean isNew) {

            assertTrue(env != null);
            invokedTriggers.add(this);
            ts.transaction = txn;
            if (isNew) {
                ts.nCreate++;
            }
            ts.nOpen++;
        }

        public void close() {
            invokedTriggers.add(this);
            ts.transaction = null;
            ts.nClose++;
        }

        public void remove(Transaction txn) {
           invokedTriggers.add(this);
           ts.transaction = txn;
           ts.nRemove++;
        }

        public void rename(Transaction txn, String newName) {
            invokedTriggers.add(this);
            ts.transaction = txn;
            ts.newName = newName;
            ts.nRename++;
        }

        public void truncate(Transaction txn) {
            invokedTriggers.add(this);
            ts.transaction = txn;
            ts.nTruncate++;
        }
    }

    protected final File envRoot = SharedTestUtils.getTestDir();
    protected EnvironmentConfig envConfig = null;
    protected DatabaseConfig dbConfig = null;

    @Before
    public void setUp()
        throws Exception {
        
        super.setUp();
        envConfig = RepTestUtils.
            createEnvConfig(RepTestUtils.SYNC_SYNC_ALL_DURABILITY);

        dbConfig = getDBConfig();
    }

    DatabaseConfig getDBConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        config.setSortedDuplicates(false);
        return config;
    }
}
