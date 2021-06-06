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
package com.sleepycat.je.rep.dual.trigger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.util.RepEnvWrapper;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.trigger.ReplicatedDatabaseTrigger;
import com.sleepycat.je.trigger.TestBase;
import com.sleepycat.je.trigger.Trigger;

/**
 * Replicated version of the trigger unit test. The test repeats the trigger
 * tests but now across the replication group ensuring that the triggers are
 * invoked at every node in the replication group.
 */
public class InvokeTest extends com.sleepycat.je.trigger.InvokeTest {

    @Before
    public void setUp() 
        throws Exception {
        
        super.setUp();
        nNodes = ((RepEnvWrapper)getWrapper()).getRepEnvInfo(envRoot).length;
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
    }

    @Override
    public void testAddRemoveTriggerExistindDbTrans() {
        // TODO: Need equivalent tests once #18262 is fixed
    }

    @Override
    public void testAddRemoveTriggerExistindDbAuto() {
     // TODO: Need equivalent tests once #18262 is fixed
    }

    /*
     * All verifyXXX methods are overriden to ensure that the entire
     * replication group is in sync before verifying that the triggers have
     * been invoked.
     */
    @Override
    protected void verifyOpen(final int nCreate, int nOpen) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyOpen(nCreate, nOpen);
    }

    @Override
    protected void verifyClose(@SuppressWarnings("unused") final int nClose) {
        /* closes are asynchronous on the Replica. */
    }

    @Override
    protected void verifyRename(final String newName,
                                final int nRename) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyRename(newName, nRename);
    }

    @Override
    protected void verifyTruncate(final int nTruncate) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyTruncate(nTruncate);
    }

    @Override
    protected void verifyRemove(final int nRemove) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyRemove(nRemove);
    }

    @Override
    protected void verifyCommit(final int nCommit) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyCommit(nCommit);
    }

    @Override
    protected void verifyAbort(final int nAbort) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyAbort(nAbort);
    }

    @Override
    protected void verifyDelete(final int nDelete,
                                final DatabaseEntry key,
                                final DatabaseEntry oldData) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyDelete(nDelete, key, oldData);

    }

    @Override
    protected void verifyPut(final int nPut,
                             final DatabaseEntry key,
                             final DatabaseEntry newData,
                             final DatabaseEntry oldData) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyPut(nPut, key, newData, oldData);
    }

    @Override
    protected void verifyAddTrigger(final int nAddTrigger) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyAddTrigger(nAddTrigger);
    }

    @Override
    protected void verifyRemoveTrigger(final int nRemoveTrigger) {
        ((RepEnvWrapper)getWrapper()).syncGroup(envRoot);
        super.verifyRemoveTrigger(nRemoveTrigger);
    }

    @Override
    protected TransactionConfig getTransactionConfig() {
        return RepTestUtils.SYNC_SYNC_ALL_TC;
    }

    @Override
    protected List<Trigger> getTriggers() {
        return new LinkedList<Trigger>(Arrays.asList((Trigger) new RDBT("t1"),
                             (Trigger) new RDBT("t2")));
    }

    @Override
    protected List<Trigger> getTransientTriggers() {
        return new LinkedList<Trigger>(Arrays.asList((Trigger) new TRDBT("tt1"),
                             (Trigger) new TRDBT("tt2")));
    }

    @Override
    protected List<Trigger> getTriggersPlusOne() {
        List<Trigger> triggers = getTriggers();
        triggers.add(new InvokeTest.RDBT("t3"));
        return triggers;
    }

    @SuppressWarnings("unused")
    public static class RDBT extends TestBase.DBT
        implements ReplicatedDatabaseTrigger {

        private static final long serialVersionUID = 1L;

        public RDBT(String name) {
            super(name);
        }

        /* Awaits syncup unit tests. */

        public void repeatCreate(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatDelete(Transaction txn, DatabaseEntry key) {
            // TODO Auto-generated method stub
        }

        public void repeatPut(Transaction txn,
                              DatabaseEntry key,
                              DatabaseEntry newData) {
            // TODO Auto-generated method stub
        }

        public void repeatRemove(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatRename(Transaction txn, String newName) {
            // TODO Auto-generated method stub
        }

        public void repeatTransaction(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatTruncate(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatAddTrigger(Transaction txn) {
            // TODO Auto-generated method stub

        }

        public void repeatRemoveTrigger(Transaction txn) {
            // TODO Auto-generated method stub

        }
    }

    @SuppressWarnings("unused")
    public static class TRDBT extends TestBase.TDBT
        implements ReplicatedDatabaseTrigger {

        public TRDBT(String name) {
            super(name);
        }

        /* Awaits syncup unit tests. */

        public void repeatCreate(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatDelete(Transaction txn, DatabaseEntry key) {
            // TODO Auto-generated method stub
        }

        public void repeatPut(Transaction txn,
                              DatabaseEntry key,
                              DatabaseEntry newData) {
            // TODO Auto-generated method stub
        }

        public void repeatRemove(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatRename(Transaction txn, String newName) {
            // TODO Auto-generated method stub
        }

        public void repeatTransaction(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatTruncate(Transaction txn) {
            // TODO Auto-generated method stub
        }

        public void repeatAddTrigger(Transaction txn) {
            // TODO Auto-generated method stub

        }

        public void repeatRemoveTrigger(Transaction txn) {
            // TODO Auto-generated method stub

        }
    }
}
