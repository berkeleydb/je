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

package com.sleepycat.je.rep.vlsn;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.vlsn.VLSNIndex.WaitTimeOutException;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Jan from Nokia runs into the issue that the on disk VLSNIndex is not 
 * consistent with what we have in the cache when doing recovery. This happens
 * in the following senario:
 * 1.Thread A writes a log entry to the log already, but it sleeps before it
 *   registers the VLSN A of that entry
 * 2.The daemon Checkpointer thread wakes up, and flush the VLSNIndex to the
 *   disk, however, VLSN A is not registered so that it is not written
 * 3.Thread A wakes up again, and it registers VLSN A, it also continues 
 *   logging some other entries
 * 4.JE crashes at this moment, and while doing recovery, JE will read VLSNs 
 *   from the CkptStart, but VLSN A is logged before the CkptStart, so JE will
 *   find a gap between the VLSN in the durable VLSNRange and what we have in 
 *   the cache
 * See SR [#19754] for more details.
 *
 * Also test SR [#20165], which was a problem with this bug fix.
 */
public class VLSNConsistencyTest extends TestBase {
    private final File envRoot;
    private final String dbName = "testDB";
    private RepEnvInfo[] repEnvInfo;

    public VLSNConsistencyTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    /**
     * Test that the VLSNs in the durable VLSNIndex and the recovery cache are
     * consistent, no matter what happens.
     */
    @Test
    public void testVLSNConsistency() 
        throws Throwable {

        try {
            repEnvInfo = RepTestUtils.setupEnvInfos(envRoot, 1, 
                                                    makeEnvConfig());
            ReplicatedEnvironment master = RepTestUtils.joinGroup(repEnvInfo);
            RepNode repNode =
                RepInternal.getNonNullRepImpl(master).getRepNode();

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            Database db = master.openDatabase(null, dbName, dbConfig);

            /* Write some data. */
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            for (int i = 0; i <= 100; i++) {
                IntegerBinding.intToEntry(i, key);
                StringBinding.stringToEntry("herococo", data);
                db.put(null, key, data);
            }

            /* Record the last commit VLSN on the master. */
            VLSN commitVLSN = repNode.getCurrentTxnEndVLSN();

            final Transaction txn = master.beginTransaction(null, null);
            IntegerBinding.intToEntry(101, key);
            StringBinding.stringToEntry("herococo", data);
            db.put(txn, key, data);

            /* Make sure the VLSN of this new record is registered. */
            int counter = 1;
            VLSN lastVLSN = repNode.getVLSNIndex().getRange().getLast();
            while (lastVLSN.compareTo(commitVLSN) <= 0) {
                Thread.sleep(1000);
                counter++;
                if (counter == 10) {
                    throw new IllegalStateException
                        ("Test is in invalid state, the leaf node is not " +
                         "flushed.");
                }
            }

            /* This hook delays the registration of the TxnCommit's VLSN. */
            VLSNHook testHook = new VLSNHook();
            /* This hook is used to flush the TxnCommit to the log. */
            CountDownLatch flushHook = new CountDownLatch(1);
            FlushHook flushLogHook = 
                new FlushHook(
                    flushHook, RepInternal.getNonNullRepImpl(master));
            RepInternal.getNonNullRepImpl(master).getLogManager().
                setDelayVLSNRegisterHook(testHook);
            RepInternal.getNonNullRepImpl(master).getLogManager().
                setFlushLogHook(flushLogHook);

            /* 
             * Commit the transaction in another thread, so that it won't block
             * this test. 
             */
            JUnitThread commitThread = new JUnitThread("Commit") {
                @Override
                public void testBody() {
                    try {
                        txn.commit();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            };

            commitThread.start();

            /* Wait until the TxnCommit is logged on the disk. */
            flushHook.await();

            StatsConfig stConfig = new StatsConfig();
            stConfig.setFast(true);
            stConfig.setClear(true);
            long lastCkptEnd = 
                master.getStats(stConfig).getLastCheckpointEnd();

            /* Do a checkpoint to flush the VLSNIndex. */
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            master.checkpoint(ckptConfig);

            long newCkptEnd = 
                master.getStats(stConfig).getLastCheckpointEnd();
            while (newCkptEnd <= lastCkptEnd) {
                Thread.sleep(1000);
                newCkptEnd = master.getStats(stConfig).getLastCheckpointEnd();
                counter++;
                if (counter == 20) {
                    throw new IllegalStateException
                        ("Checkpointer didn't finish in specified time");
                }
            }

            /* Release the CountDownLatch so that the VLSN is registered. */
            commitThread.finishTest();

            /* Write some transactions, so that RecoveryTracker is not null. */
            for (int i = 102; i <= 200; i++) {
                IntegerBinding.intToEntry(i, key);
                StringBinding.stringToEntry("herococo", data);
                db.put(null, key, data);
            }

            /* 
             * Abnormally close the Environment, so the VLSNIndex flushed by 
             * the last checkpointer is the only version on the disk. 
             */
            repEnvInfo[0].abnormalCloseEnv();

            /* Reopen the Environment again to see if the test fails. */
            repEnvInfo[0].openEnv();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            RepTestUtils.shutdownRepEnvs(repEnvInfo);
        }
    }

    /* Hook used to block the VLSN registration. */
    private static class VLSNHook implements TestHook<Object>{
        
        private boolean doDelay;
        
        public VLSNHook() {
            doDelay = true;
        }

        public void hookSetup() {
        }

        public void doIOHook() 
            throws IOException {
        }

        public void doHook(Object obj) {
	}

        public void doHook() {
            try {
                if (doDelay) {
                    doDelay = false;
                    Thread.sleep(VLSNIndex.AWAIT_CONSISTENCY_MS * 2);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public Object getHookValue() {
            return null;
        }
    }

    /* Hook used to guarantee that the TxnCommit is flushed. */
    private static class FlushHook implements TestHook<CountDownLatch> {
        private final CountDownLatch flushLatch;
        private final EnvironmentImpl envImpl;

        public FlushHook(CountDownLatch flushLatch, EnvironmentImpl envImpl) {
            this.flushLatch = flushLatch;
            this.envImpl = envImpl;
        }

        public void hookSetup() {
        }

        public void doIOHook() 
            throws IOException {
        }

        public void doHook(CountDownLatch obj) {
	}

        public void doHook() {
            try {
                envImpl.getLogManager().flushSync();
                flushLatch.countDown();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        public CountDownLatch getHookValue() {
            return flushLatch;
        }
    }

    /**
     * [#20165] All feeders must wait for the same vlsn, but the checkpointer
     * is picking a vlsn to wait for that may be great than any currently
     * registed in the VLSNIndex. This can provoke an assertion, because the
     * waitForVLSN system assumes all feeders are lodged at the same point.
     */
    @Test
    public void testCheckpointerAhead() throws Exception {
        Environment env = new Environment(envRoot, makeEnvConfig());
        VLSNIndex vlsnIndex = null;

        try {
            vlsnIndex = new VLSNIndex(DbInternal.getNonNullEnvImpl(env),
                                     "TEST_MAP_DB", new NameIdPair("n1",1),
                                      10, 100, 1000, 
                                      new RecoveryInfo());
            vlsnIndex.initAsMaster();
            CountDownLatch checker = new CountDownLatch(2);

            /* 
             * Thread A begins a write. First it bumps the vlsn. This
             * is done within the log write latch. It is still logging, and
             * has not yet registered the vlsn.
             */
            VLSN vlsnA = vlsnIndex.bump();

            /* 
             * A feeder is either already waiting on vlsnA, or begns to wait
             * for it now. It must wait because the vlsn is not yet written to
             * the log and registered.
             */
            MockFeeder feederA = new MockFeeder(vlsnIndex, vlsnA, checker);
            feederA.start();

            /* Thread B begins a write. */
            VLSN vlsnB = vlsnIndex.bump();

            /* Make sure that the waitVLSN is at vlsn A already. */
            while (true) {
                VLSN waitFor = vlsnIndex.getPutWaitVLSN();
                if ((waitFor != null)  &&
                    (waitFor.equals(vlsnA))) {
                    break;
                } 
                Thread.sleep(100);
            }

            /* 
             * Now the checkpoint awaits consistency. It uses the latest
             * generated VLSN, which is VLSN B. In the original bug, this
             * caused an assertion because the vlsnIndex.waitForVLSN
             * rightfully assumes that all Feeders await the same address,
             * and objects that the checkpointer is waiting on VLSN B
             */
            CountDownLatch ckptStarted = new CountDownLatch(1);
            MockCheckpointer ckpter = new MockCheckpointer(vlsnIndex,
                                                           checker,
                                                           ckptStarted);
            ckpter.start();
            ckptStarted.await();
            vlsnIndex.put(makeLogItem(100, vlsnA));
            vlsnIndex.put(makeLogItem(1000, vlsnB));
            checker.await();
        } finally {
            if (vlsnIndex != null) {
                vlsnIndex.close();
            }
            env.close();
        }
    }

    private EnvironmentConfig makeEnvConfig() {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setConfigParam (EnvironmentConfig.LOG_FILE_MAX, "1000000");
        return envConfig;
    }

    private LogItem makeLogItem(long useLsn, VLSN useVLSN) {
        LogItem item = new LogItem();
        item.header = new LogEntryHeader(LogEntryType.LOG_TRACE.getTypeNum(),
                                         1,
                                         100,
                                         useVLSN);
        item.lsn = useLsn;
        return item;
    }

    private class MockFeeder extends Thread {
        private final VLSN target;
        private final VLSNIndex vlsnIndex;
        private final CountDownLatch checker;

        MockFeeder(VLSNIndex index, VLSN target, CountDownLatch checker) {
            this.target = target;
            this.vlsnIndex = index;
            this.checker = checker;
        }

        @Override
        public void run() {
            try {
                vlsnIndex.waitForVLSN(target, 100000);
                checker.countDown();
            } catch (InterruptedException e) {
                fail("unexpected: " +  e);
            } catch (WaitTimeOutException e) {
                fail("unexpected: " +  e);
            }
        }
    }

    private class MockCheckpointer extends Thread {
        private final VLSNIndex vlsnIndex;
        private final CountDownLatch checker;
        private final CountDownLatch started;

        MockCheckpointer(VLSNIndex index, 
                         CountDownLatch checker,
                         CountDownLatch started) {
            this.vlsnIndex = index;
            this.checker = checker;
            this.started = started;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                vlsnIndex.awaitConsistency();
            } catch (Exception e) {
                fail(e.toString());
            }
            checker.countDown();
        }
    }
}
