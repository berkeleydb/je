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

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.util.test.SharedTestUtils;

/**
 * Tests a full write load with a disk limit to ensure that files are deleted
 * quickly enough to avoid hitting the disk limit. With HA, reserved files
 * (files cleaned and ready-to-delete) are retained until the disk limit is
 * approached. A steady-state update workload is used to generate waste as
 * quickly as possible.
 * <p>
 * Suggested steady-state runs:
 * <pre>
 *   # Test steady-state max throughput with one node.
 *   DiskLimitStress -nodes 1 -minutes 15
 *   # Test steady-state HA throughput.
 *   DiskLimitStress -nodes 3 -minutes 15
 * </pre>
 * <p>
 * In addition this test periodically lowers the disk limit on one or more
 * nodes to cause it to be violated, then restores the original limit and
 * expects the write load to continue as before. There are three cases that
 * are tested:
 * <pre>
 * 1. Master node violates disk limit, both replicas do not. No writes can
 *    occur.
 *
 * 2. One replica node violates disk limit, but not the master and the other
 *    replica. Writes can continue. The replica which violates the limit
 *    will lag, but the test doesn't stay in this mode long enough that a
 *    network restore is needed.
 *
 * 3. Both replicas violate disk limit, but not the master.
 *    InsufficientAcksException and/or InsufficientReplicasException will be
 *    thrown.
 * </pre>
 * <p>
 * Suggested run to test the three disk limit violation scenarios:
 * <pre>
 *   # Test violations of disk limit
 *   DiskLimitStress -nodes 3 -violations true -minutes 25
 * </pre>
 */
public class DiskLimitStress {

    /*
     * Sizes are designed so the data set fits within MAX_DISK, but also so
     * reserved files will be deleted. These sizes are used for the
     * steady-state test mode (-violations false).
     */
    private static final long ONE_MB = 1L << 20;
    private static final long FILE_SIZE = 100 * ONE_MB;
    private static final long CLEANER_BYTES_INTERVAL = 100 * ONE_MB;
    private static final int DATA_SIZE = 1024;
    private static final int ACTIVE_FILES = 20;
    private static final int TOTAL_FILES = 30;
    private static final int RECORDS =
        (int) ((ACTIVE_FILES * FILE_SIZE) / DATA_SIZE);
    private static final long MAX_DISK = TOTAL_FILES * FILE_SIZE;
    private static final long FREE_DISK = 0;
    private static final long HA_TIMEOUT_MS = 2000;

    /*
     * With disk violations, use a very small data set. With a larger data
     * set, there is no guarantee that all records will be updated and
     * cleaned, making the use of disk space difficult to predict.
     */
    private static final int VIOLATIONS_MODE_RECORDS = 1000;

    private boolean withViolations;
    private long runStopTime;
    private String homeDir;
    private int nodes;
    private int updateThreads;
    private int cleanerThreads;
    private int minutes;
    private int records;
    private volatile boolean stopFlag = false;
    private final AtomicReference<Throwable> unexpectedEx =
        new AtomicReference<>(null);
    private final UpdateThread[] threads;
    private RepEnvInfo[] repEnvInfo;
    private Database masterDb;
    private long lastStatTime;
    private long lastOperations;
    private long lastExceptions;
    private boolean allowDiskLimitEx;
    private boolean allowDurabilityEx;
    private boolean expectDiskLimitEx;
    private boolean expectDurabilityEx;

    public static void main(final String[] args) {
        try {
            printArgs(args);
            final DiskLimitStress test = new DiskLimitStress(args);
            test.runTest();
            System.out.println("SUCCESS");
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(-1);
        }
    }

    private static void printArgs(String[] args) {
        System.out.print("Command line args:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
    }

    private DiskLimitStress(String[] args)
        throws IOException {

        homeDir = "tmp";
        nodes = 1;
        updateThreads = 4;
        cleanerThreads = 2;
        minutes = 10;
        withViolations = false;

        for (int i = 0; i < args.length; i += 1) {
            final String arg = args[i];
            final boolean moreArgs = i < args.length - 1;
            if (arg.equals("-h") && moreArgs) {
                homeDir = args[++i];
            } else if (arg.equals("-violations") && moreArgs) {
                withViolations = Boolean.parseBoolean(args[++i]);
            } else if (arg.equals("-nodes") && moreArgs) {
                nodes = Integer.parseInt(args[++i]);
            } else if (arg.equals("-threads") && moreArgs) {
                updateThreads = Integer.parseInt(args[++i]);
            } else if (arg.equals("-cleaners") && moreArgs) {
                cleanerThreads = Integer.parseInt(args[++i]);
            } else if (arg.equals("-minutes") && moreArgs) {
                minutes= Integer.parseInt(args[++i]);
            } else {
                throw new IllegalArgumentException("Unknown arg: " + arg);
            }
        }

        threads = new UpdateThread[updateThreads];

        records = withViolations ? VIOLATIONS_MODE_RECORDS : RECORDS;

        System.out.println(
            "Resolved args:" +
                " homeDir=" + homeDir +
                " nodes=" + nodes +
                " updateThreads=" + updateThreads +
                " cleanerThreads=" + cleanerThreads +
                " totalRecords=" + records +
                " minutes=" + minutes);
    }

    private void start(final boolean smallRepTimeouts)
        throws IOException {

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setMaxDisk(MAX_DISK);
        envConfig.setSharedCache(true);
        envConfig.setCacheMode(CacheMode.EVICT_LN);
        envConfig.setCachePercent(70);

        envConfig.setConfigParam(
            EnvironmentConfig.FREE_DISK,
            String.valueOf(FREE_DISK));

        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_THREADS,
            String.valueOf(cleanerThreads));

        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_BYTES_INTERVAL,
            String.valueOf(CLEANER_BYTES_INTERVAL));

        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(FILE_SIZE));

        /* The verifier slows down the test and causes timeouts. */
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");

        final ReplicationConfig repConfig = new ReplicationConfig();

        /*
         * Use small timeouts to promptly cause InsufficientAcksException
         * and InsufficientReplicasException when replicas have a disk limit
         * violation, and to promptly restore normal write operations when
         * the violation is cleared.
         */
        if (smallRepTimeouts) {
            final String timeout = String.valueOf(HA_TIMEOUT_MS) + " ms";
            repConfig.setConfigParam(
                ReplicationConfig.FEEDER_TIMEOUT, timeout);
            repConfig.setConfigParam(
                ReplicationConfig.REPLICA_TIMEOUT, timeout);
            repConfig.setConfigParam(
                ReplicationConfig.REPLICA_ACK_TIMEOUT, timeout);
            repConfig.setConfigParam(
                ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT, timeout);
        }

        SharedTestUtils.cleanUpTestDir(new File(homeDir));

        repEnvInfo = RepTestUtils.setupEnvInfos(
            new File(homeDir), nodes, envConfig, repConfig);

        final Environment masterEnv = RepTestUtils.joinGroup(repEnvInfo);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        masterDb = masterEnv.openDatabase(null, "foo", dbConfig);

        final int keysPerThread = (records / updateThreads) + 1;
        int startKey = 0;
        for (int i = 0; i < updateThreads; i += 1) {
            threads[i] = new UpdateThread(keysPerThread, startKey);
            startKey += keysPerThread;
        }

        stopFlag = false;

        for (final Thread t : threads) {
            t.start();
        }
    }

    private void stop()
        throws Throwable {

        if (unexpectedEx.get() != null) {
            throw unexpectedEx.get();
        }

        stopFlag = true;

        for (final Thread t : threads) {
            t.join(10 * 1000);
            if (t.isAlive()) {
                t.interrupt();
                t.join(10 * 1000);
                if (t.isAlive()) {
                    throw new RuntimeException(
                        "Thread " + t.getName() + " still running");
                }
            }
        }

        masterDb.close();

        /* Tolerate DiskLimitException to simplify test. Close master last. */
        for (int i = nodes - 1; i >= 0; i -= 1) {
            try {
                repEnvInfo[i].closeEnv();
            } catch (DiskLimitException expected) {
                /* Do nothing. */
            }
        }
    }

    private void runTest() throws Throwable {

        lastStatTime = System.currentTimeMillis();
        long durationMs = minutes * 60 * 1000;

        if (withViolations) {
            if (nodes > 1) {
                durationMs /= 3;
            }

            runStopTime = System.currentTimeMillis() + durationMs;
            start(false /*smallRepTimeouts*/);
            runWithViolationsOnMaster();
            stop();

            if (nodes > 1) {
                runStopTime += durationMs;
                start(false /*smallRepTimeouts*/);
                runWithViolationsOnOneReplica();
                stop();

                runStopTime += durationMs;
                start(true /*smallRepTimeouts*/);
                runWithViolationsOnAllReplicas();
                stop();
            }
        } else {
            runStopTime = System.currentTimeMillis() + durationMs;
            start(false /*smallRepTimeouts*/);
            runWithNoViolations();
            stop();
        }
    }

    private void setDiskLimitViolation(final RepEnvInfo info) {
        setMaxDisk(info, 1);
    }

    private void clearDiskLimitViolation(final RepEnvInfo info) {
        setMaxDisk(info, MAX_DISK);
    }

    private void setMaxDisk(final RepEnvInfo info, final long size) {

        final Environment env = info.getEnv();

        env.setMutableConfig(env.getMutableConfig().setMaxDisk(size));

        info.getRepImpl().getCleaner().manageDiskUsage();
    }

    private void runWithNoViolations()
        throws Throwable {

        while (!stopFlag) {

            runAndCheckExceptions(
                System.currentTimeMillis() + 5000);

            refreshStats();
        }
    }

    private void runWithViolationsOnMaster()
        throws Throwable {

        /* Time to run before lowering MAX_DISK to cause a violation. */
        final long steadyStateMs = 5 * 1000;

        /* Time to wait for lowering MAX_DISK to take effect. */
        final long setViolationMs = 3 * 1000;

        /* Time to wait for restoring MAX_DISK to take effect. */
        final long clearViolationMs = 3 * 1000;

        allowDurabilityEx = false;
        expectDurabilityEx = false;

        while (!stopFlag) {

            System.out.println("Steady state with no violations");

            allowDiskLimitEx = false;
            expectDiskLimitEx = false;

            runAndCheckExceptions(
                System.currentTimeMillis() + steadyStateMs);

            cleanLog();

            System.out.println("Start violations on MASTER");

            setDiskLimitViolation(repEnvInfo[0]);

            allowDiskLimitEx = true;
            expectDiskLimitEx = true;

            runAndCheckExceptions(
                System.currentTimeMillis() + setViolationMs);

            System.out.println("Clear violations on MASTER");

            clearDiskLimitViolation(repEnvInfo[0]);

            allowDiskLimitEx = true;
            expectDiskLimitEx = false;

            runAndCheckExceptions(
                System.currentTimeMillis() + clearViolationMs);

            for (final UpdateThread t : threads) {
                t.clearExceptions();
            }
        }
    }

    private void runWithViolationsOnOneReplica()
        throws Throwable {

        /* Time to run before lowering MAX_DISK to cause a violation. */
        final long steadyStateMs = 3 * HA_TIMEOUT_MS;

        /*
         * Time to wait for lowering MAX_DISK to take effect.
         * For replicas we should to wait long enough to cause
         * InsufficientReplicasException as well as InsufficientAcksException.
         */
        final long setViolationMs = 3 * HA_TIMEOUT_MS;

        /* Time to wait for restoring MAX_DISK to take effect. */
        final long clearViolationMs = 5 * HA_TIMEOUT_MS;

        allowDiskLimitEx = false;
        expectDiskLimitEx = false;

        allowDurabilityEx = false;
        expectDurabilityEx = false;

        while (!stopFlag) {

            System.out.println("Steady state with no violations");

            runAndCheckExceptions(
                System.currentTimeMillis() + steadyStateMs);

            cleanLog();

            System.out.println("Start violations on ONE_REPLICA");

            setDiskLimitViolation(repEnvInfo[1]);

            runAndCheckExceptions(
                System.currentTimeMillis() + setViolationMs);

            System.out.println("Clear violations on ONE_REPLICA");

            clearDiskLimitViolation(repEnvInfo[1]);

            runAndCheckExceptions(
                System.currentTimeMillis() + clearViolationMs);

            for (final UpdateThread t : threads) {
                t.clearExceptions();
            }
        }
    }

    private void runWithViolationsOnAllReplicas()
        throws Throwable {

        /* Time to run before lowering MAX_DISK to cause a violation. */
        final long steadyStateMs = 3 * HA_TIMEOUT_MS;

        /*
         * Time to wait for lowering MAX_DISK to take effect.
         * For replicas we should to wait long enough to cause
         * InsufficientReplicasException as well as InsufficientAcksException.
         */
        final long setViolationMs = 3 * HA_TIMEOUT_MS;

        /*
         * Time to wait for restoring MAX_DISK to take effect.
         * For replicas this takes longer because the connection has
         * to be reestablished.
         */
        final long clearViolationMs = 5 * HA_TIMEOUT_MS;

        allowDiskLimitEx = false;
        expectDiskLimitEx = false;

        while (!stopFlag) {

            System.out.println("Steady state with no violations");

            allowDurabilityEx = false;
            expectDurabilityEx = false;

            runAndCheckExceptions(
                System.currentTimeMillis() + steadyStateMs);

            cleanLog();

            System.out.println("Start violations on ALL_REPLICAS");

            for (int i = 1; i < repEnvInfo.length; i += 1) {
                setDiskLimitViolation(repEnvInfo[i]);
            }

            allowDurabilityEx = true;
            expectDurabilityEx = true;

            runAndCheckExceptions(
                System.currentTimeMillis() + setViolationMs);

            System.out.println("Clear violations on ALL_REPLICAS");

            for (int i = 1; i < repEnvInfo.length; i += 1) {
                clearDiskLimitViolation(repEnvInfo[i]);
            }

            allowDurabilityEx = true;
            expectDurabilityEx = false;

            runAndCheckExceptions(
                System.currentTimeMillis() + clearViolationMs);

            for (final UpdateThread t : threads) {
                t.clearExceptions();
            }
        }
    }

    private void runAndCheckExceptions(final long stopTime)
        throws Throwable {

        if (stopTime > runStopTime) {
            stopFlag = true;
            return;
        }

        while (!stopFlag && System.currentTimeMillis() < stopTime) {

            Thread.sleep(1000);

            for (final UpdateThread t : threads) {

                if (!allowDiskLimitEx && t.diskLimitEx != null) {
                    throw new IllegalStateException(t.diskLimitEx);
                }

                if (!allowDurabilityEx && t.durabilityEx != null) {
                    throw new IllegalStateException(t.durabilityEx);
                }
            }
        }

        if (stopFlag) {
            return;
        }

        refreshStats();

        for (final UpdateThread t : threads) {

            if (expectDiskLimitEx && t.diskLimitEx == null) {
                throw new IllegalStateException(
                    "Expected DiskLimitException");
            }

            if (expectDurabilityEx && t.durabilityEx == null) {
                throw new IllegalStateException(
                    "Expected InsufficientAcksException or " +
                        "InsufficientReplicasException");
            }
        }

        if (expectDiskLimitEx || expectDurabilityEx) {
            assertTrue(lastExceptions > 0);
        }

        if (!allowDiskLimitEx && !allowDurabilityEx) {
            assertEquals(0, lastExceptions);
            assertTrue(lastOperations > 0);
        }
    }

    /**
     * During steady state, when we know there are no violations, give
     * cleaner/checkpointer a chance to run to completion.
     */
    private void cleanLog() {
        for (final RepEnvInfo info : repEnvInfo) {
            final Environment env = info.getEnv();
            env.cleanLog();
            env.checkpoint(new CheckpointConfig().setForce(true));
        }
    }

    @SuppressWarnings("unused")
    private void dumpThreads() {
        final EnvironmentImpl envImpl = repEnvInfo[0].getRepImpl();

        LoggerUtils.fullThreadDump(
            envImpl.getLogger(), envImpl, Level.SEVERE);
    }

    private void refreshStats() {

        final long curTime = System.currentTimeMillis();
        final long statMs = curTime - lastStatTime;

        if (statMs < 1000) {
            return;
        }

        long ops = 0;
        long exceptions = 0;

        for (final UpdateThread t : threads) {
            ops += t.getOps();
            exceptions += t.getExceptions();
        }

        lastStatTime = curTime;
        lastOperations = ops;
        lastExceptions = exceptions;

        final long statSec = statMs / 1000;
        final long opsPerSec = ops / statSec;
        final long exceptionsPerSec = exceptions / statSec;

        System.out.format(
            "Ops: %,d Exc: %,d Ops/s: %,d Exc/s: %,d %n",
            ops, exceptions, opsPerSec, exceptionsPerSec);

//        System.out.println(envImpl.getCleaner().getDiskLimitMessage());
//        final EnvironmentStats stats = masterEnv.getStats(null);
    }

    class UpdateThread extends Thread {

        private final int keysPerThread;
        private final int startKey;
        private volatile int ops = 0;
        private volatile int exceptions = 0;
        volatile DiskLimitException diskLimitEx;
        volatile OperationFailureException durabilityEx;

        UpdateThread(final int keysPerThread, final int startKey) {
            this.keysPerThread = keysPerThread;
            this.startKey = startKey;
        }

        int getOps() {
            final int ret = ops;
            ops = 0;
            return ret;
        }

        int getExceptions() {
            final int ret = exceptions;
            exceptions = 0;
            return ret;
        }

        void clearExceptions() {
            diskLimitEx = null;
            durabilityEx = null;
        }

        @Override
        public void run() {

            try {
                final DatabaseEntry key = new DatabaseEntry();

                final DatabaseEntry data =
                    new DatabaseEntry(new byte[DATA_SIZE]);

                while (!stopFlag) {
                    for (int i = startKey;
                         i < (startKey + keysPerThread) && !stopFlag;
                         i += 1) {

                        IntegerBinding.intToEntry(i, key);

                        try {
                            final OperationResult result = masterDb.put(
                                null, key, data, Put.OVERWRITE, null);
                            if (result == null) {
                                throw new IllegalStateException("put failed");
                            }
                            ops += 1;
                        } catch (DiskLimitException e) {
                            diskLimitEx = e;
                            exceptions += 1;
                        } catch (InsufficientAcksException|
                                InsufficientReplicasException e) {
                            durabilityEx = e;
                            exceptions += 1;
                        }
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace(System.out);
                unexpectedEx.compareAndSet(null, e);
                stopFlag = true;
            }
        }
    }
}
