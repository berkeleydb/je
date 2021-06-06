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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

/**
 * Tests SecondaryAssociation with complex associations.
 *
 * SecondaryTest tests SecondaryAssociation in the simple case where each
 * secondary is associated with a single primary. It performs a more exhaustive
 * API test.
 *
 * This test is focused on complex associations and concurrent operations.  It
 * includes:
 * - Multiple primary DBs per index
 * - Multiple "tables" per primary DB
 * - Incremental primary key deletion
 *
 * This test is intended to be run either as part of the unit test suite, or as
 * a longer running stress test when -Dlongtest=true is specified. In the
 * default mode, it runs in less than one minute but still exercises concurrent
 * operations to some degree.  When -Dlongtest=true is specified, it takes
 * around 15 minutes.
 *
 * For simplicity and speed of execution, this is not a DualTestCase because
 * SecondaryAssociation-with-HA testing is done by SecondaryTest.  TxnTestCase
 * is also not used to vary txn type; all operations are transactional.
 *
 * In this test, a many-many mapping between primaries and secondaries is
 * implemented as follows:
 * - Each primary key is 4 bytes long.
 * - A logical "table" is labeled by a primary key prefix Tn in the first two
 *   bytes of the key:  T0, T1, T2, etc.
 * - The next 2 bytes of the primary key are a randomly generated
 *   discriminator,  meaning that there are 64K maximum records per table.
 * - Primary records for all tables are spread among m primary DBs, and a
 *   primary key is hashed to determine the primary DB ID.
 * - Each table labeled Tn has n secondaries, e.g., T0 has no secondaries, and
 *   T5 has 4 secondaries.
 * - The secondaries have integer IDs from 0 to n-1, which are locally unique
 *   for each table.
 * - Each secondary key is one byte long.  It is extracted from the primary
 *   data at index N, where N is the secondary ID.
 *
 * It is the application's responsibility to guarantee that a primary or
 * secondary DB is not accessed after it is closed.  This test uses a "clean
 * cycle" mechanism to ensure that all in-progress operations on a DB are
 * completed after it is removed from the association, and before it is closed.
 * A clean cycle is defined as a complete operation based on current
 * information derived from the association.
 *
 * Limitations
 * ===========
 * Secondary addition/removal is not tested concurrently with primary
 * addition/removal, although these combinations should work in principle.
 */
public class SecondaryAssociationTest extends TestBase {
    private static final int N_TABLES;
    private static final int N_PRIMARIES;
    private static final int N_KEY_DISCRIMINATOR_BYTES = 2;
    private static final int SLEEP_MS_BETWEEN_PHASES;
    private static final boolean VERBOSE;

    static {
        if (SharedTestUtils.runLongTests()) {
            N_TABLES = 20;
            N_PRIMARIES = 50;
            SLEEP_MS_BETWEEN_PHASES = 60 * 1000;
            VERBOSE = true;
        } else {
            N_TABLES = 3;
            N_PRIMARIES = 20;
            SLEEP_MS_BETWEEN_PHASES = 1000;
            VERBOSE = false;
        }
    }

    private final Random rnd;
    private final AtomicBoolean shutdownFlag;
    private final AtomicReference<Throwable> failureException;
    private final AtomicInteger nWrites;
    private final AtomicInteger nInserts;
    private final AtomicInteger nUpdates;
    private final AtomicInteger nDeletes;
    private final MyAssociation assoc;
    private final File envHome = SharedTestUtils.getTestDir();
    private Environment env;
    private ExecutorService executor;
    private volatile int removedPriId = -1;
    private volatile int addedPriId = -1;
    private volatile Database addedPriDb;

    public SecondaryAssociationTest() {
        rnd = new Random(123);
        shutdownFlag = new AtomicBoolean(false);
        failureException = new AtomicReference<Throwable>(null);
        nWrites = new AtomicInteger(0);
        nInserts = new AtomicInteger(0);
        nUpdates = new AtomicInteger(0);
        nDeletes = new AtomicInteger(0);
        assoc = new MyAssociation();
        executor = Executors.newCachedThreadPool();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();

        final EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        config.setTransactional(true);
        config.setDurability(Durability.COMMIT_NO_SYNC);

        /* Avoid lock timeouts on slow test machines. */
        config.setLockTimeout(5, TimeUnit.SECONDS);

        env = new Environment(envHome, config);
    }

    @After
    public void tearDown()
        throws Exception {

        /* Ensure resources are released for the sake of tests that follow. */
        try {
            if (executor != null) {
                executor.shutdownNow();
            }
        } finally {
            executor = null;
            try {
                if (env != null) {
                    env.close();
                }
            } finally {
                env = null;
                /* Always call superclass method. */
                super.tearDown();
            }
        }
    }

    @Test
    public void concurrentTests()
        throws InterruptedException, ExecutionException, TimeoutException {

        /* Sleep calls are to let writes/verify run between stages. */
        createAllTables();
        final TaskMonitor writeMonitor = startPrimaryWrites();
        final TaskMonitor verifyMonitor = startVerify();
        waitForFullPrimaries();
        addSecondaries();
        Thread.sleep(SLEEP_MS_BETWEEN_PHASES);
        removeOnePrimary(writeMonitor, verifyMonitor);
        Thread.sleep(SLEEP_MS_BETWEEN_PHASES);
        addOnePrimary(writeMonitor, verifyMonitor);
        Thread.sleep(SLEEP_MS_BETWEEN_PHASES);
        removeSecondaries(writeMonitor, verifyMonitor);
        Thread.sleep(SLEEP_MS_BETWEEN_PHASES);
        writeMonitor.stop();
        verifyMonitor.stop();
        shutdown();
        closeAllTables();
        checkFailure();
    }

    private void createAllTables() {
        for (int tableId = 0; tableId < N_TABLES; tableId += 1) {
            assoc.addTable(tableId);
        }
        for (int priId = 0; priId < N_PRIMARIES; priId += 1) {
            final Database db = openPrimaryDatabase(priId);
            assoc.addPrimary(priId, db);
        }
    }

    private Database openPrimaryDatabase(final int priId) {
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setExclusiveCreate(true);
        dbConfig.setSecondaryAssociation(assoc);
        return env.openDatabase(null, "P" + priId, dbConfig);
    }

    private void closeAllTables() {
        for (final Database db : assoc.getAllPrimaries()) {
            db.close();
        }
        for (final SecondaryDatabase secDb : assoc.getAllSecondaries()) {
            secDb.close();
        }
    }

    private void addSecondaries() {
        if (VERBOSE) {
            System.out.println("Start adding secondaries");
        }
        for (int secId = 0; secId < N_TABLES; secId += 1) {
            /* Add one secondary (at most) to each table. */
            final Collection<SecondaryDatabase> dbsAdded =
                new ArrayList<SecondaryDatabase>();
            for (int tableId = 0; tableId < N_TABLES; tableId += 1) {
                if (secId >= tableId) {
                    continue;
                }
                final SecondaryConfig dbConfig = new SecondaryConfig();
                dbConfig.setTransactional(true);
                dbConfig.setAllowCreate(true);
                dbConfig.setExclusiveCreate(true);
                dbConfig.setSecondaryAssociation(assoc);
                dbConfig.setKeyCreator(new MyKeyCreator(secId));
                dbConfig.setSortedDuplicates(true);
                final SecondaryDatabase db = env.openSecondaryDatabase(
                    null, "T" + tableId + "S" + secId, null, dbConfig);
                /* Enable incremental mode BEFORE adding to association. */
                db.startIncrementalPopulation();
                assoc.addSecondary(tableId, secId, db);
                dbsAdded.add(db);
                checkFailure();
            }
            /* Populate the secondaries we just created. */
            for (final Database db : assoc.getAllPrimaries()) {
                final DatabaseEntry keyEntry = new DatabaseEntry();
                final DatabaseEntry dataEntry = new DatabaseEntry();
                final Cursor cursor = db.openCursor(
                    null, CursorConfig.READ_COMMITTED);
                OperationResult result;
                while ((result = cursor.get(
                    keyEntry, dataEntry, Get.NEXT, null)) != null) {
                    db.populateSecondaries(
                        null, keyEntry, dataEntry, result.getExpirationTime(),
                        null);
                }
                cursor.close();
            }
            /* Disable incremental mode now that population is complete. */
            for (final SecondaryDatabase db : dbsAdded) {
                db.endIncrementalPopulation();
            }
            if (VERBOSE) {
                System.out.format("Added %d secondaries after %,d writes\n",
                                  dbsAdded.size(), nWrites.get());
            }
        }
        if (VERBOSE) {
            System.out.println("Done adding secondaries");
        }
    }

    private void removeSecondaries(final TaskMonitor writeMonitor,
                                   final TaskMonitor verifyMonitor)
        throws InterruptedException {

        if (VERBOSE) {
            System.out.println("Start removing secondaries");
        }
        for (int tableId = 0; tableId < N_TABLES; tableId += 1) {
            for (int secId = 0; secId < tableId; secId += 1) {
                /* 1. Remove from association. */
                final SecondaryDatabase db =
                    assoc.removeSecondary(tableId, secId);
                /* 2. Wait for in-progress operations to complete. */
                writeMonitor.waitForCleanCycle();
                verifyMonitor.waitForCleanCycle();
                /* 3. Close/remove database. */
                final String dbName = db.getDatabaseName();
                db.close();
                env.removeDatabase(null, dbName);
                checkFailure();
            }
            assertEquals(0, assoc.getSecondaries(tableId).size());
        }
        if (VERBOSE) {
            System.out.println("Done removing secondaries");
        }
    }

    private void removeOnePrimary(final TaskMonitor writeMonitor,
                                  final TaskMonitor verifyMonitor)
        throws InterruptedException {

        if (VERBOSE) {
            System.out.println("Start removing primary");
        }

        /*
         * 1. Remove from association.
         *
         * Remove last primary, as it has the most secondaries. removedPriId is
         * set as an indicator that this DB should not longer be used for
         * verify/writes.
         */
        removedPriId = N_PRIMARIES - 1;
        final Database db = assoc.removePrimary(removedPriId);
        final long recCount = db.count();

        if (VERBOSE) {
            System.out.println("Wait for removed primary operations to stop");
        }

        /* 2. Wait for in-progress operations to complete. */
        writeMonitor.waitForCleanCycle();
        verifyMonitor.waitForCleanCycle();

        if (VERBOSE) {
            System.out.format("Close and remove primary DB with %,d records\n",
                              recCount);
        }

        /* 3. Close/remove database. */
        final String dbName = db.getDatabaseName();
        db.close();
        env.removeDatabase(null, dbName);
        if (VERBOSE) {
            System.out.println("Delete obsolete primary keys");
        }
        for (final SecondaryDatabase secDb : assoc.getAllSecondaries()) {
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();
            while (secDb.deleteObsoletePrimaryKeys(keyEntry, dataEntry, 100)) {
                checkFailure();
            }
        }
        if (VERBOSE) {
            System.out.println("Done removing primary");
        }
    }

    private void addOnePrimary(final TaskMonitor writeMonitor,
                               final TaskMonitor verifyMonitor)
        throws InterruptedException {

        if (VERBOSE) {
            System.out.println("Start adding primary");
        }

        assertTrue(removedPriId >= 0);
        assertTrue(addedPriId < 0);
        assertNull(addedPriDb);

        addedPriDb = openPrimaryDatabase(addedPriId);
        addedPriId = removedPriId;

        final int initialWrites = nWrites.get();
        while (nWrites.get() - initialWrites < 100000) {
            Thread.sleep(10);
            checkFailure();
        }

        final long recCount = addedPriDb.count();

        assoc.addPrimary(addedPriId, addedPriDb);

        removedPriId = -1;
        addedPriId = -1;
        addedPriDb = null;

        if (VERBOSE) {
            System.out.format("Done adding primary, wrote %,d\n", recCount);
        }
    }

    /**
     * Starts two threads to do writes.
     *
     * Waits for at least 500 writes before returning, to ensure the next step
     * is done concurrently with writing.
     */
    private TaskMonitor startPrimaryWrites()
        throws InterruptedException {

        final AtomicBoolean stopTaskFlag = new AtomicBoolean(false);

        class WriteTask extends Task {
            private final AtomicInteger cleanCycle;
            private final String label;

            WriteTask(final AtomicInteger cleanCycle, final String label) {
                this.cleanCycle = cleanCycle;
                this.label = label;
            }

            public void execute() {
                runPrimaryWrites(stopTaskFlag, cleanCycle, label);
            }
        }

        final AtomicInteger cleanCycle1 = new AtomicInteger(0);
        final AtomicInteger cleanCycle2 = new AtomicInteger(0);
        final Runnable task1 = new WriteTask(cleanCycle1, "t1");
        final Runnable task2 = new WriteTask(cleanCycle2, "t2");
        final Future<?> future1 = executor.submit(task1);
        final Future<?> future2 = executor.submit(task2);

        final int initialWrites = nWrites.get();
        while (nWrites.get() - initialWrites < 500) {
            Thread.sleep(10);
            checkFailure();
        }

        final TaskMonitor taskMonitor = new TaskMonitor(stopTaskFlag);
        taskMonitor.add(future1, cleanCycle1);
        taskMonitor.add(future2, cleanCycle2);
        return taskMonitor;
    }

    /**
     * Writes randomly generated primary records until shutdown/stop.
     *
     * Since the keyspace is small (64K maximum keys per table), this will
     * eventually do updates as well as inserts.  For 1/5 records, they are
     * immediately deleted after being written.
     */
    private void runPrimaryWrites(final AtomicBoolean stopTaskFlag,
                                  final AtomicInteger cleanCycle,
                                  final String label) {
        /* Key and data are fixed length. */
        final byte[] keyBytes =
            new byte[2 + N_KEY_DISCRIMINATOR_BYTES];
        final byte[] dataBytes = new byte[N_TABLES];
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        /* First byte of key is fixed. */
        keyBytes[0] = 'T';
        /* Write until shutdown or stopped. */
        while (true) {
            for (int tableId = 0; tableId < N_TABLES; tableId += 1) {
                if (shutdownFlag.get() || stopTaskFlag.get()) {
                    return;
                }
                /* Second byte of key is table ID. */
                keyBytes[1] = (byte) tableId;
                /* Rest of key is random. */
                for (int j = 2; j < keyBytes.length; j += 1) {
                    keyBytes[j] = (byte) rnd.nextInt(256);
                }
                /* Insert or update with random data. */
                keyEntry.setData(keyBytes);
                dataEntry.setData(dataBytes);
                Database priDb = assoc.getPrimary(keyEntry);
                if (priDb == null) {
                    final int priId = getPrimaryId(keyEntry);
                    if (priId == addedPriId) {
                        priDb = addedPriDb;
                    } else {
                        assertEquals(removedPriId, priId);
                        cleanCycle.incrementAndGet();
                        continue;
                    }
                }
                rnd.nextBytes(dataBytes);
                if (priDb.putNoOverwrite(null, keyEntry, dataEntry) ==
                    OperationStatus.SUCCESS) {
                    nInserts.incrementAndGet();
                } else {
                    priDb.put(null, keyEntry, dataEntry);
                    nUpdates.incrementAndGet();
                }
                /* Delete 1/5 records written. */
                if (rnd.nextInt(5) == 1) {
                    priDb.delete(null, keyEntry);
                    nDeletes.incrementAndGet();
                }
                nWrites.incrementAndGet();
                if (VERBOSE && (nWrites.get() % 100000 == 0)) {
                    printWriteTotals(label);
                }
                cleanCycle.incrementAndGet();
            }
        }
    }

    /**
     * Waits for updates to be at least 1/5 of all writes, meaning that the
     * keyspace for the primaries has been populated.
     */
    private void waitForFullPrimaries()
        throws InterruptedException {

        while (4.0 * nUpdates.get() < nInserts.get()) {
            Thread.sleep(10);
            checkFailure();
        }
        if (VERBOSE) {
            printWriteTotals("");
        }
    }

    /**
     * Starts one thread to do verification.
     */
    private TaskMonitor startVerify() {

        final AtomicBoolean stopTaskFlag = new AtomicBoolean(false);
        final AtomicInteger nPriVerified = new AtomicInteger(0);
        final AtomicInteger nSecVerified = new AtomicInteger(0);
        final AtomicInteger cleanCycles = new AtomicInteger(0);
        final Runnable task = new Task() {
            public void execute() {
                while (!shutdownFlag.get() && !stopTaskFlag.get()) {
                    runVerify(stopTaskFlag, cleanCycles,
                              nPriVerified, nSecVerified);
                }
            }
        };

        final Future<?> future = executor.submit(task);
        final TaskMonitor taskMonitor = new TaskMonitor(stopTaskFlag);
        taskMonitor.add(future, cleanCycles);
        return taskMonitor;
    }

    /**
     * Checks primary-secondary linkages/integrity, namely that a primary
     * record contains secondary keys matching the records present in the
     * secondary databases.
     */
    private void runVerify(final AtomicBoolean stopTaskFlag,
                           final AtomicInteger cleanCycles,
                           final AtomicInteger nPriVerified,
                           final AtomicInteger nSecVerified) {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final DatabaseEntry secKeyEntry = new DatabaseEntry();
        final DatabaseEntry noReturnData = new DatabaseEntry();
        noReturnData.setPartial(0, 0, true);

        for (int priId = 0; priId < N_PRIMARIES; priId += 1) {
            final Database db = assoc.getPrimary(priId);
            if (db == null) {
                assertEquals(removedPriId, priId);
                continue;
            }
            final Cursor c = db.openCursor(null, CursorConfig.READ_COMMITTED);
            try {
                while (c.getNext(keyEntry, dataEntry, null) ==
                       OperationStatus.SUCCESS) {
                    if (assoc.getPrimary(priId) == null) {
                        break;
                    }
                    final int tableId = keyEntry.getData()[1];
                    final byte[] dataBytes = dataEntry.getData();
                    for (int secId = 0; secId < tableId; secId += 1) {
                        if (shutdownFlag.get() || stopTaskFlag.get()) {
                            return;
                        }
                        final SecondaryDatabase secDb =
                            assoc.getSecondary(tableId, secId);
                        if (secDb == null ||
                            secDb.isIncrementalPopulationEnabled()) {
                            continue;
                        }
                        secKeyEntry.setData(new byte[] {dataBytes[secId]});
                        final OperationStatus status = secDb.getSearchBoth(
                            null, secKeyEntry, keyEntry, noReturnData,
                            LockMode.READ_UNCOMMITTED);
                        if (OperationStatus.SUCCESS != status) {
                            if (assoc.getPrimary(priId) == null) {
                                break;
                            }
                            fail("Sec key missing " + status + ' ' +
                                 secDb.getDatabaseName() + ' ' + priId + ' ' +
                                 secKeyEntry + ' ' + keyEntry);
                        }
                    }
                    nPriVerified.incrementAndGet();
                    if (VERBOSE && nPriVerified.get() % 500000 == 0) {
                        System.out.format("nPriVerified %,d\n",
                                          nPriVerified.get());
                    }
                }
            } finally {
                c.close();
            }
            cleanCycles.incrementAndGet();
        }

        /*
         * TODO: Perform with normal locking rather than dirty-read, once the
         * deadlock-free secondary feature is implemented.
         */
        for (int tableId = 0; tableId < N_TABLES; tableId += 1) {
            for (int secId = 0; secId < tableId; secId += 1) {
                final SecondaryDatabase secDb =
                    assoc.getSecondary(tableId, secId);
                if (secDb == null ||
                    secDb.isIncrementalPopulationEnabled()) {
                    continue;
                }
                final SecondaryCursor c =
                    secDb.openCursor(null, CursorConfig.READ_UNCOMMITTED);
                try {
                    while (c.getNext(secKeyEntry, keyEntry, dataEntry, null) ==
                           OperationStatus.SUCCESS) {
                        if (shutdownFlag.get() || stopTaskFlag.get()) {
                            return;
                        }
                        assertEquals(tableId, keyEntry.getData()[1]);
                        assertEquals(dataEntry.getData()[secId],
                                     secKeyEntry.getData()[0]);
                        nSecVerified.incrementAndGet();
                        if (VERBOSE && nSecVerified.get() % 500000 == 0) {
                            System.out.format("nSecVerified %,d\n",
                                              nSecVerified.get());
                        }
                    }
                } finally {
                    c.close();
                }
                cleanCycles.incrementAndGet();
            }
        }
    }

    private class TaskMonitor {
        private final AtomicBoolean stopFlag;
        private final List<Future<?>> futures;
        private final List<AtomicInteger> cleanCycles;

        TaskMonitor(final AtomicBoolean stopFlag) {
            this.stopFlag = stopFlag;
            futures = new ArrayList<Future<?>>();
            cleanCycles = new ArrayList<AtomicInteger>();
        }

        void add(final Future<?> future, final AtomicInteger cleanCycle) {
            futures.add(future);
            cleanCycles.add(cleanCycle);
        }

        void waitForCleanCycle()
            throws InterruptedException {

            final int[] prevCleanCycles = new int[cleanCycles.size()];
            for (int i = 0; i < prevCleanCycles.length; i += 1) {
                prevCleanCycles[i] = cleanCycles.get(i).get();
            }
            while (true) {
                boolean allDone = true;
                for (int i = 0; i < prevCleanCycles.length; i += 1) {
                    if (prevCleanCycles[i] >= cleanCycles.get(i).get()) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) {
                    break;
                }
                Thread.sleep(10);
                checkFailure();
            }
        }

        void stop()
            throws InterruptedException, ExecutionException, TimeoutException {

            stopFlag.set(true);
            for (final Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Saves first exception encountered, which also serves as a failure
     * indicator -- non-null means failure.
     */
    private void noteFailure(Throwable t) {

        t.printStackTrace(System.out);
        failureException.compareAndSet(null, t);
    }

    /**
     * If an exception caused a failure, throw it so it appears as the cause of
     * the JUnit test failure.  This method is meant to be called from the
     * main thread, i.e., the one running the JUnit test.
     */
    private void checkFailure() {
        final Throwable t = failureException.get();
        if (t == null) {
            return;
        }
        throw new IllegalStateException(
            "See cause exception. Other exceptions in output may also be " +
            "related.", t);
    }

    /**
     * A Runnable that calls an execute() method, that is implemented by the
     * caller, and handles exceptions.
     */
    private abstract class Task implements Runnable {

        public void run() {
            try {
                execute();
            } catch (Throwable t) {
                noteFailure(t);
            }
        }

        abstract void execute() throws Throwable;
    }

    private void shutdown()
        throws InterruptedException {

        shutdownFlag.set(true);
        executor.shutdown();
        if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            throw new IllegalStateException(
                "Could not terminate executor normally");
        }
        if (VERBOSE) {
            printWriteTotals("final");
        }
        checkFailure();
    }

    private void printWriteTotals(final String label) {
        System.out.format(
            "%s nWrites %,d nInserts %,d, nUpdates %,d nDeletes %,d\n", label,
            nWrites.get(), nInserts.get(), nUpdates.get(), nDeletes.get());
    }

    /**
     * Performs a simplistic (not very evenly distributed) hash of the primary
     * key to get a primary DB ID between zero and (N_PRIMARIES - 1).  For
     * this to work best, the primary key should contain some randomly
     * generated values.
     */
    private static int getPrimaryId(final DatabaseEntry primaryKey) {
        int sum = 0;
        final byte[] data = primaryKey.getData();
        for (int i = 0; i < data.length; i += 1) {
            sum += data[i];
        }
        return Math.abs(sum % N_PRIMARIES);
    }

    /**
     * Creates a secondary key from the Nth byte of the primary data, where
     * N is the secondary ID passed to the constructor.
     *
     * TODO replace with new SecondaryKeyExtractor when available.
     */
    private static class MyKeyCreator implements SecondaryKeyCreator {
        private final int secId;

        MyKeyCreator(int secId) {
            this.secId = secId;
        }

        public boolean createSecondaryKey(SecondaryDatabase secondary,
                                          DatabaseEntry key,
                                          DatabaseEntry data,
                                          DatabaseEntry result) {
            result.setData(new byte[] {data.getData()[secId]});
            return true;
        }
    }

    /**
     * This class implements a SecondaryAssociation in a semi-realistic manner,
     * simulating an app that maintains associations per logical table.
     *
     * However, in a real app, it is expected that the association metadata
     * would be maintained separately and accessed in a read-only manner via
     * this class. In other words, this class might not contain methods for
     * adding and removing members in the association.
     *
     * Non-blocking data structures are used to hold association info, to avoid
     * blocking on the methods in SecondaryAssociation, which are frequently
     * called by many threads.
     */
    private static class MyAssociation implements SecondaryAssociation {

        /* Maps a primary DB ID to the primary DB. */
        private final Map<Integer, Database> primaries =
            new ConcurrentHashMap<Integer, Database>();

        /* Maps a table ID to its associated secondaries. */
        private final Map<Integer, Map<Integer, SecondaryDatabase>> tables =
            new ConcurrentHashMap<Integer, Map<Integer, SecondaryDatabase>>();

        /* Cheap-to-read indicator that any secondary DBs are present. */
        private final AtomicInteger nSecondaries = new AtomicInteger(0);

        public boolean isEmpty() {
            return (nSecondaries.get() == 0);
        }

        public Database getPrimary(final DatabaseEntry primaryKey) {
            final int priId = getPrimaryId(primaryKey);
            return getPrimary(priId);
        }

        public Collection<SecondaryDatabase> getSecondaries(
            final DatabaseEntry primaryKey) {

            final int tableId = primaryKey.getData()[1];
            return getSecondaries(tableId);
        }

        Collection<SecondaryDatabase> getSecondaries(final int tableId) {
            final Map<Integer, SecondaryDatabase> secondaries =
                tables.get(tableId);
            assertNotNull(secondaries);
            return secondaries.values();
        }

        Database getPrimary(final int priId) {
            assertTrue(String.valueOf(priId), priId >= 0);
            assertTrue(String.valueOf(priId), priId < N_PRIMARIES);
            return primaries.get(priId);
        }

        void addPrimary(final int priId, final Database priDb) {
            final Object oldVal = primaries.put(priId, priDb);
            assertNull(oldVal);
        }

        Database removePrimary(final int priId) {
            final Database db = primaries.remove(priId);
            assertNotNull(db);
            return db;
        }

        void addTable(final int tableId) {
            final Map<Integer, SecondaryDatabase> secondaries =
                new ConcurrentHashMap<Integer, SecondaryDatabase>();
            final Object oldVal = tables.put(tableId, secondaries);
            assertNull(oldVal);
        }

        SecondaryDatabase getSecondary(final int tableId, final int secId) {
            final Map<Integer, SecondaryDatabase> secondaries =
                tables.get(tableId);
            assertNotNull(secondaries);
            final SecondaryDatabase secDb = secondaries.get(secId);
            return secDb;
        }

        void addSecondary(final int tableId,
                          final int secId,
                          final SecondaryDatabase secDb) {
            final Map<Integer, SecondaryDatabase> secondaries =
                tables.get(tableId);
            assertNotNull(secondaries);
            final Object oldVal = secondaries.put(secId, secDb);
            assertNull(oldVal);
            nSecondaries.incrementAndGet();
        }

        SecondaryDatabase removeSecondary(final int tableId, final int secId) {
            final Map<Integer, SecondaryDatabase> secondaries =
                tables.get(tableId);
            assertNotNull(secondaries);
            final SecondaryDatabase secDb = secondaries.remove(secId);
            assertNotNull(secDb);
            nSecondaries.decrementAndGet();
            return secDb;
        }

        Collection<Database> getAllPrimaries() {
            return primaries.values();
        }

        Collection<SecondaryDatabase> getAllSecondaries() {
            final Collection<SecondaryDatabase> dbs =
                new ArrayList<SecondaryDatabase>();
            for (final Map<Integer, SecondaryDatabase> secondaries :
                 tables.values()) {
                dbs.addAll(secondaries.values());
            }
            return dbs;
        }
    }
}
