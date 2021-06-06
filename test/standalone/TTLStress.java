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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.test.SpeedyTTLTime;
import com.sleepycat.je.utilint.TracerFormatter;

/**
 * Tests concurrent access when using TTL.
 *
 * Goals:
 *  - Read records that are expiring.
 *  - Delete records that are expiring.
 *  - Update TTL for records that are expiring, including changing TTL to zero.
 *  - Lock a record multiple times per txn.
 *  - Read key then data in separate ops.
 *  - Use secondaries that expire.
 *  - Read primary and secondary records individually in separate ops.
 *  - Verify expected data.
 *  - Tolerate only expected deviant behavior, only on expiration boundaries.
 */
public class TTLStress {

    private static final WriteOptions ONE_HOUR_TTL =
        new WriteOptions().setTTL(1, TimeUnit.HOURS).setUpdateTTL(true);

    /* Must be at least long enough to empty the full queue. */
    private static final int TERMINATION_SEC = 10 * 60;

    /* Since a TTL of 1 hour is used, this is the TTL in millis. */
    private static final int FAKE_MILLIS_PER_HOUR = 100;

    /*
     * Must be at least the time for a thread to wake up and do an insert. The
     * insert and a read operation for that record are queued one after the
     * other and may both be assigned to threads at about the same time.
     */
    private static final int THREAD_SWITCH_TIME = 5000;

    /*
     * The read operation expects to read the record, and then for it to
     * expire, all before EXPIRATION_MAX_MILLIS.
     */
    private static final int EXPIRATION_MAX_MILLIS =
        (FAKE_MILLIS_PER_HOUR * 2) + THREAD_SWITCH_TIME;

    private static final DateFormat DATE_FORMAT =
        TracerFormatter.makeDateFormat();

    private static final int DEFAULT_TEST_THREADS = 10;
    private static final int DEFAULT_CLEANER_THREADS = 2;
    private static final int DEFAULT_DURATION_MINUTES = 30;
    private static final int DEFAULT_MAIN_CACHE_MB = 200;
    private static final int DEFAULT_OFFHEAP_CACHE_MB = 200;
    private static final int DEFAULT_QUEUE_SIZE = 1000;
    private static final String DEFAULT_HOME_DIR = "tmp";

    public static void main(final String[] args) {
        try {
            printArgs(args);
            final TTLStress test = new TTLStress(args);
            test.runTest();
            test.close();
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(-1);
        }
    }

    private Environment env;
    private Database db;
    private SecondaryDatabase secDb;
    private int durationMinutes = DEFAULT_DURATION_MINUTES;
    private ThreadPoolExecutor executor;
    private final SpeedyTTLTime speedyTime =
        new SpeedyTTLTime(FAKE_MILLIS_PER_HOUR);
    private final AtomicInteger nInserts = new AtomicInteger(0);
    private final AtomicInteger nUpdates = new AtomicInteger(0);
    private final AtomicInteger nDeletions = new AtomicInteger(0);
    private final AtomicInteger nDeleteExpired = new AtomicInteger(0);
    private final AtomicInteger nPriReads = new AtomicInteger(0);
    private final AtomicInteger nPriExpired = new AtomicInteger(0);
    private final AtomicInteger nPriExpiredData = new AtomicInteger(0);
    private final AtomicInteger nPriDeleted = new AtomicInteger(0);
    private final AtomicInteger nPriNotFound = new AtomicInteger(0);
    private final AtomicInteger nSecReads = new AtomicInteger(0);
    private final AtomicInteger nSecExpired = new AtomicInteger(0);
    private final AtomicInteger nSecExpiredData = new AtomicInteger(0);
    private final AtomicInteger nSecDeleted = new AtomicInteger(0);
    private final AtomicInteger nSecNotFound = new AtomicInteger(0);

    private TTLStress(String[] args) {

        int nTestThreads = DEFAULT_TEST_THREADS;
        int queueSize = DEFAULT_QUEUE_SIZE;
        int nCleanerThreads = DEFAULT_CLEANER_THREADS;
        int mainCacheMb = DEFAULT_MAIN_CACHE_MB;
        int offheapCacheMb = DEFAULT_OFFHEAP_CACHE_MB;
        String homeDir = DEFAULT_HOME_DIR;

        /* Parse arguments. */
        for (int i = 0; i < args.length; i += 1) {
            final String arg = args[i];
            final boolean moreArgs = i < args.length - 1;
            if (arg.equals("-h") && moreArgs) {
                homeDir = args[++i];
            } else if (arg.equals("-threads") && moreArgs) {
                nTestThreads = Integer.parseInt(args[++i]);
            } else if (arg.equals("-cleaners") && moreArgs) {
                nCleanerThreads = Integer.parseInt(args[++i]);
            } else if (arg.equals("-minutes") && moreArgs) {
                durationMinutes = Integer.parseInt(args[++i]);
            } else if (arg.equals("-cacheMB") && moreArgs) {
                mainCacheMb = Integer.parseInt(args[++i]);
            } else if (arg.equals("-offheapMB") && moreArgs) {
                offheapCacheMb = Integer.parseInt(args[++i]);
            } else if (arg.equals("-queueSize") && moreArgs) {
                queueSize = Integer.parseInt(args[++i]);
            } else {
                throw new IllegalArgumentException("Unknown arg: " + arg);
            }
        }

        executor = new ThreadPoolExecutor(
            nTestThreads, nTestThreads,
            0L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(queueSize),
            new ThreadPoolExecutor.AbortPolicy());

        executor.prestartAllCoreThreads();

        open(homeDir, nCleanerThreads, mainCacheMb, offheapCacheMb);
    }

    private static void printArgs(String[] args) {
        System.out.print("\nCommand line arguments:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
    }

    private void open(final String homeDir,
                      final int nCleanerThreads,
                      final int mainCacheMb,
                      final int offheapCacheMb) {

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX,
            String.valueOf(1024 * 1024));
        envConfig.setConfigParam(
            EnvironmentConfig.CLEANER_THREADS,
            String.valueOf(nCleanerThreads));
        envConfig.setCacheSize(mainCacheMb * (1024 * 1024));
        envConfig.setOffHeapCacheSize(offheapCacheMb * (1024 * 1024));
        /* Account for very slow test machines. */
        envConfig.setLockTimeout(30, TimeUnit.SECONDS);

        env = new Environment(new File(homeDir), envConfig);

        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);

        db = env.openDatabase(null, "priDb", dbConfig);

        final SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(true);
        secConfig.setTransactional(true);
        secConfig.setCacheMode(CacheMode.EVICT_LN);
        secConfig.setSortedDuplicates(true);
        secConfig.setKeyCreator(new SecondaryKeyCreator() {
            @Override
            public boolean createSecondaryKey(final SecondaryDatabase secDb,
                                              final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final DatabaseEntry result) {
                result.setData(key.getData());
                return true;
            }
        });

        secDb = env.openSecondaryDatabase(null, "secDb", db, secConfig);
    }

    private void close() throws InterruptedException {

        log("Starting shutdown");

        executor.shutdown();

        if (!executor.awaitTermination(TERMINATION_SEC, TimeUnit.SECONDS)) {

            System.out.println(
                "Could not terminate gracefully after " +
                TERMINATION_SEC + " seconds");

            final List<Runnable> stillRunning = executor.shutdownNow();

            if (!stillRunning.isEmpty()) {

                System.out.println(
                    "Did not empty queue during close after " +
                    TERMINATION_SEC + " seconds, " + stillRunning.size() +
                    " tasks still running.");

                System.exit(1);
            }
        }

        secDb.close();
        db.close();
        env.close();

        log(String.format(
            "Test succeeded %n" +
            "nInserts: %,d %n" +
            "nUpdates: %,d %n" +
            "nDeletions: %,d %n" +
            "nPriReads: %,d %n" +
            "nPriExpired: %,d %n" +
            "nPriExpiredData: %,d %n" +
            "nPriDeleted: %,d %n" +
            "nPriNotFound: %,d %n" +
            "nSecReads: %,d %n" +
            "nSecExpired: %,d %n" +
            "nSecExpiredData: %,d %n" +
            "nSecDeleted: %,d %n" +
            "nSecNotFound: %,d %n" +
            "nDeleteExpired: %,d ",
            nInserts.get(),
            nUpdates.get(),
            nDeletions.get(),
            nPriReads.get(),
            nPriExpired.get(),
            nPriExpiredData.get(),
            nPriDeleted.get(),
            nPriNotFound.get(),
            nSecReads.get(),
            nSecExpired.get(),
            nSecExpiredData.get(),
            nSecDeleted.get(),
            nSecNotFound.get(),
            nDeleteExpired.get()));
    }

    private static void log(final String msg) {
        synchronized (DATE_FORMAT) {
            System.out.println(
                DATE_FORMAT.format(System.currentTimeMillis()) + " " + msg);
        }
    }

    private void runTest() throws Throwable {

        final long endTime =
            System.currentTimeMillis() + (durationMinutes * 60 * 1000);

        final Random rnd = new Random(123);
        final BlockingQueue<Runnable> queue = executor.getQueue();

        speedyTime.start();

        while (System.currentTimeMillis() < endTime) {

            final boolean doDelete;
            final boolean doUpdate;
            int nOps = 3; // insert, read primary and secondary

            switch (rnd.nextInt(3)) {
            case 0:
                doDelete = false;
                doUpdate = false;
                break;
            case 1:
                doDelete = false;
                doUpdate = true;
                nOps += 1;
                break;
            case 2:
                doDelete = true;
                doUpdate = false;
                nOps += 1;
                break;
            default:
                throw new RuntimeException();
            }

            if (queue.remainingCapacity() < nOps) {
                continue;
            }

            final long key = rnd.nextLong();
            final AtomicBoolean abandonOp = new AtomicBoolean(false);
            final AtomicLong expirationTime = new AtomicLong(0);

            executor.execute(new Write(
                key, abandonOp, expirationTime, !doUpdate /*doInsert*/));

            executor.execute(new PrimaryRead(
                key, abandonOp, expirationTime, doDelete));

            executor.execute(new SecondaryRead(
                key, abandonOp, expirationTime, doDelete, doUpdate));

            if (doUpdate) {
                executor.execute(new Write(
                    key, abandonOp, expirationTime, false /*doInsert*/));
            }

            if (doDelete) {
                executor.execute(new Delete(
                    key, abandonOp, expirationTime));
            }
        }
    }

    private class Write implements Runnable {

        private static final boolean DEBUG_WRITE = false;

        private final long key;
        private final AtomicBoolean abandonOp;
        private final AtomicLong expirationTime;
        private final boolean doInsert;

        Write(final long key,
              final AtomicBoolean abandonOp,
              final AtomicLong expirationTime,
              final boolean doInsert) {
            this.key = key;
            this.abandonOp = abandonOp;
            this.doInsert = doInsert;
            this.expirationTime = expirationTime;
        }

        @Override
        public void run() {

            Thread.currentThread().setName("Write");

            try {
                final DatabaseEntry keyEntry = new DatabaseEntry();
                LongBinding.longToEntry(key, keyEntry);

                final boolean useSeparateLN = (key & 1) != 0;
                final byte[] dataBytes = new byte[useSeparateLN ? 20 : 8];
                final DatabaseEntry dataEntry = new DatabaseEntry();

                TupleBase.outputToEntry(
                    new TupleOutput(dataBytes).writeLong(key),
                    dataEntry);

                final Transaction txn = env.beginTransaction(null, null);

                final long opBeforeTime =
                    speedyTime.realTimeToFakeTime(System.currentTimeMillis());

                OperationResult result = db.put(
                    txn, keyEntry, dataEntry,
                    doInsert ? Put.NO_OVERWRITE : Put.OVERWRITE,
                    ONE_HOUR_TTL);

                final long opAfterTime =
                    speedyTime.realTimeToFakeTime(System.currentTimeMillis());

                if (result == null && doInsert) {
                    log("Apparent duplicate random number as key: " + key);
                    txn.abort();
                    abandonOp.set(true);
                    return;
                }

                assertNotNull("Could not write: " + key, result);

                final long opBeforeExpTime =
                    writeTimeToExpirationTime(opBeforeTime);

                final long opAfterExpTime =
                    writeTimeToExpirationTime(opAfterTime);

                final long resultExpTime = result.getExpirationTime();
                boolean resultExpTimeMatches = false;

                for (long time = opBeforeExpTime;
                     time <= opAfterExpTime;
                     time += TTL.MILLIS_PER_HOUR) {

                    if (resultExpTime == time) {
                        resultExpTimeMatches = true;
                        break;
                    }
                }

                if (!resultExpTimeMatches) {
                    fail(
                        "key: " + key +
                        " opBeforeTime: " +
                        formatTime(opBeforeTime) +
                        " opAfterTime: " +
                        formatTime(opAfterTime) +
                        " opBeforeExpTime: " +
                        formatTime(opBeforeExpTime) +
                        " opAfterExpTime: " +
                        formatTime(opAfterExpTime) +
                        " resultExpTime: " +
                        formatTime(resultExpTime));
                }

                expirationTime.set(resultExpTime);

                if (DEBUG_WRITE) {
                    log(
                        (doInsert ? "Inserted " : "Updated ") +
                        "key: " + key +
                        " opBeforeTime: " +
                        formatTime(opBeforeTime) +
                        " opAfterTime: " +
                        formatTime(opAfterTime) +
                        " resultExpTime: " +
                        formatTime(resultExpTime));

                    result = db.get(
                        txn, keyEntry, dataEntry, Get.SEARCH, null);

                    assertNotNull("Could not read: " + key, result);

                    assertEquals(
                        resultExpTime, result.getExpirationTime());
                }

                txn.commit();

                if (doInsert) {
                    nInserts.incrementAndGet();
                } else {
                    nUpdates.incrementAndGet();
                }

            } catch (Throwable e) {
                e.printStackTrace(System.out);
                System.exit(1);
            }
        }
    }

    private class Delete implements Runnable {

        private final long key;
        private final AtomicBoolean abandonOp;
        private final AtomicLong expirationTime;

        Delete(final long key,
               final AtomicBoolean abandonOp,
               final AtomicLong expirationTime) {
            this.key = key;
            this.abandonOp = abandonOp;
            this.expirationTime = expirationTime;
        }

        @Override
        public void run() {

            Thread.currentThread().setName("Delete");

            final long startTime = System.currentTimeMillis();
            final DatabaseEntry keyEntry = new DatabaseEntry();
            LongBinding.longToEntry(key, keyEntry);

            try {
                while (true) {

                    if (abandonOp.get()) {
                        return;
                    }

                    final long sysTime = System.currentTimeMillis();
                    final long opTime = speedyTime.realTimeToFakeTime(sysTime);

                    if (sysTime - startTime > EXPIRATION_MAX_MILLIS) {

                        if (TTL.isExpired(expirationTime.get())) {
                            nDeleteExpired.incrementAndGet();

                        } else {
                            fail("Did not expire: " + key +
                                " opTime: " + formatTime(opTime) +
                                " expirationTime: " +
                                formatTime(expirationTime.get()));
                        }
                        break;
                    }

                    final OperationResult result = db.delete(
                        null, keyEntry, null);

                    if (result == null) {

                        if (TTL.isExpired(expirationTime.get())) {
                            nDeleteExpired.incrementAndGet();
                            return;
                        }

                        continue;
                    }

                    assertEquals(
                        expirationTime.get(), result.getExpirationTime());

                    nDeletions.incrementAndGet();

                    return;
                }
            } catch (Throwable e) {
                e.printStackTrace(System.out);
                System.exit(1);
            }
        }
    }

    private class PrimaryRead implements Runnable {

        private final long key;
        private final AtomicBoolean abandonOp;
        private final AtomicLong expirationTime;
        private final boolean doDelete;

        PrimaryRead(final long key,
                    final AtomicBoolean abandonOp,
                    final AtomicLong expirationTime,
                    final boolean doDelete) {
            this.key = key;
            this.abandonOp = abandonOp;
            this.expirationTime = expirationTime;
            this.doDelete = doDelete;
        }

        @Override
        public void run() {

            Thread.currentThread().setName("PrimaryRead");

            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();
            final long startTime = System.currentTimeMillis();
            OperationResult result;

            while (true) {

                if (abandonOp.get()) {
                    return;
                }

                try (final Cursor cursor = db.openCursor(null, null)) {

                    final long sysTime = System.currentTimeMillis();
                    final long opTime = speedyTime.realTimeToFakeTime(sysTime);

                    if (sysTime - startTime > EXPIRATION_MAX_MILLIS) {

                        if (TTL.isExpired(expirationTime.get())) {
                            nPriExpired.incrementAndGet();

                        } else if (doDelete) {
                            nPriDeleted.incrementAndGet();

                        } else {
                            fail("Did not expire: " + key +
                                " opTime: " + formatTime(opTime) +
                                " expirationTime: " +
                                formatTime(expirationTime.get()));
                        }
                        break;
                    }

                    nPriReads.incrementAndGet();

                    LongBinding.longToEntry(key, keyEntry);
                    final boolean readDataSeparately = (key % 3) == 0;

                    result = cursor.get(
                        keyEntry,
                        readDataSeparately ? null : dataEntry,
                        Get.SEARCH, null);

                    if (result == null) {
                        if (TTL.isExpired(expirationTime.get())) {
                            nPriExpired.incrementAndGet();
                            break;
                        }
                        continue;
                    }

                    assertEquals(key, LongBinding.entryToLong(keyEntry));
                    assertEquals(
                        expirationTime.get(), result.getExpirationTime());

                    if (!readDataSeparately) {
                        final TupleInput input =
                            TupleBase.entryToInput(dataEntry);
                        assertEquals(key, input.readLong());
                    }

                    result = cursor.get(
                        keyEntry, dataEntry, Get.CURRENT, null);

                    if (result == null) {
                        if (readDataSeparately &&
                            TTL.isExpired(expirationTime.get())) {
                            nPriExpiredData.incrementAndGet();
                            break;
                        }
                        fail("Could not read locked record: " + key);
                    }

                    assertEquals(key, LongBinding.entryToLong(keyEntry));
                    final TupleInput input = TupleBase.entryToInput(dataEntry);
                    assertEquals(key, input.readLong());
                    assertEquals(
                        expirationTime.get(), result.getExpirationTime());

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    System.exit(1);
                }
            }
        }
    }

    private class SecondaryRead implements Runnable {

        private final long key;
        private final AtomicBoolean abandonOp;
        private final AtomicLong expirationTime;
        private final boolean doDelete;
        private final boolean doUpdate;

        SecondaryRead(final long key,
                      final AtomicBoolean abandonOp,
                      final AtomicLong expirationTime,
                      final boolean doDelete,
                      final boolean doUpdate) {
            this.key = key;
            this.abandonOp = abandonOp;
            this.expirationTime = expirationTime;
            this.doDelete = doDelete;
            this.doUpdate = doUpdate;
        }

        @Override
        public void run() {

            Thread.currentThread().setName("SecondaryRead");

            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry pKeyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();
            final long startTime = System.currentTimeMillis();
            OperationResult result;

            while (true) {

                if (abandonOp.get()) {
                    return;
                }

                try (final SecondaryCursor cursor =
                         secDb.openCursor(null, null)) {

                    final long sysTime = System.currentTimeMillis();
                    final long opTime = speedyTime.realTimeToFakeTime(sysTime);

                    if (sysTime - startTime > EXPIRATION_MAX_MILLIS) {

                        if (TTL.isExpired(expirationTime.get())) {
                            nSecExpired.incrementAndGet();

                        } else if (doDelete) {
                            nSecDeleted.incrementAndGet();

                        } else {
                            fail("Did not expire: " + key +
                                " currentTime: " + formatTime(opTime) +
                                " expirationTime: " +
                                formatTime(expirationTime.get()));
                        }
                        break;
                    }

                    nSecReads.incrementAndGet();

                    LongBinding.longToEntry(key, keyEntry);

                    /*
                     * If we read data separately when an update or deletion is
                     * being done, this would cause deadlocks.
                     */
                    final boolean readDataSeparately =
                        !doUpdate && !doDelete && (key % 3) == 0;

                    result = cursor.get(
                        keyEntry, pKeyEntry,
                        readDataSeparately ? null : dataEntry,
                        Get.SEARCH,  null);

                    if (result == null) {
                        if (TTL.isExpired(expirationTime.get())) {
                            nSecExpired.incrementAndGet();
                            break;
                        }
                        continue;
                    }

                    assertEquals(key, LongBinding.entryToLong(keyEntry));
                    assertEquals(
                        expirationTime.get(), result.getExpirationTime());

                    assertTrue(Arrays.equals(
                        keyEntry.getData(), pKeyEntry.getData()));

                    if (!readDataSeparately) {
                        final TupleInput input =
                            TupleBase.entryToInput(dataEntry);
                        assertEquals(key, input.readLong());
                    }

                    result = cursor.get(
                        keyEntry, pKeyEntry, dataEntry, Get.CURRENT, null);

                    if (result == null) {
                        if (readDataSeparately &&
                            TTL.isExpired(expirationTime.get())) {
                            nSecExpiredData.incrementAndGet();
                            break;
                        }
                        fail("Could not read locked record: " + key +
                            " readDataSeparately: " + readDataSeparately);
                    }

                    assertEquals(key, LongBinding.entryToLong(keyEntry));

                    assertTrue(Arrays.equals(
                        keyEntry.getData(), pKeyEntry.getData()));

                    final TupleInput input = TupleBase.entryToInput(dataEntry);
                    assertEquals(key, input.readLong());
                    assertEquals(
                        expirationTime.get(), result.getExpirationTime());

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    System.exit(1);
                }
            }
        }
    }

    private long writeTimeToExpirationTime(final long writeTime) {

        final int expiration = TTL.systemTimeToExpiration(
            writeTime + TTL.MILLIS_PER_HOUR,
            true /*hours*/);

        return TTL.expirationToSystemTime(expiration, true /*hours*/);
    }

    private String formatTime(final long time) {
        synchronized (DATE_FORMAT) {
            return DATE_FORMAT.format(time);
        }
    }

}
