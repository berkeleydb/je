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

import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.MemoryBudget;

/**
 * A large database with random key distribution has lots of IN waste,
 * especially if records are small; this creates a worst-case scenario for the
 * cleaner and also possibly for the evictor.  Simulate such an application and
 * measure how well the cleaner and evictor keep up.
 *
 * Some commonly used command lines for running this program are:
 *
 *   # Init new DB, then do updates forever.
 *   java BigRandom -h HOME -init
 *
 *   # Do updates on an existing DB forever.
 *   java BigRandom -h HOME
 *
 *   # Init new DB, then stop and print total rate (MOST COMMON OPTION)
 *   java BigRandom -h HOME -initonly
 *
 *   # -locality N adds locality of reference for N transactions.
 *   java BigRandom -h HOME -initonly -locality 5
 *
 *   # -nosync speeds things up quite a bit
 *   java BigRandom -h HOME -initonly -locality 5 -nosync
 *
 * Each transaction does the following in "grow" mode.  In "no grow" mode, it
 * does one less insert, keeping the total number of keys constant.
 *
 *   2 inserts, 1 delete, 1 update, 10 reads
 *
 * The delete and update operations include a read to find the record.
 *
 * Every operation uses a random key, unless the -locality option is used.  If
 * "-locality 100" is specified, each thread will perform 100 transactions by
 * incrementing the insertion key rather than generating a random number.  Then
 * a random number is generated as the next starting key.  This is done per
 * thread, so each thread will be working in a different key area.
 */
public class BigRandom implements Runnable {

    private String homeDir = "tmp";
    private Environment env;
    private Database db;
    private boolean done;
    private int nDeadlocks;
    private boolean init;
    private boolean initOnly;
    private boolean fastInit;
    private boolean verbose;
    private boolean sequentialKeys;
    private boolean noSync;
    private int nMaxKeys = 10000000;
    private long nKeys;
    private long sequence;
    private int nTransactions;
    private int nMaxTransactions;
    private int nThreads = 4;
    private int oneThreadKeys;
    private long traceInterval = 10000; // 10 seconds
    private boolean preload;
    private int maxLocalKeyTxns;
    private int keySize = 10;
    private int dataSize = 20;
    private int nReadsPerWrite = 10;
    private int maxRetries = 100;
    private SecureRandom random = new SecureRandom();
    private long startTime;
    private long priorTime = startTime;
    private int priorTxns;
    private int[] tpTxns = new int[120]; // 120 * 10 sec = ~20 minutes worth
    private long[] tpMillis = new long[tpTxns.length];
    private int tpIndex = tpTxns.length - 1;
    private int tpMaxIndex;
    private long tpTotalTxns;
    private long tpTotalMillis;
    private int thisTxns;
    private int thisSecs;
    private int thisTp;
    private int avgTp;
    private long time;
    private int totalSecs;
    private int subDir = 0;

    public static void main(String args[]) {
        try {
            new BigRandom().run(args);
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void run(String args[])
        throws Exception {

        for (int i = 0; i < args.length; i += 1) {
            String arg = args[i];
            boolean moreArgs = i < args.length - 1;
            if (arg.equals("-v")) {
                verbose = true;
            } else if (arg.equals("-seq")) {
                sequentialKeys = true;
            } else if (arg.equals("-nosync")) {
                noSync = true;
            } else if (arg.equals("-h") && moreArgs) {
                homeDir = args[++i];
            } else if (arg.equals("-preload")) {
                preload = true;
            } else if (arg.equals("-init")) {
                init = true;
            } else if (arg.equals("-initonly")) {
                init = true;
                initOnly = true;
            } else if (arg.equals("-fastinit")) {
                init = true;
                fastInit = true;
                initOnly = true;
            } else if (arg.equals("-keys") && moreArgs) {
                nMaxKeys = Integer.parseInt(args[++i]);
            } else if (arg.equals("-txns") && moreArgs) {
                nMaxTransactions = Integer.parseInt(args[++i]);
            } else if (arg.equals("-threads") && moreArgs) {
                nThreads = Integer.parseInt(args[++i]);
            } else if (arg.equals("-onethreadkeys") && moreArgs) {
                oneThreadKeys = Integer.parseInt(args[++i]);
            } else if (arg.equals("-locality") && moreArgs) {
                maxLocalKeyTxns = Integer.parseInt(args[++i]);
            } else if (arg.equals("-subDir") && moreArgs) {
                subDir = Integer.parseInt(args[++i]);
            } else {
                usage("Unknown arg: " + arg);
            }
        }
        openEnv();
        printArgs(args);
        printLegend();
        if (sequentialKeys) {
            sequence = getLastSequence();
        }
        if (preload) {
            doPreload();
        }
        StatsConfig statsConfig = new StatsConfig();
        statsConfig.setFast(true);
        statsConfig.setClear(true);
        startTime = System.currentTimeMillis();
        priorTime = startTime;

        Thread[] threads = new Thread[nThreads];
        if (oneThreadKeys > 0) {
            threads[0] = new Thread(this);
            threads[0].start();
        } else {
            for (int i = 0; i < nThreads; i += 1) {
                threads[i] = new Thread(this);
                threads[i].start();
                Thread.sleep(1000); /* Stagger threads. */
            }
        }

        while (!done) {
            Thread.sleep(traceInterval);
            calcThroughput();
            /* JE-only begin */
            EnvironmentStats stats = env.getStats(statsConfig);
            MemoryBudget mb =
                DbInternal.getNonNullEnvImpl(env).getMemoryBudget();
            int inListSize =
                DbInternal.getNonNullEnvImpl(env).getInMemoryINs().
                getSize();
            System.out.println("\nsec: " + totalSecs + ',' + thisSecs +
                               " txn: " + thisTxns + ',' +
                               thisTp + ',' + avgTp +
                               " keys: " + nKeys +
                               " dlck: " + nDeadlocks +
                               " buf: " +
                               stats.getNNotResident() + ',' +
                               stats.getNCacheMiss() +
                               "\ncleaner: " +
                               stats.getNCleanerEntriesRead() + ',' +
                               stats.getNCleanerRuns() + ',' +
                               stats.getNCleanerDeletions() + ',' +
                               stats.getCleanerBacklog() +
                               " evict: " +
                               stats.getNBINsStripped() + ',' +
                               stats.getNNodesExplicitlyEvicted() + ',' +
                               mb.getCacheMemoryUsage() + ',' +
                               inListSize +
                               " ckpt: " +
                               stats.getNCheckpoints() + ',' +
                               stats.getNFullINFlush() + ',' +
                               stats.getNFullBINFlush() + ',' +
                               stats.getNDeltaINFlush());
            /* JE-only end */
            nDeadlocks = 0;

            if (oneThreadKeys > 0 && oneThreadKeys >= nKeys) {
                for (int i = 1; i < nThreads; i += 1) {
                    threads[i] = new Thread(this);
                    threads[i].start();
                    Thread.sleep(1000); /* Stagger threads. */
                }
                oneThreadKeys = 0;
            }
        }

        for (int i = 0; i < nThreads; i += 1) {
            if (threads[i] != null) {
                threads[i].join();
            }
        }

        time = System.currentTimeMillis();
        totalSecs = (int) ((time - startTime) / 1000);
        System.out.println("\nTotal seconds: " + totalSecs +
                           " txn/sec: " + (nTransactions / totalSecs));
        closeEnv();
    }

    private void calcThroughput() {

        time = System.currentTimeMillis();
        totalSecs = (int) ((time - startTime) / 1000);
        int txns = nTransactions;
        thisTxns = txns - priorTxns;
        int thisMillis = (int) (time - priorTime);
        thisSecs = thisMillis / 1000;
        thisTp = thisTxns / thisSecs;

        tpIndex += 1;
        if (tpIndex == tpTxns.length) {
            tpIndex = 0;
        }
        tpTotalTxns += thisTxns;
        tpTotalTxns -= tpTxns[tpIndex];
        tpTotalMillis += thisMillis;
        tpTotalMillis -= tpMillis[tpIndex];
        tpTxns[tpIndex] = thisTxns;
        tpMillis[tpIndex] = thisMillis;
        if (tpMaxIndex < tpTxns.length) {
            tpMaxIndex = tpIndex + 1;
        }
        avgTp = (int) ((tpTotalTxns / (tpTotalMillis / 1000)));

        priorTxns = txns;
        priorTime = time;
    }

    private void printArgs(String[] args)
        throws DatabaseException {

        System.out.print("Command line arguments:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
        System.out.println();
        System.out.println("Environment configuration:");
        System.out.println(env.getConfig());
        System.out.println();
    }

    private void printLegend() {

        /* JE-only begin */
        System.out.println(
            "Legend:\n" +
            "sec:   <totalSeconds>,<runSeconds>\n" +
            "txn:   <txns>,<txnPerSec>,<runningAvgTxnPerSec>\n" +
            "keys:  <totalKeys>\n" +
            "dlck:  <deadlocks>\n" +
            "buf:   <notResident>,<cacheMisses>\n" +
            "clean: <entriesRead>,<filesCleaned>,<filesDeleted>,<backlog>\n" +
            "evict: <binsStripped>,<nodesEvicted>,<cacheSize>,<INListSize>\n" +
            "ckpt:  <checkpointsStarted>,<fullINs>,<fullBINs>,<deltaINs>");
        /* JE-only end */
    }

    private void usage(String error) {

        if (error != null) {
            System.err.println(error);
        }
        System.err.println
            ("java " + getClass().getName() + '\n' +
             "      [-h <homeDir>] [-v] [-init | -initonly | -fastinit]\n" +
             "      [-keys <maxKeys>] [-txns <maxTxns>] [-seq]\n" +
             "      [-threads <appThreads>] [-onethreadkeys <nKeys>]\n" +
             "      [-locality <nTxns>] [-nosync] [-preload]");
        System.exit(2);
    }

    private void openEnv() throws Exception {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(init);
        envConfig.setCacheMode(CacheMode.EVICT_LN);
        envConfig.setConfigParam(EnvironmentConfig.MAX_OFF_HEAP_MEMORY,
            "" + (50 * 1024 * 1024));

        if (noSync) {
            envConfig.setTxnNoSync(true);
        }

        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(homeDir), subDir);
        }

        long startTime = System.currentTimeMillis();
        env = new Environment(new File(homeDir), envConfig);
        long endTime = System.currentTimeMillis();
        System.out.println("Recovery time: " + ((endTime - startTime) / 1000));
        System.out.println();

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(init);
        dbConfig.setExclusiveCreate(init);
        dbConfig.setTransactional(true);
        /* JE-only begin */
        db = env.openDatabase(null, "BigRandom", dbConfig);
        /* JE-only end */
    }

    private void closeEnv()
        throws DatabaseException {

        db.close();
        env.close();
    }

    public void run() {

        /*
         * The key is reused over multiple loop iterations for computing a
         * local insertion key, so it must be instantiated at the top of the
         * loop.  In makeInsertKey a local insertion key is creating by adding
         * one to the last key accessed.
         */
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        int localKeyTxns = 0;
        byte[] lastInsertKey = null;
        OperationStatus status;

        while (!done) {

            try {
                /*
                 * When using local keys, only the first insert will be with a
                 * random key, and only if we've exceeded the maximum number of
                 * local key transactions.  When not using local keys, all keys
                 * are randomly generated.
                 */
                boolean useLocalKeys = maxLocalKeyTxns > 0;
                boolean insertRandomKey = true;
                if (useLocalKeys) {
                    if (localKeyTxns < maxLocalKeyTxns) {
                        insertRandomKey = false;
                        localKeyTxns += 1;
                    } else {
                        localKeyTxns = 0;
                    }
                }

                /* Perform the transaction. */
                for (int retry = 0;; retry += 1) {
                    Transaction txn = env.beginTransaction(null, null);
                    Cursor c = db.openCursor(txn, null);
                    try {
                        boolean addedKey = false;
                        if (init && nKeys < nMaxKeys) {
                            key.setData(lastInsertKey);
                            insert(c, key, data, insertRandomKey);
                            lastInsertKey = copyData(key);
                            insertRandomKey = !useLocalKeys;
                            addedKey = true;
                        }
                        if (!fastInit) {
                            /* Insert. */
                            key.setData(lastInsertKey);
                            insert(c, key, data, insertRandomKey);
                            lastInsertKey = copyData(key);
                            if (useLocalKeys) {
                                /* Update the following key. */
                                status = c.getNext(key, data, LockMode.RMW);
                                if (status == OperationStatus.SUCCESS) {
                                    c.putCurrent(data);
                                    /* Delete the following key. */
                                    status = c.getNext
                                        (key, data, LockMode.RMW);
                                    if (status == OperationStatus.SUCCESS) {
                                        c.delete();
                                    }
                                }
                                /* Read.  Use RMW to avoid deadlocks. */
                                for (int i = 0; i < nReadsPerWrite; i += 1) {
                                    c.getNext(key, data, LockMode.RMW);
                                }
                            } else {
                                /* Update */
                                getRandom(c, key, data, LockMode.RMW);
                                c.putCurrent(data);
                                /* Delete */
                                getRandom(c, key, data, LockMode.RMW);
                                c.delete();
                                /* Read */
                                for (int i = 0; i < nReadsPerWrite; i += 1) {
                                    getRandom(c, key, data, null);
                                }
                            }
                        }
                        c.close();
                        txn.commit();
                        nTransactions += 1;
                        if (addedKey) {
                            nKeys += 1;
                        }
                        if (initOnly && nKeys >= nMaxKeys) {
                            done = true;
                        }
                        if (nMaxTransactions != 0 &&
                            nTransactions >= nMaxTransactions) {
                            done = true;
                        }
                        break;
                    } catch (LockConflictException e) {
                        c.close();
                        txn.abort();
                        if (retry >= maxRetries) {
                            throw e;
                        }
                        /* Break deadlock cycle with a small sleep. */
                        Thread.sleep(5);
                        nDeadlocks += 1;
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private void insert(Cursor c, DatabaseEntry key, DatabaseEntry data,
                        boolean insertRandomKey)
        throws DatabaseException {

        makeData(data);
        while (true) {
            makeInsertKey(c, key, insertRandomKey);
            OperationStatus status = c.putNoOverwrite(key, data);
            if (status == OperationStatus.KEYEXIST) {
                if (sequentialKeys) {
                    System.out.println("****** Duplicate sequential key.");
                } else if (insertRandomKey) {
                    System.out.println("****** Duplicate random key.");
                } else {
                    System.out.println("****** Duplicate local key.");
                }
            } else {
                if (status != OperationStatus.SUCCESS) {
                    System.out.println
                        ("Unexpected return value from insert(): " + status);
                }
                break;
            }
        }
    }

    private void getRandom(Cursor c, DatabaseEntry key, DatabaseEntry data,
                           LockMode lockMode)
        throws DatabaseException {

        makeRandomKey(key);
        OperationStatus status = c.getSearchKeyRange(key, data, lockMode);
        if (status == OperationStatus.NOTFOUND) {
            status = c.getLast(key, data, lockMode);
            if (status != OperationStatus.SUCCESS) {
                System.out.println
                    ("Unexpected return value from getRandom(): " + status);
            }
        }
    }

    private long getLastSequence()
        throws DatabaseException {

        if (!sequentialKeys) throw new IllegalStateException();
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        Cursor c = db.openCursor(null, null);
        try {
            OperationStatus status = c.getLast(key, data, null);
            if (status == OperationStatus.SUCCESS) {
                TupleInput in = new TupleInput(key.getData(),
                                               key.getOffset(),
                                               key.getSize());
                return in.readLong();
            } else {
                return 0;
            }
        } finally {
            c.close();
        }
    }

    private void doPreload()
        throws DatabaseException {

        System.out.println("Preloading");
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry key = new DatabaseEntry();
        Cursor c = db.openCursor(null, null);
        try {
            long startTime = System.currentTimeMillis();
            int count = 0;
            while (c.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
                   OperationStatus.SUCCESS) {
                count += 1;
            }
            long endTime = System.currentTimeMillis();
            int seconds = (int) ((endTime - startTime) / 1000);
            System.out.println
                ("Preloaded records=" + count + " seconds=" + seconds);
        } finally {
            c.close();
        }
    }

    private void makeInsertKey(Cursor c, DatabaseEntry key,
                               boolean insertRandomKey) {
        if (sequentialKeys) {
            long val;
            synchronized (this) {
                val = ++sequence;
            }
            makeLongKey(key, val);
        } else if (!insertRandomKey && key.getData() != null) {
            BigInteger num = new BigInteger(copyData(key));
            num = num.add(BigInteger.ONE);
            key.setData(num.toByteArray());
        } else {
            makeRandomKey(key);
        }
    }

    private void makeRandomKey(DatabaseEntry key) {

        if (sequentialKeys) {
            makeLongKey(key, (long) (random.nextFloat() * sequence));
        } else {
            byte[] bytes = new byte[keySize];
            random.nextBytes(bytes);
            key.setData(bytes);
        }
    }

    private void makeLongKey(DatabaseEntry key, long val) {

        TupleOutput out = new TupleOutput();
        out.writeLong(val);
        byte[] pad = new byte[keySize - 8];
        out.writeFast(pad);
        if (out.getBufferOffset() != 0 || out.getBufferLength() != keySize) {
            throw new IllegalStateException();
        }
        key.setData(out.getBufferBytes(), 0, keySize);
    }

    private void makeData(DatabaseEntry data) {

        byte[] bytes = new byte[dataSize];
        for (int i = 0; i < bytes.length; i += 1) {
            bytes[i] = (byte) i;
        }
        data.setData(bytes);
    }

    private byte[] copyData(DatabaseEntry data) {

        byte[] buf = new byte[data.getSize()];
        System.arraycopy(data.getData(), data.getOffset(), buf, 0, buf.length);
        return buf;
    }
}
