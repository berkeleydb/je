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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Properties;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Typical usage:
 * # Initialize the DBs
 * java EnvSharedCache -h HOME -initonly
 *
 * # Run updates with two classes of worker threads (different cache size)
 * java EnvSharedCache -h HOME -shared -cachetest -txns 1000000
 */
public class EnvSharedCache implements Runnable {

    private static final int INSERT = 1;
    private static final int UPDATE = 2;
    private static final int SELECT = 3;
    private static boolean verbose = false;
    private static boolean debug = false;
    private static boolean openTest = false;
    private static boolean cacheTest = false;
    private static boolean sharedTest = false;
    private static boolean evenTest = false;
    private static boolean initOnly = false;
    private static String delimiter = System.getProperty("file.separator");
    private static String homeDirPrefix = "db";
    private static StringBuilder inputArgs = new StringBuilder();
    private static int nEnvs = 4;
    private static int nThreadsPerEnv = 4;
    private static int nMaxKeys = 1000000;
    private static int subDir = 0;
    private static int nMaxTransactions = 100000;
    private static float nCacheMissThreshold = 0.5f;
    private static float nCacheSizeThreshold = 0.40f;
    private static float nThruputThreshold = 0.5f;
    private Environment[] envs;
    private Database[] dbs;
    private EnvironmentStats[] envStats;
    private SecureRandom random = new SecureRandom();
    private boolean isSharedCacheRun = false;
    private int keySize = 10;
    private int dataSize = 100;
    private int nRecordsPerThread = 0;
    private int nDeletesPerThread = 0;
    private int nInitEnvs = 0;
    private int nInitThreadsPerEnv = 0;
    private int nTransactions[][];
    private int nInserts[][];
    private int nUpdates[][];
    private int nDeletes[][];
    private int nSelects[][];
    private int nReadsPerWrite = 10;
    private float nThroughput = 0.0f;
    private long nElapsedTime[][];

    public static void main(String args[]) {
        try {
            /* Parse command-line input arguments. */
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                boolean moreArgs = i < args.length - 1;
                if (arg.equals("-v")) {
                    verbose = true;
                } else if (arg.equals("-d")) {
                    debug = true;
                } else if (arg.equals("-initonly")) {
                    initOnly = true;
                } else if (arg.equals("-opentest")) {
                    openTest = true;
                } else if (arg.equals("-cachetest")) {
                    cacheTest = true;
                } else if (arg.equals("-eventest")) {
                    evenTest = true;
                } else if (arg.equals("-h") && moreArgs) {
                    homeDirPrefix = args[++i] + delimiter + homeDirPrefix;
                } else if (arg.equals("-shared")) {
                    sharedTest = true;
                } else if (arg.equals("-envs") && moreArgs) {
                    nEnvs = Integer.parseInt(args[++i]);
                } else if (arg.equals("-keys") && moreArgs) {
                    nMaxKeys = Integer.parseInt(args[++i]);
                } else if (arg.equals("-txns") && moreArgs) {
                    nMaxTransactions = Integer.parseInt(args[++i]);
                } else if (arg.equals("-threads") && moreArgs) {
                    nThreadsPerEnv = Integer.parseInt(args[++i]);
                } else if (arg.equals("-subDir") && moreArgs) {
                    subDir = Integer.parseInt(args[++i]);
                } else if (arg.equals("-help")) {
                    usage(null);
                    System.exit(0);
                } else {
                    usage("Unknown arg: " + arg);
                    System.exit(1);
                }
            }
            /* Save command-line input arguments. */
            for (String s : args) {
                inputArgs.append(" " + s);
            }
            System.out.println("\nCommand-line input arguments:\n  "
                    + inputArgs);
            /*
             * If -shared flag is specified, compare EnvironmentStats
             * between shareCache and nonSharedCache runs to judge
             * whether environment shared cache test passes/fails.
             */
            if (sharedTest) {
                EnvSharedCache nonSharedCacheRun = new EnvSharedCache();
                nonSharedCacheRun.setSharedCacheRun(false);

                EnvSharedCache sharedCacheRun = new EnvSharedCache();
                sharedCacheRun.setSharedCacheRun(true);

                System.out.println("Starting non-sharedCache test...");
                nonSharedCacheRun.startTest();
                System.out.println("\nStarting sharedCache test...");
                sharedCacheRun.startTest();
                /* Compare stats to judge test passes/fails. */
                if (!verifyResults(nonSharedCacheRun, sharedCacheRun)) {
                    /* Failed to meet test criteria, exit with error. */
                    System.exit(1);
                }
            } else {
                new EnvSharedCache().startTest();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Print the usage.
     */
    private static void usage(String msg) {
        String usageStr;
        if (msg != null) {
            System.err.println(msg);
        }
        usageStr = "Usage: java EnvSharedCache\n"
            + "            [-v] [-d] [-h <homeDirPrefix>]\n"
            + "            [-envs <numOfEnvs>]\n"
            + "            [-threads <numOfThreadsPerEnv>]\n"
            + "            [-keys <numOfKeysPerThread>] [-initonly]\n\n"
            + "Usage: java EnvSharedCache\n"
            + "            [-v] [-d] [-h <homeDirPrefix>]\n"
            + "            [-envs <numOfEnvs>]\n"
            + "            [-threads <numOfThreadsPerEnv>]\n"
            + "            [-txns <numOfTransactionsPerThread>]\n"
            + "            [-cachetest [-shared] [-opentest] [-eventest]]";
        System.err.println(usageStr);
    }

    /**
     * Compare results between non-shared and shared cache run.
     */
    public static boolean verifyResults(EnvSharedCache nonSharedCache,
                                        EnvSharedCache sharedCache) {
        EnvironmentStats nonSharedStatsArray[] = nonSharedCache.getEnvStats();
        EnvironmentStats sharedStatsArray[] = sharedCache.getEnvStats();
        boolean thruputCheck = false;
        boolean cacheMissCheck = false;
        boolean cacheSizeCheck = false;
        boolean overallCheck = true;
        System.out.println
            ("\n\n          "
             + "Multi-Env SharedCache Test Summary Report At: "
             + new java.util.Date());
        System.out.println
            ("                         Non-Shared      Shared       Pass/Fail");
        System.out.println
            ("                         ----------    ----------    ----------");
        /* Check to see if throughput meet the given threshold. */
        if (evenTest) {
            thruputCheck =
                (Math.abs(sharedCache.nThroughput - nonSharedCache.nThroughput)
                     / nonSharedCache.nThroughput)
                <= nThruputThreshold;
            overallCheck &= thruputCheck;
        }
        System.out.printf
            ("Throughput(%.2f):        %10.2f    %10.2f    %10S%n",
             nThruputThreshold,
             nonSharedCache.nThroughput,
             sharedCache.nThroughput,
             (evenTest ? (thruputCheck ? "PASS" : "FAIL") : "N/A"));
        for (int i = 0; i < nEnvs; i++) {
            EnvironmentStats nonSharedStats = nonSharedStatsArray[i];
            EnvironmentStats sharedStats = sharedStatsArray[i];
            System.out.printf("Env(%d)\n", i);
            /*
             * Check if the regular worker's NCacheMiss variation meet
             * the given threshold. This check doesn't make sense
             * to smallCache workers.
             */
            if ((!openTest) && (!evenTest) && ((i % 2) != 1)) {
                cacheMissCheck = sharedStats.getNCacheMiss()
                    <= (nonSharedStats.getNCacheMiss() * nCacheMissThreshold);
            } else {
                cacheMissCheck = true;
            }
            overallCheck &= cacheMissCheck;
            System.out.printf
                ("        NCacheMiss(%.2f):%10d    %10d    %10S\n",
                 nCacheMissThreshold,
                 nonSharedStats.getNCacheMiss(),
                 sharedStats.getNCacheMiss(),
                 (!openTest) && (!evenTest)
                 ? (cacheMissCheck ? "PASS" : "FAIL")
                 : "N/A");
            /* For eventest, check CacheDataBytes to see if within 25%. */
            if (evenTest) {
                cacheSizeCheck =
                    ((float) Math.abs(sharedStats.getDataBytes()
                                - nonSharedStats.getDataBytes())
                         / nonSharedStats.getDataBytes())
                    <= nCacheSizeThreshold;
                overallCheck &= cacheSizeCheck;
            }
            System.out.printf
                ("         DataBytes(%.2f):%10d    %10d    %10S\n",
                 nCacheSizeThreshold,
                 nonSharedStats.getDataBytes(),
                 sharedStats.getDataBytes(),
                 (evenTest ? (cacheSizeCheck ? "PASS" : "FAIL") : "N/A"));
            System.out.printf
                ("             NLogBuffers:%10d    %10d\n",
                 nonSharedStats.getNLogBuffers(),
                 sharedStats.getNLogBuffers());
            System.out.printf
                ("         LogBuffersBytes:%10d    %10d\n",
                 nonSharedStats.getBufferBytes(),
                 sharedStats.getBufferBytes());
            System.out.printf
                ("         CacheTotalBytes:%10d    %10d\n",
                 nonSharedStats.getCacheTotalBytes(),
                 sharedStats.getCacheTotalBytes());
            System.out.printf
                ("            NNotResident:%10d    %10d\n",
                 nonSharedStats.getNNotResident(),
                 sharedStats.getNNotResident());
            System.out.printf
                ("         NSharedCacheEnv:%10d    %10d\n",
                 nonSharedStats.getNSharedCacheEnvironments(),
                 sharedStats.getNSharedCacheEnvironments());
            System.out.printf
                ("        SCacheTotalBytes:%10d    %10d\n",
                 nonSharedStats.getSharedCacheTotalBytes(),
                 sharedStats.getSharedCacheTotalBytes());
        }
        System.out.print("\nThe run is: " + (sharedTest ? "-shared " : "")
                + (openTest ? "-opentest " : "")
                + (evenTest ? "-eventest " : "")
                + "\nThe run is considered as: "
                + (overallCheck ? "PASS" : "FAIL") + "\n");
        return overallCheck;
    }

    /**
     * Set the isSharedCacheRun flag.
     */
    private void setSharedCacheRun(boolean flag) {
        isSharedCacheRun = flag;
    }

    /**
     * Get the envStats.
     */
    private EnvironmentStats[] getEnvStats() {
        return envStats;
    }

    /**
     * Precheck if database files exist before starting the run.
     */
    private boolean validateHomeDir() {
        for (int i = 0; i < nEnvs; i++) {
            File f = new File(homeDirPrefix + i);
            if (f.isDirectory()) {
                continue;
            } else if (initOnly) {
                f.mkdirs();
            } else {
                return false;
            }
        }
        return true;
    }

    private void startTest() throws Exception {

        if (!validateHomeDir()) {
            System.err.println("ERROR: Invalid HomeDirPrefix!"
                    + " Please specify a valid HomeDirPrefix parameter"
                    + " that points to your *.jdb files.");
            System.exit(1);
        }
        /* Read properties from ${DB0}/run.properties file. */
        File file = new File(homeDirPrefix + "0"
                + System.getProperty("file.separator") + "run.properties");
        Properties prop = new Properties();
        if (file.exists()) {
            FileInputStream in = new FileInputStream(file);
            prop.load(in);
            nRecordsPerThread =
                Integer.parseInt(prop.getProperty("RecordsPerThread"));
            nDeletesPerThread =
                Integer.parseInt(prop.getProperty("DeletesPerThread"));
            nInitEnvs =
                Integer.parseInt(prop.getProperty("InitEnvs"));
            nInitThreadsPerEnv =
                Integer.parseInt(prop.getProperty("InitThreadsPerEnv"));
            in.close();
        }
        if (initOnly) {
            nInitEnvs = nEnvs;
            nInitThreadsPerEnv = nThreadsPerEnv;
        } else if (nInitEnvs > 0 && nEnvs > nInitEnvs) {
            System.out.println("Warning: The number of environments"
                    + " specified here is beyond the value of environments"
                    + " when last initiating databases.\nAuto adjust to"
                    + " last initiating value: " + nInitEnvs);
        } else if (nInitThreadsPerEnv > 0
                && nThreadsPerEnv > nInitThreadsPerEnv) {
            System.out.println("Warning: The number of threads specified"
                    + " here is beyond the value of threads when last"
                    + " initiating databases.\nAuto adjust to last"
                    + " initiating value: " + nInitThreadsPerEnv);
            nThreadsPerEnv = nInitThreadsPerEnv;
        }

        envs = new Environment[nEnvs];
        dbs = new Database[nEnvs];
        envStats = new EnvironmentStats[nEnvs];
        nInserts = new int[nEnvs][nThreadsPerEnv];
        nUpdates = new int[nEnvs][nThreadsPerEnv];
        nDeletes = new int[nEnvs][nThreadsPerEnv];
        nSelects = new int[nEnvs][nThreadsPerEnv];
        nTransactions = new int[nEnvs][nThreadsPerEnv];
        nElapsedTime = new long[nEnvs][nThreadsPerEnv];

        /*
         * Initialize the Environments and open the Databases. For
         * open/close test, we initialize with each transaction in the
         * thread main loop.
         */
        if (!openTest) {
            for (int i = 0; i < nEnvs; i++) {
                envs[i] = openEnv(i);
                dbs[i] = openDB(envs[i], i);
            }
        }

        /* Create the workers and initialize operation counters. */
        Thread[][] threads = new Thread[nEnvs][nThreadsPerEnv];
        for (int i = 0; i < nEnvs; i++) {
            for (int j = 0; j < nThreadsPerEnv; j++) {
                nInserts[i][j] = 0;
                nUpdates[i][j] = 0;
                nDeletes[i][j] = 0;
                nSelects[i][j] = 0;
                nTransactions[i][j] = 0;
                threads[i][j] =
                    new Thread(this, Integer.toString(i * nThreadsPerEnv + j));
                threads[i][j].start();
                Thread.sleep(100);
            }
        }

        /* Wait until threads finished. */
        for (int i = 0; i < nEnvs; i++) {
            for (int j = 0; j < nThreadsPerEnv; j++) {
                if (threads[i][j] != null) {
                    threads[i][j].join();
                }
            }
        }

        if (!openTest) {
            for (int i = 0; i < nEnvs; i++) {
                /* Put EnvironmentStats objects into arrays before closing. */
                envStats[i] = getStats(envs[i], i);
            }

            for (int i = 0; i < nEnvs; i++) {
                closeEnv(envs[i], dbs[i]);
            }
        }

        /* Calculate elapsed time, transactions and throughput. */
        int transactions = 0;
        long timeMillis = 0;
        float elapsedSecs = 0.0f;
        float throughput = 0.0f;
        for (int i = 0; i < nEnvs; i++) {
            int inserts = 0, updates = 0, deletes = 0, selects = 0;
            for (int j = 0; j < nThreadsPerEnv; j++) {
                inserts += nInserts[i][j];
                updates += nUpdates[i][j];
                deletes += nDeletes[i][j];
                selects += nSelects[i][j];
                transactions += nTransactions[i][j];
                timeMillis += nElapsedTime[i][j];
                elapsedSecs = (float) nElapsedTime[i][j] / 1000;
                throughput = (float) nTransactions[i][j] / elapsedSecs;
                if (verbose) {
                    System.out.printf("%nENV(%d) Thread %d "
                            + " Running time: %.2f secs Transactions: %d"
                            + " Throughput: %.2f txns/sec", i, j, elapsedSecs,
                            nTransactions[i][j], throughput);
                }
            }
            if (verbose) {
                System.out.println("\nENV(" + i + "): " + inserts + " inserts "
                        + updates + " updates " + deletes + " deletes "
                        + selects + " selects ");
            }
        }
        elapsedSecs = (float) timeMillis / (nEnvs * nThreadsPerEnv * 1000);
        throughput = (float) transactions / elapsedSecs;
        nThroughput = throughput;
        System.out.printf("%nAverage elapsed time: %.2f secs"
                + " Transactions: %d Throughput: %.2f txns/sec%n",
                elapsedSecs, transactions, throughput);

        /* Create/Update ${DB0}/run.properties file. */
        FileOutputStream out = new FileOutputStream(file);
        prop.setProperty("RecordsPerThread", Integer.toString(nRecordsPerThread
                + nInserts[0][0] - nDeletes[0][0]));
        prop.setProperty("DeletesPerThread", Integer.toString(nDeletesPerThread
                + nDeletes[0][0]));
        prop.setProperty("InitEnvs", Integer.toString(nInitEnvs));
        prop.setProperty("InitThreadsPerEnv",
                         Integer.toString(nInitThreadsPerEnv));
        prop.store(out, "EnvSharedCache test runtime properties."
                + " Please don't update/remove this file.");
        out.close();
    }

    /**
     * Print and return the cache related stats for the env.
     */
    private EnvironmentStats getStats(Environment env, int envId)
            throws Exception {

        assert (env != null) : "getStats: Null env pointer";

        StatsConfig statsConfig = new StatsConfig();
        statsConfig.setFast(true);
        statsConfig.setClear(true);
        EnvironmentStats stats = env.getStats(statsConfig);
        return stats;
    }

    /**
     * Open an Environment.
     */
    private Environment openEnv(int i) throws Exception {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        /*
         * When using Zing JDK, the object may occupy more 40% percent
         * memory than Oracle JDk. When the cache size is 10M, it is
         * enough for caching most of the records, but it is not enough
         * for Zing JDK. So for Zing JDK, we increase the cache size. 
         */
        int factor = 1;                
        if (JVMSystemUtils.ZING_JVM) {
            factor = 2;
        }
        if (isSharedCacheRun) {
            envConfig.setCacheSize(10000000 * nEnvs * factor);
            envConfig.setSharedCache(true);
        } else {
            envConfig.setCacheSize(10000000 * factor);
            envConfig.setSharedCache(false);
        }

        /*
         * Because the evictor has multiple LRU lists per LRUSet, the accuracy
         * of the LRU varies too much to be predictable in this test,
         * especially due to outliers on some machines. Use a single LRU list
         * per LRUSet.
         */
        envConfig.setConfigParam(
            EnvironmentConfig.EVICTOR_N_LRU_LISTS, "1");

        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(homeDirPrefix + i), subDir, true);
        }

        Environment env = new Environment(new File(homeDirPrefix + i),
                envConfig);
        return env;
    }

    /**
     * Open a Database.
     */
    private Database openDB(Environment env, int i) throws Exception {

        assert (env != null) : "openDB: Null env pointer";

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        return env.openDatabase(null, "db" + i, dbConfig);
    }

    /**
     * Close the Database and Environment.
     */
    private void closeEnv(Environment env, Database db)
            throws DatabaseException {

        assert (db != null) : "closeEnv: Null db pointer";
        assert (env != null) : "closeEnv: Null env pointer";

        db.close();
        env.close();
    }

    /**
     * Generate the data.
     */
    private void makeData(DatabaseEntry data) {

        assert (data != null) : "makeData: Null data pointer";

        byte[] bytes = new byte[dataSize];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        data.setData(bytes);
    }

    /**
     * Generate the random data.
     */
    private void makeRandomData(DatabaseEntry data) {

        assert (data != null) : "makeRandomData: Null data pointer";

        byte[] bytes = new byte[dataSize];
        random.nextBytes(bytes);
        data.setData(bytes);
    }

    /**
     * Return a copy of the byte array in data.
     */
    private byte[] copyData(DatabaseEntry data) {

        assert (data != null) : "copyData: Null data pointer";

        byte[] buf = new byte[data.getSize()];
        System.arraycopy(data.getData(), data.getOffset(), buf, 0, buf.length);
        return buf;
    }

    /**
     * Return a copy of the byte array in data starting at the offset.
     */
    private byte[] copyData(DatabaseEntry data, int offset) {

        assert (data != null) : "copyData: Null data pointer";

        byte[] buf = new byte[data.getSize() - offset];
        System.arraycopy(data.getData(), data.getOffset() + offset,
                         buf, 0, buf.length);
        return buf;
    }

    /**
     * Generate the insert key with a prefix string.
     */
    private void makeInsertKey(Cursor c,
                               DatabaseEntry key,
                               String keyPrefix,
                               boolean smallCache) {

        assert (c != null) : "makeInsertKey: Null cursor pointer";
        assert (key != null) : "makeInsertKey: Null key pointer";
        assert (keyPrefix != null) : "makeInsertKey: Null keyPrefix pointer";

        String buf = keyPrefix;
        int num;
        if (key.getData() != null) {
            num = Integer.parseInt
                (new String(copyData(key, keyPrefix.length())));
            num++;
        } else {
            /*
             * For regular working set, we define:
             * deletion always occurs at the first database record,
             * and insertion always appends to the last record,
             * search randomly between the first and last.
             */
            if (smallCache) {
                num = nRecordsPerThread;
            } else {
                num = nRecordsPerThread + nDeletesPerThread;
            }
        }
        buf += Integer.toString(num);
        key.setData(buf.getBytes());
    }

    /**
     * Insert a record.
     */
    private void insert(Cursor c,
                        DatabaseEntry key,
                        DatabaseEntry data,
                        String keyPrefix,
                        boolean smallCache) throws DatabaseException {

        assert (c != null) : "insert: Null cursor pointer";
        assert (key != null) : "insert: Null key pointer";
        assert (data != null) : "insert: Null data pointer";

        makeData(data);
        boolean done = false;
        while (!done) {
            /*
             * Generate a key that is prefixed with the thread name so each
             * thread is working on its own data set to reduce deadlocks.
             */
            makeInsertKey(c, key, keyPrefix, smallCache);
            OperationStatus status = c.putNoOverwrite(key, data);
            if (status == OperationStatus.KEYEXIST) {
                System.out.println("Duplicate key.");
            } else {
                if (status != OperationStatus.SUCCESS) {
                    System.out.println("Unexpected insert error: " + status);
                }
                done = true;
            }
        }
    }

    /**
     * Generate the search key with a prefix string.
     */
    private void makeSearchKey(Cursor c,
                               DatabaseEntry key,
                               String keyPrefix,
                               boolean smallCache,
                               int offset) {

        assert (c != null) : "makeSearchKey: Null cursor pointer";
        assert (key != null) : "makeSearchKey: Null key pointer";
        assert (keyPrefix != null) : "makeSearchKey: Null keyPrefix pointer";

        String buf = keyPrefix;
        int num;
        if (smallCache) {
            num = offset;
        } else {
            /*
             * For regular working set, we create the random search key
             * between the current "beginning" and "end" of database records.
             */
            num = random.nextInt(nRecordsPerThread) + nDeletesPerThread
                    + offset;
        }
        buf += Integer.toString(num);
        key.setData(buf.getBytes());
    }

    public void run() {
        Environment env = null;
        Database db = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry searchKey = new DatabaseEntry();
        DatabaseEntry searchData = new DatabaseEntry();
        boolean done = false;
        boolean smallCache = false;
        byte[] lastInsertKey = null;
        Transaction txn = null;
        Cursor c = null;
        int nKeys = 0;
        OperationStatus status;

        String threadName = Thread.currentThread().getName();
        int envId = Integer.parseInt(threadName) / nThreadsPerEnv;
        int threadId = Integer.parseInt(threadName) % nThreadsPerEnv;
        String keyPrefix = threadId + "-";

        if (verbose) {
            System.out.println("Thread " + threadId + " started on ENV("
                    + envId + ")");
        }

        /* Initialize with start time. */
        nElapsedTime[envId][threadId] = System.currentTimeMillis();

        /*
         * If it is not evenTest (even work load on each env), to test cache
         * utilization efficiency, we create two classes of users. One set
         * will simply insert, update, and delete the same record repeatedly
         * and the other set will have a larger working set.
         * The former will use very little cache and will result in waste
         * in non-shared cache case.
         */
        smallCache = (!evenTest) & ((envId % 2) == 1);

        if (!openTest) {
            env = envs[envId];
            db = dbs[envId];
        }

        while (!done) {
            try {
                /* Test the env open/close */
                if (openTest) {
                    env = openEnv(envId);
                    db = openDB(env, envId);
                }

                txn = env.beginTransaction(null, null);
                c = db.openCursor(txn, null);

                if (initOnly && nKeys < nMaxKeys) {
                    insert(c, key, data, keyPrefix, smallCache);
                    checkCorrectness(INSERT, key, data, keyPrefix, smallCache,
                            nKeys);
                    nKeys++;
                    nInserts[envId][threadId]++;
                }

                if (!initOnly) {
                    /* Insert */
                    if (smallCache) {
                        /*
                         * Set key to null, so every time
                         * it will insert the same key.
                         */
                        key.setData(null);
                    }
                    insert(c, key, data, keyPrefix, smallCache);
                    if (smallCache) {
                        checkCorrectness(INSERT, key, data, keyPrefix,
                                smallCache, nRecordsPerThread);
                    } else {
                        checkCorrectness(INSERT, key, data, keyPrefix,
                                smallCache,
                                (nRecordsPerThread + nDeletesPerThread
                                     + nInserts[envId][threadId]));
                    }
                    lastInsertKey = copyData(key);
                    nInserts[envId][threadId]++;
                    /* Update */
                    if (smallCache) {
                        searchKey.setData(lastInsertKey);
                    } else {
                        makeSearchKey(c, searchKey, keyPrefix, smallCache,
                                nDeletes[envId][threadId]);
                    }
                    status = c.getSearchKeyRange(searchKey, searchData,
                            LockMode.DEFAULT);
                    if (status == OperationStatus.SUCCESS) {
                        makeRandomData(data);
                        status = c.putCurrent(data);
                        if (status == OperationStatus.SUCCESS) {
                            c.getSearchKey(searchKey, searchData,
                                    LockMode.DEFAULT);
                            if (smallCache) {
                                checkCorrectness(UPDATE, searchKey, searchData,
                                        keyPrefix, smallCache,
                                        nRecordsPerThread);
                            } else {
                                checkCorrectness(UPDATE, searchKey, searchData,
                                        keyPrefix, smallCache,
                                        nDeletes[envId][threadId]);
                            }
                            nUpdates[envId][threadId]++;
                        }
                        /* Delete */
                        if (!smallCache) {
                            String buf = keyPrefix
                                    + Integer.toString(nDeletesPerThread
                                            + nDeletes[envId][threadId]);
                            searchKey.setData(buf.getBytes());
                            status = c.getSearchKey(searchKey, searchData,
                                    LockMode.DEFAULT);
                        }
                        if (status == OperationStatus.SUCCESS) {
                            status = c.delete();
                            if (status == OperationStatus.SUCCESS) {
                                status = c.getSearchKey(searchKey, searchData,
                                        LockMode.DEFAULT);
                                /*
                                 * Delete correctness check: only checks if
                                 * the record still exists.
                                 */
                                if (status != OperationStatus.NOTFOUND) {
                                    System.err.println
                                        ("DELETE Correctness Check Failed: "
                                         + "key/data pair still exists after "
                                         + "deletion.");
                                    System.exit(1);
                                }
                                nDeletes[envId][threadId]++;
                            }
                        }
                    }
                    /* Read */
                    if (nReadsPerWrite > 0) {
                        int i;
                        for (i = 0; i < nReadsPerWrite; i++) {
                            if (smallCache) {
                                makeSearchKey(c, searchKey, keyPrefix,
                                        smallCache, i);
                                c.getSearchKey(searchKey, searchData,
                                        LockMode.DEFAULT);
                                checkCorrectness(SELECT, searchKey, searchData,
                                        keyPrefix, smallCache, i);
                            } else {
                                makeSearchKey(c, searchKey, keyPrefix,
                                        smallCache, nDeletes[envId][threadId]);
                                c.getSearchKey(searchKey, searchData,
                                        LockMode.DEFAULT);
                                checkCorrectness(SELECT, searchKey, searchData,
                                        keyPrefix, smallCache,
                                        nDeletes[envId][threadId]);
                            }

                            /* 
                             * Call Thread.yield() to try to eliminate the
                             * possible unfair-thread-scheduling issue which
                             * may cause the throughput cache failure.
                             */
                            Thread.yield();
                        }
                        nSelects[envId][threadId] += i;
                    }
                }
                c.close();
                txn.commit();
                nTransactions[envId][threadId]++;
                if (initOnly) {
                    if (nKeys >= nMaxKeys) {
                        done = true;
                    }
                } else if (nMaxTransactions != 0
                        && nTransactions[envId][threadId] >= nMaxTransactions) {
                    done = true;
                }
                if (done && openTest && (threadId == (nThreadsPerEnv - 1))) {
                    envStats[envId] = getStats(env, envId);
                }
                if (openTest) {
                    closeEnv(env, db);
                }
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        } // End of while loop.

        /* Calculate elapsed time. */
        nElapsedTime[envId][threadId] = System.currentTimeMillis()
                - nElapsedTime[envId][threadId];
        if (verbose) {
            System.out.println("Thread " + threadId + " finished on ENV("
                    + envId + ")");
        }
    }

    /**
     * Operation correctness check.
     */
    private void checkCorrectness(int operationType,
                                  DatabaseEntry key,
                                  DatabaseEntry data,
                                  String keyPrefix,
                                  boolean smallCache,
                                  int checkNum) {

        assert (key != null) : "checkCorrectness: Null key pointer";
        assert (keyPrefix != null) : "checkCorrectness: Null keyPrefix pointer";

        String s = new String(key.getData());
        int num = Integer.parseInt(s.substring(s.indexOf("-") + 1));
        DatabaseEntry d = new DatabaseEntry();
        makeData(d);
        if (operationType == INSERT) {
            if (num != checkNum) {
                System.err.println("INSERT Correctness Check Failed: "
                        + "key value: " + s + " doesn't match checkNum: "
                        + checkNum + ".");
                System.exit(1);
            }
        } else if (operationType == UPDATE) {
            if (smallCache && (num != checkNum)) {
                System.err.println("UPDATE Correctness Check Failed: "
                        + "key value " + s + " doesn't match checkNum "
                        + checkNum + ".");
                System.exit(1);
            } else if (!smallCache) {
                if (num < checkNum) {
                    System.err.println("UPDATE Correctness Check Failed: "
                            + "key value should be larger than "
                            + checkNum + ".");
                    System.exit(1);
                } else if (num
                        > (nRecordsPerThread + nDeletesPerThread + checkNum)) {
                    System.err.println("UPDATE Correctness Check Failed: "
                            + "key value should be smaller than "
                            + (nRecordsPerThread + nDeletesPerThread + checkNum)
                            + ".");
                    System.exit(1);
                }
            } else if (Arrays.equals(data.getData(), d.getData())) {
                System.err.println("UPDATE Correctness Check Failed: "
                        + "data value doesn't change.");
                System.exit(1);
            }
        } else if (operationType == SELECT) {
            if (smallCache && num != checkNum) {
                System.err.println("SELECT Correctness Check Failed: "
                        + "key value: " + s + " doesn't match checkNum: "
                        + checkNum + ".");
                System.exit(1);
            } else if (!smallCache) {
                if (num < checkNum) {
                    System.err.println("SELECT Correctness Check Failed: "
                            + "key value should be larger than "
                            + checkNum + ".");
                    System.exit(1);
                } else if (num
                        > (nRecordsPerThread + nDeletesPerThread + checkNum)) {
                    System.err.println("SELECT Correctness Check Failed: "
                            + "key value should be smaller than "
                            + (nRecordsPerThread + nDeletesPerThread + checkNum)
                            + ".");
                    System.exit(1);
                }
            }
        }
    }
}
