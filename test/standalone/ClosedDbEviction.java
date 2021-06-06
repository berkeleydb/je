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
import java.util.Random;

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
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * Applications with a large number of databases, randomly open and close
 * databases at any time when needed. The mapping tree nodes (roots) in closed
 * databases won't be evicted from cache immediately. As the applications run
 * over time, this could cause a lot of waste in cache or even bad performance
 * and OutOfMemoryError if cache overflows.
 *
 * We want to simulate such a scenario to test the efficiency of eviction of
 * closed databases for SR 13415, to make sure that the eviction would not
 * cause corruption or concurrency bugs:
 * + Ensure that concurrency bugs don't occur when multiple threads are trying
 *   to close, evict and open a single database.
 * + Another potential problem is that the database doesn't open correctly
 *   after being closed and evicted;
 * + Cache budgeting is not done correctly during eviction or re-loading of
 *   the database after eviction.
 *
 */
public class ClosedDbEviction {
    private static int nDataAccessDbs = 1;
    private static int nRegularDbs = 100000;
    private static int nDbRecords = 100;
    private static int nInitThreads = 8;
    private static int nContentionThreads = 4;
    private static int nDbsPerSet = 5;
    private static int nKeepOpenedDbs = 100;
    private static int subDir = 3;
    private static boolean offHeap = false;
    private static int nOps[] = new int[nContentionThreads];
    private static long nTxnPerRecovery = 1000000l;
    private static long nTotalTxns = 100000000l;
    private static boolean verbose = false;
    private static boolean init = false;
    private static boolean contention = false;
    private static boolean evict = false;
    private static boolean recovery = false;
    private static boolean runDataAccessThread = true;
    private static String homeDir = "./tmp";
    private static Environment env = null;
    private static Database dataAccessDb = null;
    private static Database metadataDb = null;
    private static Database[] openDbList =  new Database[nKeepOpenedDbs];
    private static Random random = new Random();
    private static Runtime rt = Runtime.getRuntime();

    public static void main(String[] args) {
        try {
            ClosedDbEviction eviction = new ClosedDbEviction();
            eviction.start(args);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /* Output command-line input arguments to log. */
    private void printArgs(String[] args) {
        System.out.print("\nCommand line arguments:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
    }

    void start(String[] args) {
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException();
            }

            /* Parse command-line input arguments. */
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                String arg2 = (i < args.length - 1) ? args[i + 1] : null;
                if (arg.equals("-v")) {
                    verbose = true;
                } else if (arg.equals("-h")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    homeDir = args[++i];
                } else if (arg.equals("-init")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        nRegularDbs = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    init = true;
                } else if (arg.equals("-contention")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        nTotalTxns = Long.parseLong(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    contention = true;
                } else if (arg.equals("-evict")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        nTotalTxns = Long.parseLong(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    evict = true;
                } else if (arg.equals("-recovery")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        nTxnPerRecovery = Long.parseLong(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    recovery = true;
                } else if (arg.equals("-subDir")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        subDir = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                } else if (arg.equals("-offheap")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    offHeap = Boolean.parseBoolean(args[++i]);
                } else {
                    throw new IllegalArgumentException(arg);
                }
            }
            /* Correctness self-check: nTotalTxns >= nTxnPerRecovery. */
            if (nTotalTxns < nTxnPerRecovery) {
                System.err.println
                    ("ERROR: <nTotalTxns> argument should be larger than " +
                     nTxnPerRecovery + "!");
                System.exit(1);
            }
            printArgs(args);
        } catch (IllegalArgumentException e) {
            System.out.println
                ("Usage: ClosedDbEviction [-v] -h <envHome> -init <nDbs>\n" +
                 "Usage: ClosedDbEviction [-v] -h <envHome> " +
                 "[-contention <nTotalTxns> | -evict <nTotalTxns>] " +
                 "[-recovery <nTxnsPerRecovery>]");
            e.printStackTrace();
            System.exit(1);
        }

        try {
            if (init) {
                doInit();
            } else if (contention) {
                doContention();
            } else if (evict) {
                doEvict();
            } else {
                System.err.println("No such argument.");
                System.exit(1);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Initialize nRegularDBs, one dataAccessDb and one metadataDB.
     */
    private void doInit() {

        class InitThread extends Thread {
            public int id;
            private Environment env = null;
            private Database db = null;

            /**
             * Constructor used for initializing databases.
             */
            InitThread(int id, Environment env) {
                this.id = id;
                this.env = env;
            }

            public void run() {
                try {
                    DatabaseConfig dbConfig = new DatabaseConfig();
                    dbConfig.setAllowCreate(true);
                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry data = new DatabaseEntry();
                    for (int i = 0;
                         i <= ((nRegularDbs + nDataAccessDbs) / nInitThreads);
                         i++) {

                        int dbId = id + (i * nInitThreads);
                        int totalRecords = nDbRecords;
                        boolean isDataAccessDb = false;
                        String dbName = "db" + dbId;
                        dbConfig.setDeferredWrite(dbId <= (nRegularDbs / 10));
                        if (dbId >= nRegularDbs) {
                            if (dbId < (nRegularDbs + nDataAccessDbs)) {
                                isDataAccessDb = true;
                                dbName = "dataAccessDb";
                                totalRecords = 10 * nDbRecords;
                            } else {
                                break;
                            }
                        }
                        /* Open the database. */
                        db = env.openDatabase(null, dbName, dbConfig);
                        /* Insert totalRecords into database. */
                        for (int j = 0; j < totalRecords; j++) {
                            key.setData(Integer.toString(j).getBytes("UTF-8"));
                            makeData(data, j, isDataAccessDb);
                            OperationStatus status = db.put(null, key, data);
                            if (status != OperationStatus.SUCCESS) {
                                System.err.println
                                    ("ERROR: failed to insert the #" + j +
                                     " key/data pair into " +
                                     db.getDatabaseName());
                                System.exit(1);
                            }
                        }
                        db.close();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            /**
             * Generate the data. nDataAccessDbs should have a bigger size of
             * data entry; regularDbs only make data entry equal to
             * (index + "th-dataEntry").
             */
            private void makeData(DatabaseEntry data,
                                  int index,
                                  boolean isDataAccessDb) throws Exception {

                assert (data != null) : "makeData: Null data pointer";

                if (isDataAccessDb) {
                    byte[] bytes = new byte[1024];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = (byte) i;
                    }
                    data.setData(bytes);
                } else {
                    data.setData((Integer.toString(index) + "th-dataEntry").
                            getBytes("UTF-8"));
                }
            }
        }

        /*
         * Initialize "nRegularDbs" regular Dbs, one dataAccessDb and one
         * metaDataDb according to these rules:
         * - The "nRegularDBs" databases, with the dbIds range from
         *   0 to (nRegularDBs - 1). Each of them would have "nDbRecords".
         * - 10% of all "nRegularDBs" are deferredWrite databases.
         * - 90% of all "nRegularDBs" are regular databases.
         * - The dataAccessDb has "10 * nDbRecords" key/data pairs.
         * - The metaDataDb is to save "nRegularDbs" info for contention test.
         */
        try {
            openEnv(128 * 1024 * 1024);
            saveMetadata();
            InitThread[] threads = new InitThread[nInitThreads];
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < threads.length; i++) {
                InitThread t = new InitThread(i, env);
                t.start();
                threads[i] = t;
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            long endTime = System.currentTimeMillis();
            if (verbose) {
                float elapsedSeconds =  (endTime - startTime) / 1000f;
                float throughput = (nRegularDbs * nDbRecords) / elapsedSeconds;
                System.out.println
                    ("\nInitialization Statistics Report" +
                     "\n Run starts at: " + (new java.util.Date(startTime)) +
                     ", finishes at: " + (new java.util.Date(endTime)) +
                     "\n Initialized " + nRegularDbs + " databases, " +
                     "each contains " + nDbRecords + " records." +
                     "\n Elapsed seconds: " + elapsedSeconds +
                     ", throughput: " + throughput + " ops/sec.");
            }
            closeEnv();
        } catch (DatabaseException de) {
            de.printStackTrace();
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Simulate some contentions to make sure that the eviction would not
     * cause corruption or concurrency bugs.
     */
    private void doContention() {

        class ContentionThread extends Thread {
            public int id;
            private float dataCheckPossibility = .01f;
            private long txns;
            private boolean done = false;
            private Database currentDb = null;
            private Database lastOpenedDb = null;

            /**
             * Constructor used for initializing databases.
             */
            ContentionThread(int id, long txns) {
                this.id = id;
                this.txns = txns;
            }

            public void run() {
                try {

                    /* Start dataAccessThread here. */
                    startDataAccessor();

                    /*
                     * All contention threads try to open "nDbsPerSet" DBs
                     * from the same set concurrently.
                     */
                    while (!done) {
                        int dbId = random.nextInt(nDbsPerSet);
                        currentDb = env.openDatabase(null, "db" + dbId, null);
                        if (lastOpenedDb != null) {
                            lastOpenedDb.close();
                        }
                        lastOpenedDb = currentDb;
                        if (random.nextFloat() <= dataCheckPossibility) {
                            verifyData();
                        }
                        nOps[id]++;
                        if (nOps[id] > txns) {
                            if (lastOpenedDb != null) {
                                lastOpenedDb.close();
                            }
                            done = true;
                        }
                    }

                    /* Stop dataAccessThread here. */
                    stopDataAccessor();
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            private void startDataAccessor() {
                runDataAccessThread = true;
            }

            private void stopDataAccessor() {
                runDataAccessThread = false;
            }

            /**
             * Do the corruption check: just check that the data
             * that is present looks correct.
             */
            private void verifyData() throws Exception {
                long dbCount = currentDb.count();
                if (dbCount != nDbRecords) {
                    System.err.println
                        ("WARNING: total records in " +
                         currentDb.getDatabaseName() + ": " + dbCount +
                         " doesn't meet the expected value: " + nDbRecords);
                    System.exit(1);
                } else {
                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry data = new DatabaseEntry();
                    for (int i = 0; i < nDbRecords; i++) {
                        key.setData(Integer.toString(i).getBytes("UTF-8"));
                        OperationStatus status =
                            currentDb.get(null, key, data, LockMode.DEFAULT);
                        if (status != OperationStatus.SUCCESS) {
                            System.err.println
                                ("ERROR: failed to retrieve the #" +
                                 i + " key/data pair from " +
                                 currentDb.getDatabaseName());
                            System.exit(1);
                        } else if (!(new String(data.getData(), "UTF-8")).
                                equals((Integer.toString(i) +
                                        "th-dataEntry"))) {
                            System.err.println
                                ("ERROR: current key/data pair: " + i +
                                 "/" + (new String(data.getData(), "UTF-8")) +
                                 " doesn't match the expected: " +
                                 i + "/" + i +"th-dataEntry in " +
                                 currentDb.getDatabaseName());
                            System.exit(1);
                        }
                    }
                }
            }
        }

        class DataAccessThread extends Thread {

            public void run() {
                try {
                    while (runDataAccessThread) {
                        /* Access records to fill up cache. */
                        DatabaseEntry key = new DatabaseEntry();
                        key.setData(Integer.
                                    toString(random.nextInt(10 * nDbRecords)).
                                    getBytes("UTF-8"));
                        DatabaseEntry data = new DatabaseEntry();
                        OperationStatus status =
                            dataAccessDb.get(null, key, data,
                                             LockMode.DEFAULT);
                        if (status != OperationStatus.SUCCESS) {
                            System.err.println
                                ("ERROR: failed to retrieve the #" +
                                 new String(key.getData(), "UTF-8") +
                                 " key/data pair from dataAccessDb.");
                            System.exit(1);
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }

        /*
         * Simulate some contentions according to following rules:
         * - Several threads try to open/close a set of databases repeatedly.
         * - The other thread will continually access records from dataAccessDb
         *   to fill up cache.
         */
        try {
            long startTime = System.currentTimeMillis();
            long txns = nTotalTxns;
            if (recovery) {
                txns = nTxnPerRecovery;
            }
            for (int loop = 0; loop < nTotalTxns / txns; loop++) {
                /* Clear nOps[] before each run starts. */
                for (int i = 0; i < nContentionThreads; i++) {
                    nOps[i] = 0;
                }
                openEnv(1024 * 1024);
                readMetadata();
                DataAccessThread dat = new DataAccessThread();
                ContentionThread[] threads =
                    new ContentionThread[nContentionThreads];
                for (int i = 0; i < threads.length; i++) {
                    ContentionThread t =
                        new ContentionThread(i, txns);
                    t.start();
                    threads[i] = t;
                }
                dat.start();
                for (int i = 0; i < threads.length; i++) {
                    threads[i].join();
                }
                dat.join();
                if (!checkStats(txns)) {
                    System.err.println
                        ("doContention: stats check failed.");
                    System.exit(1);
                }
                closeEnv();
            }
            long endTime = System.currentTimeMillis();
            float elapsedSecs =  (endTime - startTime) / 1000f;
            float throughput = nTotalTxns / elapsedSecs;
            if (verbose) {
                System.out.println
                    ("\nContention Test Statistics Report" +
                     "\n Starts at: " + (new java.util.Date(startTime)) +
                     ", Finishes at: " + (new java.util.Date(endTime)) +
                     "\n Total operations: " + nTotalTxns +
                     ", Elapsed seconds: " + elapsedSecs +
                     ", Throughput: " + throughput + " ops/sec.");
            }
        } catch (DatabaseException de) {
            de.printStackTrace();
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void doEvict() {
        final int offset = random.nextInt(nRegularDbs - nKeepOpenedDbs);

        class EvictThread extends Thread {
            public int id;
            private float dataAccessPossibility = .01f;
            private long txns = 0;
            private Database currentDb = null;
            private Database lastOpenedDb = null;

            /**
             * Constructor.
             */
            public EvictThread(int id, long txns) {
                this.id = id;
                this.txns = txns;
            }

            public void run() {
                try {
                    int dbId;
                    boolean done = false;
                    while (!done) {
                        dbId = random.nextInt(nRegularDbs);
                        if ((0 <= (dbId - offset)) &&
                                ((dbId - offset) < nKeepOpenedDbs)) {

                            /*
                             * Randomly select nKeepOpenedDbs databases opened
                             * in a time. The dbId ranges from <offset> to
                             * <offset + nKeepOpenedDbs - 1>.
                             */
                            if (openDbList[dbId - offset] == null) {
                                openDbList[dbId - offset] =
                                    env.openDatabase(null, "db" + dbId, null);
                            }
                        } else {
                            /* Each thread select randomly from all DBs. */
                            currentDb =
                                env.openDatabase(null, "db" + dbId, null);
                            if (random.nextFloat() < dataAccessPossibility) {
                                DatabaseEntry key = new DatabaseEntry();
                                DatabaseEntry data = new DatabaseEntry();
                                key.setData(Integer.toString
                                            (random.nextInt(nDbRecords)).
                                            getBytes("UTF-8"));
                                currentDb.get(null, key, data,
                                              LockMode.DEFAULT);
                            }
                            if (lastOpenedDb != null) {
                                lastOpenedDb.close();
                            }
                            lastOpenedDb = currentDb;
                        }
                        nOps[id]++;
                        if (nOps[id] > txns) {
                            if (lastOpenedDb != null) {
                                lastOpenedDb.close();
                            }
                            /* Close nKeepOpenedDbs before exit. */
                            for (int i = 0; i < nKeepOpenedDbs; i++) {
                                currentDb = openDbList[i];
                                if (currentDb != null) {
                                    currentDb.close();
                                    openDbList[i] = null;
                                }
                            }
                            done = true;
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }

        /*
         * Simulate some contentions according to following rules:
         * - Several threads try to open/close a set of databases repeatedly.
         * - The other thread will continually access records from dataAccessDb
         *   to fill up cache.
         */
        try {
            long startTime = System.currentTimeMillis();
            long txns = nTotalTxns;
            if (recovery) {
                txns = nTxnPerRecovery;
            }
            for (int loop = 0; loop < nTotalTxns / txns; loop++) {
                /* Clear nOps[] before each run starts. */
                for (int i = 0; i < nContentionThreads; i++) {
                    nOps[i] = 0;
                }
                /* When using Zing JDK, the cache size should be increased. */
                if (JVMSystemUtils.ZING_JVM) {
                    openEnv(8 * 1024 * 1024);
                } else {
                    openEnv(512 * 1024);
                }
                readMetadata();
                EvictThread[] threads = new EvictThread[nContentionThreads];
                for (int i = 0; i < threads.length; i++) {
                    EvictThread t = new EvictThread(i, txns);
                    t.start();
                    threads[i] = t;
                }
                for (int i = 0; i < threads.length; i++) {
                    threads[i].join();
                }
                if (!checkStats(txns)) {
                    System.err.println("doEvict: stats check failed.");
                    System.exit(1);
                }
                closeEnv();
            }
            long endTime = System.currentTimeMillis();
            if (verbose) {
                float elapsedSeconds =  (endTime - startTime) / 1000f;
                float throughput = nTotalTxns / elapsedSeconds;
                System.out.println
                    ("\nEviction Test Statistics Report" +
                     "\n Run starts at: " + (new java.util.Date(startTime)) +
                     ", finishes at: " + (new java.util.Date(endTime)) +
                     "\n Total operations: " + nTotalTxns +
                     ", Elapsed seconds: " + elapsedSeconds +
                     ", Throughput: " + throughput + " ops/sec.");
            }
        } catch (DatabaseException de) {
            de.printStackTrace();
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Open an Environment.
     */
    private void openEnv(long cacheSize) throws DatabaseException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setCacheSize(cacheSize);
        if (offHeap) {
            /* Do not reduce main cache size, test will run too slowly. */
            envConfig.setOffHeapCacheSize(cacheSize);
        }
        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(homeDir), subDir, true);
        }
        env = new Environment(new File(homeDir), envConfig);
        if (contention) {
            dataAccessDb = env.openDatabase(null, "dataAccessDb", null);
        }
    }

    /**
     * Check to see if stats looks correct.
     */
    private boolean checkStats(long txns) throws DatabaseException {

        /* Get EnvironmentStats numbers. */
        StatsConfig statsConfig = new StatsConfig();
        statsConfig.setFast(true);
        statsConfig.setClear(true);
        EnvironmentStats stats = env.getStats(statsConfig);
        long evictedINs = stats.getNNodesExplicitlyEvicted();
        long evictedRoots = stats.getNRootNodesEvicted();
        long dataBytes = stats.getDataBytes();
        /* Check the eviction of INs and ROOTs actually happens. */
        boolean nodesCheck = (evictedINs > 0);
        boolean rootsCheck = (evictedRoots > 0);
        if (verbose) {
            System.out.printf
                ("\n\tEviction Statistics(calc txns: %d)%n" +
                 "                Data     Pass/Fail%n" +
                 "             ----------  ---------%n" +
                 "EvictedINs:  %10d  %9S%n" +
                 "EvictedRoots:%10d  %9S%n" +
                 "DataBytes:   %10d%n" +
                 "jvm.maxMem:  %10d%n" +
                 "jvm.freeMem: %10d%n" +
                 "jvm.totlMem: %10d%n",
                 txns, evictedINs, (nodesCheck ? "PASS" : "FAIL"),
                 evictedRoots, (rootsCheck ? "PASS" : "FAIL"),
                 dataBytes, rt.maxMemory(), rt.freeMemory(), rt.totalMemory());
            System.out.println
                ("The test criteria: EvictedINs > 0, EvictedRoots > 0.");
        }

        return nodesCheck && rootsCheck;
    }

    /**
     * Close the Databases and Environment.
     */
    private void closeEnv() throws DatabaseException {

        if (dataAccessDb != null) {
            dataAccessDb.close();
        }
        if (env != null) {
            env.close();
        }
    }

    /**
     * Store meta-data information into metadataDb.
     */
    private void saveMetadata() throws Exception {

        /* Store meta-data information into one additional database. */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        metadataDb = env.openDatabase(null, "metadataDb", dbConfig);
        OperationStatus status =
            metadataDb.put(null,
                           new DatabaseEntry("nRegularDbs".getBytes("UTF-8")),
                           new DatabaseEntry(Integer.
                                             toString(nRegularDbs).
                                             getBytes("UTF-8")));
        if (status != OperationStatus.SUCCESS) {
            System.err.println
                ("Not able to save info into the metadata database.");
            System.exit(1);
        }
        metadataDb.close();
    }

    /**
     * Retrieve meta-data information from metadataDb.
     */
    private void readMetadata() throws Exception {

        /* Retrieve meta-data information from metadataDB. */
        metadataDb = env.openDatabase(null, "metadataDb", null);
        DatabaseEntry key = new DatabaseEntry("nRegularDbs".getBytes("UTF-8"));
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status =
            metadataDb.get(null, key, data, LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) {
            System.err.println
                ("Couldn't retrieve info from the metadata database.");
            System.exit(1);
        }
        nRegularDbs = Integer.parseInt(new String (data.getData(), "UTF-8"));
        metadataDb.close();
    }
}
