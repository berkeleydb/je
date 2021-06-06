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
import java.util.concurrent.CountDownLatch;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.evictor.Evictor;

/**
 * Before 4.1.9, if users try to open multiple shared cache environments
 * (with different enviroment home) in multiple threads, they may run into the
 * issue that those shared cache environments are not sharing the cache.
 *
 * This is because we divide the DbEnvPool.getEnvironment into two 
 * synchronization blocks, for shared cache environment specially, the two 
 * blocks do different things:
 * 1. First synchronization block looks for an existed shared cache environment
 * before creating a new shared cache environment, so that it can reuse the
 * cache.
 * 2. Second synchroniation block will add a shared cache environment into the
 * shared cache list after its recovery is done.
 *
 * This would bring issues when we open environments in multiple threads:
 * 1. Thread A opens a shared cache environment, it doesn't find any shared 
 * cache environment instance in the list at that time
 * 2. Thread A sleeps before it goes to the second synchronization block, so 
 * currently no shared cache environments in the list
 * 3. Thread B starts to open another shared cache environment, it goes to the
 * first block, find no instance in the list, it won't use the shared cache
 *
 * The above scenario results in multiple shared cache environments are using
 * different cacches, and may result in OOME, see [#19165] for more details.
 *
 * This test simulates this case, it will open shared cache enviroments in
 * different threads and do some database operations. When operations in all
 * threads are finished, before closing the database, this test will check to
 * see if the SharedCacheTotalBytes in each environment are the same, also it
 * will check that all the environments are using the same Evictor.
 */
public class OpenEnvStress {
    private static final String DIR_PREFIX = "env";
    private static final String DB_NAME = "testDb";
    /* Root dir for environment directories. */
    private File envRoot;
    /* Number of updating threads. */
    private int numEnvs = 10;
    /* Total update operations. */
    private int totalIterations = 5000;
    /* True if open Environments shared chace. */
    private boolean sharedCache = false;

    public static void main(String args[]) {
        try {
            OpenEnvStress test = new OpenEnvStress();
            test.parseArgs(args);
            test.doWork();
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(-1);
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

    private void usage(String error) {
        if (error != null) {
            System.err.println(error);
        }
        System.err.println
            ("java " + getClass().getName() + '\n' +
             "      [-root <envRoot>] [-numEnvs <number of environments>]\n" +
             "      [-totalIterations <total iterations of open/close " +
             "environments>]\n" +
             "      [-sharedCache <true if environments share cache>]\n");
        System.exit(1);
    }
    
    private void parseArgs(String args[]) {
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException();
            }
            /* Parse command-line input arguments. */
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                boolean moreArgs = i < args.length - 1;
                if (arg.equals("-root") && moreArgs) {
                    envRoot = new File(args[++i]);
                } else if (arg.equals("-numEnvs") && moreArgs) {
                    numEnvs = Integer.parseInt(args[++i]);
                } else if (arg.equals("-totalIterations") && moreArgs) {
                    totalIterations = Integer.parseInt(args[++i]);
                } else if (arg.equals("-sharedCache") && moreArgs) {
                    sharedCache = new Boolean(args[++i]);
                } else {
                    usage("Unknown arg: " + arg);
                }
            }

            if (envRoot == null) {
                throw new IllegalArgumentException
                    ("-root is an required argument.");
            }

            if (numEnvs <= 0 || totalIterations <= 0) {
                throw new IllegalArgumentException
                    ("-numEnvs and -totalIterations require positive " +
                     "argument.");
            }

            printArgs(args);
        } catch (IllegalArgumentException e) {
            usage("IllegalArguments! ");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /* Do the work. */
    public void doWork() 
        throws Exception {

        System.out.println("Staring test.....");

        /* 
         * Start multiple threads to open multiple Environments, to make sure 
         * the issue described in SR [#19165] is gone, it would be better to
         * run this process multiple times.
         */
        for (int i = 1; i <= totalIterations; i++) {
            initEnvDirs(i == 1);
            CountDownLatch startSignal = new CountDownLatch(1);
            CountDownLatch endSignal = new CountDownLatch(numEnvs);
            OpenEnvAndWorkThread[] threads = new OpenEnvAndWorkThread[numEnvs];
            for (int j = 1; j <= numEnvs; j++) {
                threads[j - 1] = new OpenEnvAndWorkThread
                    (startSignal, endSignal, 
                     new File(envRoot, DIR_PREFIX + j), j);
                threads[j - 1].start();
            }
            startSignal.countDown();
            endSignal.await();
            doSharedCacheCheck(threads);
            for (int j = 1; j <= numEnvs; j++) {
                threads[j - 1].closeEnv();
            }

            if (i % 1000 == 0) {
                System.out.println("Iteration: " + i + " finishes.");
            }
        }
    }

    private void initEnvDirs(boolean create) {
        /* Create Environment home directories for Environments. */
        if (create) {
            for (int i = 1; i <= numEnvs; i++) {
                File envFile = new File(envRoot, DIR_PREFIX + i);
                if (!envFile.exists()) {
                    assert envFile.mkdir();
                }
            }

            return;
        }

        /* Remove the log files in the Environment home directory. */
        File[] files = envRoot.listFiles();
        for (File file : files) {
            if (file.isDirectory() && file.getName().contains(DIR_PREFIX)) {
                File[] logFiles = file.listFiles();
                for (File logFile : logFiles) {
                    assert logFile.delete();
                }
            }
        }
    }

    /* 
     * Check that SharedCache Environments are using the same Evictor and the 
     * same shared cache. 
     */
    private void doSharedCacheCheck(OpenEnvAndWorkThread[] threads) {
        if (!sharedCache) {
            return;
        }

        Evictor evictor = null;
        long sharedCacheBytes = 0;
        for (OpenEnvAndWorkThread thread : threads) {
            if (evictor == null) {
                evictor = getEvictor(thread);
                sharedCacheBytes = getSharedCacheBytes(thread);
            } else {
                final Evictor tmpEvictor = getEvictor(thread);
                final long tmpSharedCacheBytes = getSharedCacheBytes(thread);
                if (evictor == tmpEvictor &&
                    sharedCacheBytes == tmpSharedCacheBytes) {
                   continue;
                }
                
                throw new IllegalStateException
                    ("SharedCache Environments in the same JVM don't really " +
                     "use the same cache" + ". First evictor is " + evictor +
                     ". Current evictor is " + tmpEvictor + ". First " +
                     "sharedCacheBytes is " + sharedCacheBytes + ". Current " +
                     "sharedCacheBytes is " + tmpSharedCacheBytes); 
            }
        }
    }

    private Evictor getEvictor(OpenEnvAndWorkThread thread) {
        return DbInternal.getNonNullEnvImpl(thread.getEnv()).getEvictor();
    }

    /* Return the SharedCacheTotalBytes for an Environment. */
    private long getSharedCacheBytes(OpenEnvAndWorkThread thread) {
        StatsConfig stConfig = new StatsConfig();
        stConfig.setFast(true);

        EnvironmentStats envStats = thread.getEnv().getStats(stConfig);
        
        return envStats.getSharedCacheTotalBytes();
    }

    /* 
     * The thread which open/close a shared cache Environment, also do some 
     * database operations to consume cache. 
     */
    class OpenEnvAndWorkThread extends Thread {
        private final CountDownLatch start;
        private final CountDownLatch end;
        private final File envHome;
        private final int threadIndex;
        private Environment env;
        private Database db;

        public OpenEnvAndWorkThread(CountDownLatch start, 
                                    CountDownLatch end,
                                    File envHome,
                                    int threadIndex) {
            this.start = start;
            this.end = end;
            this.envHome = envHome;
            this.threadIndex = threadIndex;
        }

        public void run() {
            try {
                EnvironmentConfig envConfig = new EnvironmentConfig();
                envConfig.setAllowCreate(true);
                envConfig.setSharedCache(sharedCache);
                envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");

                start.await();

                env = new Environment(envHome, envConfig);

                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                db = env.openDatabase(null, DB_NAME, dbConfig);

                /* 
                 * Insert different data sets in different environments to 
                 * consume different cache, but we expect the same 
                 * SharedCacheTotalBytes among all environments. 
                 */
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                for (int i = 1; i <= threadIndex * 100; i++) {
                    IntegerBinding.intToEntry(i, key);
                    StringBinding.stringToEntry("herococo", data);
                    db.put(null, key, data);
                }

                end.countDown();
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        /* Close the database and environments. */
        public void closeEnv() {
            if (db != null) {
                db.close();
            }

            if (env != null) {
                env.close();
            }
        }

        /* Return the Environment handle. */
        public Environment getEnv() {
            return env;
        }
    }
}
