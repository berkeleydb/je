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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.TracerFormatter;

/**
 * Used to test a queue-like application that appends data at the end of a key
 * range and then deletes it later at the front of the key range.  There are
 * several different types of workloads (see Workload subclasses) and multiple
 * workloads can be run concurrently with separate configurations.
 *
 * For example, the following was used to simulate a worst-case scenario where
 * JE MapLNs would be flushed for a large number of databases at every
 * checkpoint.
 *
 *  java -XX:+UseCompressedOops -Xmx3g -Xms3g -cp je-5.0.73.jar:classes \
 *      MixedLifetimeQueue -h /tmp/data \
 *      -workload long-lived -chunkAcrossDbs 500 \
 *          -ops 1500 -key 16 -data 200 -interval 1 S -lifetime 6 H \
 *      -workload short-lived -chunkAcrossDbs 500 \
 *          -ops 500 -key 16 -data 3000 -interval 50 MS -lifetime 1 MIN
 */
public class MixedLifetimeQueue {

    private static void usage(final String msg) {

        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println
            ("usage:" +
             "\njava "  + MixedLifetimeQueue.class.getName() +
             "\n   -h <envHome>" +
             "\n      # Environment home directory" +
             "\n  [-dbs <entries>]" +
             "\n      # Number of entries per Btree node; default: 128" +
             "\n      # default: write sequential keys");

        System.exit(2);
    }

    public static void main(String[] args) {
        try {
            System.out.print("Command line: ");
            for (String s : args) {
                System.out.print(s);
                System.out.print(' ');
            }
            System.out.println();
            final long start = System.currentTimeMillis();
            System.out.println("Opening environment");
            MixedLifetimeQueue test = new MixedLifetimeQueue(args);
            System.out.println("Starting test");
            test.execute();
            test.close();
            final long end = System.currentTimeMillis();
            System.out.println("Time: " + ((end - start) / 1000) + " sec");
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    final Environment env;
    final long runTimeMs;
    final AtomicLong sequenceNumber = new AtomicLong();
    final List<Workload> workloads = new ArrayList<Workload>();
    final ScheduledExecutorService executor;
    AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    final TracerFormatter dateFormat = new TracerFormatter();

    MixedLifetimeQueue(final String[] argsArray) {

        final List<String> argsList = Arrays.asList(argsArray);
        final List<List<String>> argsPartitions =
            partitionList(argsList, "-workload", false);
        final List<String> firstArgPartition = argsPartitions.get(0);
        final List<List<String>> workloadArgPartitions =
            argsPartitions.subList(1, argsPartitions.size());

        final String envHome = getStringArg(firstArgPartition, "-h", null);
        env = openEnv(new File(envHome));

        runTimeMs = getDurationArg(firstArgPartition, "-runtime", 0);

        executor = Executors.newScheduledThreadPool(
            workloadArgPartitions.size() * 2 + 1);

        for (List<String> args : workloadArgPartitions) {
            final String name = getStringArg(args, "-workload", null);
            final boolean chunkByDb = getSwitchArg(args, "-chunkByDb");
            final int chunkAcrossDbs = getIntArg(args, "-chunkAcrossDbs", 0);
            final int nOps = getIntArg(args, "-ops", null);
            final int keySize = getIntArg(args, "-key", null);
            if (keySize < 16) {
                throw new IllegalArgumentException("Invalid key size: " +
                                                   keySize);
            }
            final int dataSize = getIntArg(args, "-data", null);
            final int intervalMs = getDurationArg(args, "-interval", null);
            final int lifetimeMs = getDurationArg(args, "-lifetime", null);
            final Workload workload =
                createWorkload(name, chunkByDb, chunkAcrossDbs, nOps,
                               keySize, dataSize, intervalMs, lifetimeMs);
            workloads.add(workload);
            System.out.println("Workload defined: " + args);
        }
    }

    static Environment openEnv(final File envHome) {
        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        //envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setCacheMode(CacheMode.EVICT_LN);
        /*
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_FETCH_OBSOLETE_SIZE,
                                 "true");
        */
        //envConfig.setConfigParam("je.cleaner.calc.recentLNSizes", "40");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                 String.valueOf(100 * 1024 * 1024));
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE,
                                 "2000");
        envConfig.setConfigParam(EnvironmentConfig.LOG_NUM_BUFFERS, "16");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                                 "40");
        envConfig.setConfigParam(EnvironmentConfig.LOG_WRITE_QUEUE_SIZE,
                                 "2097152");
        envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
                                 String.valueOf(200 * 1024 * 1024));
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, "4");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE,
                                 "1048576");
        envConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, "23");
        return new Environment(envHome, envConfig);
    }

    Database openDb(final String dbName) {
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        return env.openDatabase(null, dbName, dbConfig);
    }

    void close()
        throws InterruptedException {

        executor.shutdown();
        boolean cleanShutdown = executor.awaitTermination(5, TimeUnit.MINUTES);
        for (final Workload w : workloads) {
            w.close();
        }
        env.close();
        if (!cleanShutdown) {
            throw new IllegalStateException(
                    "Could not terminate executor after 5 minutes");
        }
    }

    void execute()
        throws InterruptedException {

        startMonitor();
        for (final Workload w : workloads) {
            w.start();
        }
        synchronized (shutdownFlag) {
            shutdownFlag.wait();
        }
    }

    void startMonitor() {
        final StatsConfig statsConfig = new StatsConfig().setClear(true);
        final long startTime = System.currentTimeMillis();
        executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    long now = System.currentTimeMillis();
                    System.out.println(dateFormat.getDate(now));
                    System.out.println(env.getStats(statsConfig));
                    System.out.println();

                    if ((runTimeMs > 0) &&
                        (startTime + runTimeMs >= now)) {
                        shutdownFlag.set(true);
                        shutdownFlag.notify();
                    }
                }
            }, 0, 1, TimeUnit.MINUTES);
    }

    Workload createWorkload(final String name,
                            final boolean chunkByDb,
                            final int chunkAcrossDbs,
                            final int nOps,
                            final int keySize,
                            final int dataSize,
                            final int intervalMs,
                            final int lifetimeMs) {
        if (chunkByDb) {
            return new ChunkByDb(name, nOps, keySize, dataSize, intervalMs,
                                 lifetimeMs);
        }
        if (chunkAcrossDbs > 0) {
            return new ChunkAcrossDbs(name, nOps, keySize, dataSize,
                                      intervalMs, lifetimeMs, chunkAcrossDbs);
        }
        return new ChunkByKeyRange(name, nOps, keySize, dataSize, intervalMs,
                                   lifetimeMs);
    }

    abstract class Workload {

        final String name;
        final int nOps;
        final int keySize;
        final int dataSize;
        final int intervalMs;
        final int lifetimeMs;
        final byte[] dataBytes;
        final byte[] keyExtraBytes;

        Workload(final String name,
                 final int nOps,
                 final int keySize,
                 final int dataSize,
                 final int intervalMs,
                 final int lifetimeMs) {
            this.name = name;
            this.nOps = nOps;
            this.keySize = keySize;
            this.dataSize = dataSize;
            this.intervalMs = intervalMs;
            this.lifetimeMs = lifetimeMs;
            dataBytes = new byte[dataSize];
            keyExtraBytes = new byte[Math.min(0, keySize - 16)];
        }

        void log(String msg) {
            System.out.println(dateFormat.getDate(System.currentTimeMillis()) +
                               "[" + name + "] " + msg);
        }

        void start() {

            executor.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        if (!shutdownFlag.get()) {
                            try {
                                runInsertions();
                            } catch (Throwable e) {
                                e.printStackTrace();
                                System.exit(1);
                            }
                        }
                    }
                }, 0, intervalMs, TimeUnit.MILLISECONDS);

            executor.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        if (!shutdownFlag.get()) {
                            try {
                                runDeletions();
                            } catch (Throwable e) {
                                e.printStackTrace();
                                System.exit(1);
                            }
                        }
                    }
                }, intervalMs / 2, intervalMs, TimeUnit.MILLISECONDS);
        }

        abstract void close();

        abstract void runInsertions();

        abstract void runDeletions();
        
        byte[] getNewKey() {
            final TupleOutput os = new TupleOutput();
            os.writeLong(System.currentTimeMillis());
            os.writeLong(sequenceNumber.incrementAndGet());
            os.writeFast(keyExtraBytes);
            return os.toByteArray();
        }
        
        long getTimeFromKey(final DatabaseEntry entry) {
            final TupleInput input = new TupleInput(entry.getData(),
                                                    entry.getOffset(),
                                                    entry.getSize());
            return input.readLong();
        }

        String getDbName(final long time) {
            return name + "#" + dateFormat.getDate(time);
        }

        void insertRecords(final Database db, final int nRecords) {
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry value = new DatabaseEntry();
            for (int i = 0; i < nRecords; i += 1) {
                key.setData(getNewKey());
                value.setData(dataBytes);
                final OperationStatus status = db.putNoOverwrite(null, key,
                                                                 value);
                if (status != OperationStatus.SUCCESS) {
                    throw new IllegalStateException(status.toString());
                }
            }
        }

        int deleteRecords(final Database db, final long lifetimeMs) {
            final long maxTime = System.currentTimeMillis() - lifetimeMs;
            int cnt = 0;
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry value = new DatabaseEntry();
            value.setPartial(0, 0, true);
            boolean done = false;
            while (!done) {
                final Transaction txn = env.beginTransaction(null, null);
                try {
                    final Cursor cursor = db.openCursor(txn, null);
                    try {
                        for (int i = 0; i < 1000; i += 1) {
                            final OperationStatus status =
                                cursor.getNext(key, value, LockMode.RMW);
                            if (status != OperationStatus.SUCCESS) {
                                done = true;
                                break;
                            }
                            if (getTimeFromKey(key) > maxTime) {
                                done = true;
                                break;
                            }
                            cursor.delete();
                            cnt += 1;
                        }
                    } finally {
                        cursor.close();
                    }
                    txn.commit();
                } finally {
                    if (txn.isValid()) {
                        txn.abort();
                    }
                }
            }
            return cnt;
        }

        String insertDatabase(final int nRecords) {
            final String dbName = getDbName(System.currentTimeMillis());
            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setExclusiveCreate(true);
            dbConfig.setTransactional(true);
            final Database newDb = env.openDatabase(null, dbName, dbConfig);
            insertRecords(newDb, nRecords);
            newDb.close();
            return dbName;
        }

        SortedSet<String> deleteDatabases(final long lifetimeMs) {
            final String firstName = name + "#";
            final String lastName = getDbName(System.currentTimeMillis() -
                                              lifetimeMs); 
            final SortedSet<String> dbNames = 
                new TreeSet<String>(env.getDatabaseNames()).
                subSet(firstName, lastName);
            for (final String dbName : dbNames) {
                env.removeDatabase(null, dbName);
            }
            return dbNames;
        }
    }

    /**
     * Each chunk (nOps records) is a separate database.
     */
    class ChunkByDb extends Workload {

        ChunkByDb(final String name,
                  final int nOps,
                  final int keySize,
                  final int dataSize,
                  final int intervalMs,
                  final int lifetimeMs) {
            super(name, nOps, keySize, dataSize, intervalMs, lifetimeMs);
        }

        @Override
        void close() {
        }

        @Override
        void runInsertions() {
            final String dbName = insertDatabase(nOps);
            log("Created DB " + dbName + " with " + nOps + " entries");
        }

        @Override
        void runDeletions() {
            final SortedSet<String> dbs = deleteDatabases(lifetimeMs);
            log("Deleted DBs " + dbs);
        }
    }

    /**
     * Each chunk is a key range, and all chunks (for this workload) are in a
     * single database.
     */
    class ChunkByKeyRange extends Workload {

        private final Database db;

        ChunkByKeyRange(final String name,
                        final int nOps,
                        final int keySize,
                        final int dataSize,
                        final int intervalMs,
                        final int lifetimeMs) {
            super(name, nOps, keySize, dataSize, intervalMs, lifetimeMs);
            db = openDb(name + "#");
        }

        @Override
        void close() {
            db.close();
        }

        @Override
        void runInsertions() {
            insertRecords(db, nOps);
            log("Inserted " + nOps + " entries");
        }

        @Override
        void runDeletions() {
            final int cnt = deleteRecords(db, lifetimeMs);
            log("Deleted " + cnt + " entries");
        }
    }

    /**
     * Each chunk is a key range that is evenly spread across multiple
     * databases.
     * <p>
     * This is meant to simulate a worst-case scenario for the flushing of
     * MapLNs in a NoSQL DB app, since writing a single chunk dirties the
     * metadata for many databases.  But it isn't a realistic queue app for
     * NoSQL DB, since in such an app chunks should be designed to have the
     * same major key (like LOBs) and would not be spread across databases.
     */
    class ChunkAcrossDbs extends Workload {

        private final int nDbs;
        private final List<Database> dbList;
        private int nextDbIndex = 0;

        ChunkAcrossDbs(final String name,
                       final int nOps,
                       final int keySize,
                       final int dataSize,
                       final int intervalMs,
                       final int lifetimeMs,
                       final int nDbs) {
            super(name, nOps, keySize, dataSize, intervalMs, lifetimeMs);
            this.nDbs = nDbs;
            dbList = new ArrayList<Database>(nDbs);
            for (int i = 0; i < nDbs; i += 1) {
                dbList.add(openDb(name + "#" + i));
            }
        }

        @Override
        void close() {
            for (final Database db : dbList) {
                db.close();
            }
        }

        @Override
        void runInsertions() {
            for (int i = 0; i < nOps; i += 1) {
                insertRecords(dbList.get(nextDbIndex), 1);
                if (nextDbIndex >= dbList.size()) {
                    nextDbIndex = 0;
                }
            }
            log("Inserted " + nOps + " entries");
        }

        @Override
        void runDeletions() {
            int cnt = 0;
            for (final Database db : dbList) {
                cnt += deleteRecords(db, lifetimeMs);
            }
            log("Deleted " + cnt + " entries");
        }
    }

    static boolean getSwitchArg(final List<String> args,
                                final String name) {
        return args.contains(name);
    }

    static String getStringArg(final List<String> args,
                               final String name,
                               final String defaultValue) {
        final int i = args.indexOf(name);
        if (i < 0) {
            if (defaultValue == null) {
                usage("Missing " + name);
            }
            return defaultValue;
        }
        if (i + 1 >= args.size()) {
            usage("Missing value after " + name);
        }
        final String val = args.get(i + 1);
        if (val.startsWith("-")) {
            usage("Invalid value after " + name + " : " + val);
        }
        return val;
    }

    static int getIntArg(final List<String> args,
                         final String name,
                         final Integer defaultValue) {
        final String val = getStringArg(args, name,
                                        (defaultValue != null) ?
                                        String.valueOf(defaultValue) :
                                        null);
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            usage("Invalid number after " + name + " : " + val);
            return 0; // for compiler
        }
    }

    static int getDurationArg(final List<String> args,
                              final String name,
                              final Integer defaultValue) {
        final int i = args.indexOf(name);
        if (i < 0) {
            if (defaultValue == null) {
                usage("Missing " + name);
            }
            return defaultValue;
        }
        if (i + 2 >= args.size()) {
            usage("Missing time and units after " + name);
        }
        final String val = args.get(i + 1) + ' ' + args.get(i + 2);
        try {
            return PropUtil.parseDuration(val);
        } catch (IllegalArgumentException e) {
            usage("Invalid duration after " + name + " : " + val +
                  ", " + e.getMessage());
            return 0; // for compiler
        }
    }

    static <T> List<List<T>> partitionList(final List<T> list,
                                           final T divider,
                                           final boolean appendDivider) {
        final List<List<T>> results = new ArrayList<List<T>>();
        int start = 0;
        for (int i = 0; i < list.size(); i += 1) {
            if (divider.equals(list.get(i))) {
                final int nextStart = appendDivider ? (i + 1) : i;
                results.add(list.subList(start, nextStart));
                start = nextStart;
            }
        }
        results.add(list.subList(start, list.size()));
        return results;
    }
}
