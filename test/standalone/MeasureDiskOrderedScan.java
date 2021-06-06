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
import java.io.IOException;
import java.util.Random;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Measures performance of a full scan with DiskOrderedCursor compared to a
 * regular cursor walk.
 *
 * The DB population and scan are run as two separate invocations, so that the
 * file system cache can be cleared prior to the scan.
 *
 * To populate the DB we insert and update (with the same value) each record to
 * produce a log that is around 50% utilized, to add realism.  With
 * insert-only, a log that is 100% utilized would result in less real/random
 * IO.
 *
 * Variants on scan:
 *  - key-only or key-plus-data
 *  - with or without duplicates
 *  - preload or not
 *
 * Other scan parameters:
 *  - internalMemoryLimit
 *  - lsnBatchSize
 *
 * Data set parameters, normally held constant across multiple runs:
 *  - writes are in key order or random order
 *  - number of records
 *  - key and data size
 *  - dups per key (for dup db)
 *  - JE cache size
 *
 * MeasureDiskOrderedScan.sh (in this directory) is a script for running this
 * program that can be copied and modified to run a subset of tests.
 */
public class MeasureDiskOrderedScan {

    private enum Action { Populate, DirtyReadScan, DiskOrderedScan };

    private String homeDir = "tmp";
    private Environment env = null;
    private Database db = null;
    private Action action = null;
    private boolean dupDb = false;
    private boolean keysOnly = false;
    private boolean preload = false;
    private boolean sequentialWrites = false;
    private int nRecords = 25 * 1000 * 1000;
    private int keySize = 10;
    private int dataSize = 1000;
    private long lsnBatchSize = Long.MAX_VALUE;
    private long internalMemoryLimit = 100L * 1000 * 1000;
    private long jeCacheSize = 1000L * 1000 * 1000;
    private Random random = new Random();
    private long startTime;
    private long endTime;

    public static void main(String args[]) {
        try {
            new MeasureDiskOrderedScan(args).run();
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private MeasureDiskOrderedScan(String args[]) {
        boolean dataSizeSpecified = false;
        for (int i = 0; i < args.length; i += 1) {
            final String arg = args[i];
            if (arg.equals("-dupDb")) {
                dupDb = true;
            } else if (arg.equals("-keysOnly")) {
                keysOnly = true;
            } else if (arg.equals("-preload")) {
                preload = true;
            } else if (arg.equals("-sequentialWrites")) {
                sequentialWrites = true;
            } else {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException
                        ("Missing value for arg: " + arg);
                }
                i += 1;
                final String value = args[i];
                if (arg.equals("-h")) {
                    homeDir = value;
                } else if (arg.equals("-action")) {
                    action = Enum.valueOf(Action.class, value);
                } else if (arg.equals("-nRecords")) {
                    nRecords = Integer.parseInt(value);
                } else if (arg.equals("-keySize")) {
                    keySize = Integer.parseInt(value);
                } else if (arg.equals("-dataSize")) {
                    dataSize = Integer.parseInt(value);
                    dataSizeSpecified = true;
                } else if (arg.equals("-lsnBatchSize")) {
                    lsnBatchSize = Long.parseLong(value);
                } else if (arg.equals("-internalMemoryLimit")) {
                    internalMemoryLimit = Long.parseLong(value);
                } else if (arg.equals("-jeCacheSize")) {
                    jeCacheSize = Long.parseLong(value);
                } else {
                    throw new IllegalArgumentException("Unknown arg: " + arg);
                }
            }
        }
        if (lsnBatchSize != Long.MAX_VALUE &&
            internalMemoryLimit != Long.MAX_VALUE) {
            throw new IllegalArgumentException
                ("Only one of lsnBatchSize and internalMemoryLimit may be " +
                 "specified (not equal to Long.MAX_VALUE)");
        }
        if (dupDb && !dataSizeSpecified) {
            dataSize = keySize;
        }
        printArgs(args);
    }
    
    private void printArgs(String[] args) {
        System.out.print("Command line arguments:");
        for (String arg : args) {
            System.out.print(' ');
            System.out.print(arg);
        }
        System.out.println();
        System.out.println("Effective arguments:" +
            " action=" + action +
            " dupDb=" + dupDb +
            " keysOnly=" + keysOnly +
            " preload=" + preload +
            " sequentialWrites=" + sequentialWrites +
            " nRecords=" + nRecords +
            " keySize=" + keySize +
            " dataSize=" + dataSize +
            " lsnBatchSize=" + lsnBatchSize +
            " internalMemoryLimit=" + internalMemoryLimit +
            " jeCacheSize=" + jeCacheSize);
    }
    
    private void run() throws IOException {

        open();
        if (preload) {
            db.preload(null); /* LNs are not loaded. */
        }
        final double startTime = System.currentTimeMillis();
        switch (action) {
            case Populate:
                populate();
                break;
            case DirtyReadScan:
                dirtyReadScan();
                break;
            case DiskOrderedScan:
                diskOrderedScan();
                break;
            default:
                fail(action);
        }
        final double endTime = System.currentTimeMillis();
        final double totalSecs = (endTime - startTime) / 1000;
        final double throughput = nRecords / totalSecs;
        System.out.println("\nTotal seconds: " + totalSecs +
                           " txn/sec: " + throughput);
        close();
    }

    private void open() throws IOException {

        final long minMemory =
            (internalMemoryLimit != Long.MAX_VALUE ? internalMemoryLimit : 0) +
            jeCacheSize + (jeCacheSize / 2);

        if (Runtime.getRuntime().maxMemory() < minMemory) {
            throw new IllegalArgumentException
                ("Must set heap size to at least internalMemoryLimit (if " +
                 "specified) plus 1.5 X jeCacheSize: " + minMemory);
        }

        final boolean create = (action == Action.Populate);

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(create);
        envConfig.setCacheSize(jeCacheSize);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                 String.valueOf(1000 * 1000 * 1000));

        env = new Environment(new File(homeDir), envConfig);
        
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(create);
        dbConfig.setExclusiveCreate(create);
        dbConfig.setSortedDuplicates(dupDb);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);

        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void close() {
        db.close();
        env.close();
    }

    private void populate() {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        for (long i = 0; i < nRecords; i += 1) {
            if (sequentialWrites) {
                makeLongKey(key, i);
            } else {
                makeRandomKey(key);
            }
            makeData(data);
            OperationStatus status;
            /* Insert */
            if (dupDb) {
                status = db.putNoDupData(null, key, data);
            } else {
                status = db.putNoOverwrite(null, key, data);
            }
            if (status != OperationStatus.SUCCESS) {
                fail(status);
            }
            /* Update to create waste */
            status = db.put(null, key, data);
            if (status != OperationStatus.SUCCESS) {
                fail(status);
            }
        }
    }

    private void dirtyReadScan() {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        if (keysOnly) {
            data.setPartial(0, 0, true);
        }
        final Cursor cursor = db.openCursor(null, null);
        int nScanned = 0;
        while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
               OperationStatus.SUCCESS) {
            if (sequentialWrites) {
                checkLongKey(key, nScanned);
            } else {
                checkAnyKey(key);
            }
            if (!keysOnly) {
                checkData(data);
            }
            nScanned += 1;
        }
        cursor.close();
        checkEquals(nRecords, nScanned);
    }

    private void diskOrderedScan() {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
        config.setKeysOnly(keysOnly);
        config.setInternalMemoryLimit(internalMemoryLimit);
        config.setLSNBatchSize(lsnBatchSize);
        final DiskOrderedCursor cursor = db.openCursor(config);
        int nScanned = 0;
        while (cursor.getNext(key, data, LockMode.READ_UNCOMMITTED) ==
               OperationStatus.SUCCESS) {
            checkAnyKey(key);
            if (!keysOnly) {
                checkData(data);
            }
            nScanned += 1;
        }
        cursor.close();
        checkEquals(nRecords, nScanned);
    }

    private void makeLongKey(DatabaseEntry key, long value) {
        final byte[] bytes = new byte[keySize];
        final TupleOutput out = new TupleOutput(bytes);
        out.writeLong(value);
        key.setData(bytes);
    }

    private void checkLongKey(DatabaseEntry key, long value) {
        checkEquals(keySize, key.getSize());
        final TupleInput in = new TupleInput(key.getData());
        checkEquals(value, in.readLong());
    }

    private void makeRandomKey(DatabaseEntry key) {
        final byte[] bytes = new byte[keySize];
        random.nextBytes(bytes);
        key.setData(bytes);
    }

    private void checkAnyKey(DatabaseEntry key) {
        checkEquals(keySize, key.getSize());
    }

    private void makeData(DatabaseEntry data) {
        final byte[] bytes = new byte[dataSize];
        for (int i = 0; i < bytes.length; i += 1) {
            bytes[i] = (byte) i;
        }
        data.setData(bytes);
    }

    private void checkData(DatabaseEntry data) {
        checkEquals(dataSize, data.getSize());
        final byte[] bytes = data.getData();
        for (int i = 0; i < bytes.length; i += 1) {
            checkEquals(bytes[i], (byte) i);
        }
    }

    private void fail(Object msg) {
        throw new IllegalStateException(msg.toString());
    }

    private void check(boolean cond) {
        if (!cond) {
            fail("check failed, see stack");
        }
    }

    private void checkEquals(Object o1, Object o2) {
        if (o1 == null || o2 == null) {
            if (o1 != null || o2 != null) {
                fail("Only one is null; o1=" + o1 + " o2=" + o2);
            }
        }
        if (!o1.equals(o2)) {
            fail("Not equal; o1=" + o1 + " o2=" + o2);
        }
    }
}
