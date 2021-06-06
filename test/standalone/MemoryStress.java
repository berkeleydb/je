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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Random;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.incomp.INCompressor;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;

/**
 * The original version of this test was written by Brian O'Neill of Amazon,
 * for SR 11163. It used to get OutOfMemoryError at 720,000 records on Linda's
 * laptop, on JE 1.5.3.
 */
public class MemoryStress {
    private Environment env;
    private Database db;
    private StatsConfig statsConfig;
    private EnvironmentImpl envImpl;

    private DecimalFormat decimalFormat;
    private NumberFormat numberFormat;

    private int reportingInterval = 10000;
    private int nextSerialKey = 0;

    private String environmentHome;
    private int numThreads;
    private boolean insertDups;
    private boolean serialKeys;
    private boolean doDelete;
    private boolean deleteExisting;
    private int totalOps = Integer.MAX_VALUE;

    /* accumulated stats */
    private int totalEvictPasses;
    private int totalSelected;
    private int totalScanned;
    private int totalExEvicted;
    private int totalStripped;
    private int totalCkpts;
    private int totalCleaned;
    private int totalNotResident;
    private int totalCacheMiss;
    private int subDir;

    public static void main(String[] args) {
        try {
            MemoryStress ms = new MemoryStress();
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                String arg2 = (i < args.length - 1) ? args[i + 1] : null;
                if (arg.equals("-h")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    ms.environmentHome = arg2;
                    i += 1;
                } else if (arg.equals("-nThreads")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        ms.numThreads = Integer.parseInt(arg2);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    i += 1;
                } else if (arg.equals("-nOps")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        ms.totalOps = Integer.parseInt(arg2);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    i += 1;
                } else if (arg.equals("-subDir")) {
                    if (arg2 == null) {
                        throw new IllegalArgumentException(arg);
                    }
                    try {
                        ms.subDir = Integer.parseInt(arg2);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(arg2);
                    }
                    i += 1;
                } else if (arg.equals("-dups")) {
                    ms.insertDups = true;
                } else if (arg.equals("-serial")) {
                    ms.serialKeys = true;
                } else if (arg.equals("-delete")) {
                    ms.doDelete = true;
                } else if (arg.equals("-deleteExisting")) {
                    ms.deleteExisting = true;
                } else {
                    throw new IllegalArgumentException(arg);
                }
            }
            if (ms.environmentHome == null) {
                throw new IllegalArgumentException("-h not specified");
            }
            ms.run();
            System.exit(0);
        } catch (IllegalArgumentException e) {
            System.out.println(
                "Usage: MemoryStress -h <envHome> [-nThreads <nThreads>" +
                "-nOps <nOps> -dups -serial -delete -deleteExisting]");
            e.printStackTrace();
            System.exit(2);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    MemoryStress() {
        decimalFormat = new DecimalFormat();
        decimalFormat.setMaximumFractionDigits(2);
        decimalFormat.setMinimumFractionDigits(2);

        numberFormat = NumberFormat.getInstance();

        statsConfig = new StatsConfig();
        statsConfig.setFast(true);
        statsConfig.setClear(true);
    }

    void run()
        throws DatabaseException, InterruptedException  {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setReadOnly(false);
        envConfig.setAllowCreate(true);
        envConfig.setTxnNoSync(true);
        //envConfig.setConfigParam("je.maxOffHeapMemory", "5000000");

        if (subDir > 0) {
            envConfig.setConfigParam
                (EnvironmentConfig.LOG_N_DATA_DIRECTORIES, subDir + "");
            Utils.createSubDirs(new File(environmentHome), subDir);
        }

        env = new Environment(new File(environmentHome), envConfig);

        EnvironmentConfig seeConfig = env.getConfig();
        System.out.println("maxMem = " +
                           numberFormat.format(seeConfig.getCacheSize()));
        System.out.println(seeConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(insertDups);
        dbConfig.setTransactional(true);
        dbConfig.setReadOnly(false);
        dbConfig.setAllowCreate(true);

        db = env.openDatabase(null, "test", dbConfig);

        Worker[] workers = new Worker[numThreads];
        for (int i = 0; i < numThreads; i++) {
            Worker w = new Worker(i, db, totalOps);
            w.start();
            workers[i] = w;
        }

        for (int i = 0; i < numThreads; i++) {
            workers[i].join();
        }

        db.close();

        long startTime = System.currentTimeMillis();
        env.close();
        String timeStr = numberFormat.format
            ((System.currentTimeMillis() - startTime)/1e3);
        System.out.println("Environment.close took " + timeStr + " seconds");
    }

    private class Worker extends Thread {

        public int id;
        Database db;
        private int totalOps;

        Worker(int id, Database db, int totalOps) {
            this.id = id;
            this.db = db;
            this.totalOps = totalOps;
        }

        public void run() {
            int count = 0;
            Random rnd = new Random(4361 + id);
            byte[] key = new byte[10];
            byte[] value = new byte[100];

            long start = System.currentTimeMillis();

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();

            try {
                int intervalCount = 0;
                long intervalStart = start;
                while (count < totalOps) {
                    if (deleteExisting) {
                        Transaction txn = env.beginTransaction(null, null);
                        Cursor cursor = db.openCursor(txn, null);
                        OperationStatus status =
                            cursor.getFirst(keyEntry, valueEntry, null);
                        if (status == OperationStatus.SUCCESS) {
                            cursor.delete();
                        }
                        cursor.close();
                        txn.commit();
                        if (status == OperationStatus.SUCCESS) {
                            count += 1;
                        } else {
                            System.out.println("No more records");
                            break;
                        }
                    } else {
                        if (serialKeys) {
                            int keyValue = getNextSerialKey();
                            IntegerBinding.intToEntry(keyValue, keyEntry);
                            System.arraycopy(keyEntry.getData(), 0, key, 0, 4);
                            keyEntry.setData(key);
                        } else {
                            rnd.nextBytes(key);
                            keyEntry.setData(key);
                        }
                        rnd.nextBytes(value);
                        valueEntry.setData(value);

                        db.put(null, keyEntry, valueEntry);
                        count++;
                        intervalCount++;

                        if (insertDups) {
                            for (int i = 0; i < 3; i += 1) {
                                rnd.nextBytes(value);
                                valueEntry.setData(value);
                                db.put(null, keyEntry, valueEntry);
                                count += 1;
                            }
                        }

                        if (doDelete) {
                            db.delete(null, keyEntry);
                        }
                    }

                    if (count % reportingInterval == 0) {
                        reportStats(id, intervalCount, count,
                                    intervalStart, start, db);
                        intervalCount = 0;
                        intervalStart = System.currentTimeMillis();
                    }
                }
                reportStats(id, intervalCount, count,
                            intervalStart, start, db);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized int getNextSerialKey() {
        return nextSerialKey++;
    }

    private void reportStats(int threadId,
                             int intervalCount,
                             int count,
                             long intervalStart,
                             long start,
                             Database db)
        throws DatabaseException {

        long end = System.currentTimeMillis();

        double seconds = (end - start)/1e3;
        double intervalSeconds = (end - intervalStart)/1e3;
        double rate = (double)(intervalCount/intervalSeconds);
        double totalRate = (double)(count/seconds);
        MemoryBudget mb = envImpl.getMemoryBudget();
        INList inList = envImpl.getInMemoryINs();
        INCompressor compressor = envImpl.getINCompressor();
        EnvironmentStats stats = env.getStats(statsConfig);
        System.out.println("id=" + threadId +
                           " " +  numberFormat.format(count) +
                           " rate=" +
                           decimalFormat.format(rate) +
                           " totalRate=" +
                           decimalFormat.format(totalRate) +
                           " cache=" +
                           numberFormat.format(mb.getCacheMemoryUsage()) +
                           " inList=" +
                           numberFormat.format(inList.getSize()) +
                           " passes=" +
                           stats.getNEvictPasses() +
                           " sel=" +
                           numberFormat.format(stats.getNNodesSelected()) +
                           " scan=" +
                           numberFormat.format(stats.getNNodesScanned()) +
                           " evict=" +
                           numberFormat.format(stats.getNNodesExplicitlyEvicted()) +
                           " strip=" +
                           numberFormat.format(stats.getNBINsStripped()) +
                           " ckpt=" +
                           stats.getNCheckpoints() +
                           " clean=" +
                           stats.getNCleanerRuns() +
                           " cleanBacklog=" +
                           stats.getCleanerBacklog() +
                           " compress=" +
                           compressor.getBinRefQueueSize() +
                           " notRes/cmiss=" +
                           stats.getNNotResident() + "/" +
                           stats.getNCacheMiss());
        totalEvictPasses += stats.getNEvictPasses();
        totalSelected += stats.getNNodesSelected();
        totalScanned += stats.getNNodesScanned();
        totalExEvicted += stats.getNNodesExplicitlyEvicted();
        totalStripped += stats.getNBINsStripped();
        totalCkpts += stats.getNCheckpoints();
        totalCleaned += stats.getNCleanerRuns();
        totalNotResident += stats.getNNotResident();
        totalCacheMiss += stats.getNCacheMiss();
        System.out.println("id=" + threadId +
                           " " +  numberFormat.format(count) +
                           " totals: " +
                           numberFormat.format(totalEvictPasses) +
                           " sel=" + numberFormat.format(totalSelected) +
                           " scan=" + numberFormat.format(totalScanned) +
                           " evict=" + numberFormat.format(totalExEvicted) +
                           " strip=" + numberFormat.format(totalStripped) +
                           " ckpt=" + numberFormat.format(totalCkpts) +
                           " clean=" + numberFormat.format(totalCleaned) +
                           " notRes=" + numberFormat.format(totalNotResident) +
                           " miss=" + numberFormat.format(totalCacheMiss));

        //summarizeINList(inList);
        //summarizeBtree(db);
        System.out.println("\n");
    }

    private void summarizeINList(INList inList)
        throws DatabaseException {

        int binCount = 0;
        int binBytes = 0;
        int inCount = 0;
        int inBytes = 0;
        
        Iterator iter = inList.iterator();

        while (iter.hasNext()) {
            IN theIN = (IN) iter.next();
            if (theIN instanceof BIN) {
                binCount++;
                //                    binBytes += theIN.computeMemorySize();
                BIN theBIN = (BIN) theIN;
                theBIN.evictLNs();
                binBytes += theIN.getBudgetedMemorySize();
                /*
                  for (int i = 0; i < theBIN.getNEntries(); i++) {
                  if (theBIN.getTarget(i) != null) {
                  lnCount++;
                  //            lnBytes += theBIN.getTarget(i).
                  //        getMemorySizeIncludedByParent();
                  }
                  }
                */
            } else if (theIN instanceof IN) {
                inCount++;
                inBytes += theIN.getBudgetedMemorySize();
            } else {
                System.out.println("non-IN, non-BIN found on INList");
            }
        }

        double perBIN = ((double)binBytes)/binCount;
        double perIN = ((double)inBytes)/inCount;

        System.out.println("INList: " +
                           " nBINs: " + numberFormat.format(binCount) +
                           " binBytes (incl LNBytes): " + binBytes +
                           " perBin: " + numberFormat.format(perBIN) +
                           " nINs: " + numberFormat.format(inCount) +
                           " inBytes: " + inBytes +
                           " perIN: " + numberFormat.format(perIN));
        //" nLNs: " + numberFormat.format(lnCount));
        //   " lnBytes (incl in binBytes): " + lnBytes);
    }

    private void summarizeBtree(Database db)
        throws DatabaseException {

        StatsConfig dbStatsConfig = new StatsConfig();
        dbStatsConfig.setFast(false);
        BtreeStats stats = (BtreeStats) db.getStats(null);
        System.out.print("BTreeStats: BINCount=" +
                         stats.getBottomInternalNodeCount() +
                         " INCount=" +
                         stats.getInternalNodeCount() +
                         " LNCount=" +
                         stats.getLeafNodeCount() +
                         " treeDepth=" +
                         stats.getMainTreeMaxDepth() +
                         " ");
        summarizeINsByLevel("IN", stats.getINsByLevel());
    }

    private void summarizeINsByLevel(String msg, long[] insByLevel) {
        if (insByLevel != null) {
            System.out.print(msg + " count by level: ");
            for (int i = 0; i < insByLevel.length; i++) {
                long cnt = insByLevel[i];
                if (cnt != 0) {
                    System.out.print("[" + i + "," + insByLevel[i] + "]");
                }
            }
            System.out.print("   ");
        }
    }
}
