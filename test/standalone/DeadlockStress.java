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
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;

/**
 * Application to simulate different deadlock scenarios.
 * 
 * The simple scenario:
 *   Two threads access two records in opposite order with their own txns.
 */
public class DeadlockStress {
    
    private String homeDir = "./tmp";
    private Environment env = null;
    private Database db;
    private int dbSize = 100;
    private int totalTxns = 1000;
    private int factor = 100;
    private int maxRetry = 100;
    /* The number of operations in each Txn in mix access mode. */
    private int opNum = 5;
    /* 
     * The number of threads used in mix access mode. It should be divided
     * by dbSize. 
     */
    private int threadNum = 20;
    
    /* The run time for mix access mode. */
    private long runtime = 5 * 60 * 1000; // 10minutes
    
    boolean verbose = true;
    
    private CountDownLatch startSignal;
    
    private boolean deadlockDone = false;
    
    
    void openEnv() {  
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        /*
        envConfig.setConfigParam
            (EnvironmentParams.LOCK_TIMEOUT.getName(), "1000 ms");
        */
        try {
            File envHome = new File(homeDir);
            env = new Environment(envHome, envConfig);
        } catch (Error e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }
    
    void closeEnv() {
        try {
            
            if (db != null) {
                db.close();
            }
            
            if (env != null) {
                env.close();
            }            
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public static void main(String args[]){        
        try {
            DeadlockStress test = new DeadlockStress();
            test.parseArgs(args);
            test.run();
            System.exit(0);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
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
    
    protected void parseArgs(String args[]) 
        throws Exception {

        for (int i = 0; i < args.length; i++) {
            boolean moreArgs = i < args.length - 1;
            if (args[i].equals("-h") && moreArgs) {
                homeDir = args[++i];
            } else if (args[i].equals("-dbSize") && moreArgs) {
                dbSize = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-totalTxns") && moreArgs) {
                totalTxns = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-retry") && moreArgs) {
                maxRetry = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-opnum") && moreArgs) {
                opNum = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-threads") && moreArgs) {
                threadNum = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-time") && moreArgs) {
                runtime = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-verbose") && moreArgs ) {
                verbose = Boolean.parseBoolean(args[++i]);
            } else {
                usage("Error: Unknown arg: " + args[i]);
            }
        }
        
        printArgs(args);
    }
    

    private void usage(String error) {

        if (error != null) {
            System.err.println(error);
        }
        System.err.println
            ("java " + getClass().getName() + '\n' +
             "      [-h <homeDir>] [-dbsize] [-totalTxns]\n");
        System.exit(1);
    }
                                
    public void run() 
       throws Exception {
        openEnv();
        insertRecords();
        compareExceptionMessFoDebug();
        doTwoThreadsDeadlock();
        doTwoThreadsNoInteraction();
        doTwoThreadsPartInteraction();
        doThreeThreadsDeadlock();
        doThreeThreadsNoInteraction();
        doThreeThreadsPartInteraction();
        doDeadlockOnOneRecord();
        noDeadlockOnOneRecord();
        doDeadlockOneCommonLocker();
        doDeadlockTwoCommonLockers();
        doMixedOperationWithDeadlock();
        doMixedOperationSortedToNoDeadlock();
        doMixedOperationNoInteraction();
        doMixedOperationWithDeadlockSecondary();
        doMixedOperationSortedToNoDeadlockSecondary();
        doMixedOperationNoInteractionSecondary();
        closeEnv();
    }
    
    private void insertRecords() 
        throws Exception, InterruptedException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        for (int i = 0; i < dbSize; i++) {
            IntegerBinding.intToEntry(i, key);
            IntegerBinding.intToEntry(i, data);
            db.put(null, key, data);
        }
    }
    
    public void compareExceptionMessFoDebug()
        throws InterruptedException {
        System.out.println("Compare Exception content");     
        startSignal = new CountDownLatch(1);
        
        AccessThreadBreakWhenDeadlock thread1 =
            new AccessThreadBreakWhenDeadlock(1,1,2,-1,false);
        AccessThreadBreakWhenDeadlock thread2 =
            new AccessThreadBreakWhenDeadlock(2,2,1,-1,false);

        thread1.start();
        thread2.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
    }
    
    public void doTwoThreadsDeadlock()
        throws InterruptedException {
        System.out.println("Deadlock between two threads");     
        startSignal = new CountDownLatch(1);
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,false);
        AccessThread thread2 = new AccessThread(2,2,1,-1,false);
        thread1.start();
        thread2.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
    }
    
    public void doTwoThreadsNoInteraction()
        throws InterruptedException {
        System.out.println("Two threads do not have any interaction");     
        startSignal = new CountDownLatch(1);
        totalTxns = factor * totalTxns;
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,false);
        AccessThread thread2 = new AccessThread(2,3,4,-1,false);
        thread1.start();
        thread2.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join();
        totalTxns = totalTxns / factor;
    }
    
    public void doTwoThreadsPartInteraction()
        throws InterruptedException {
        System.out.println("Two threads have part interaction");     
        startSignal = new CountDownLatch(1);
        totalTxns = factor * totalTxns;
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,false);
        AccessThread thread2 = new AccessThread(2,3,1,-1,false);
        thread1.start();
        thread2.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join();
        totalTxns = totalTxns / factor;
    }
    
    public void doThreeThreadsDeadlock()
        throws InterruptedException {
        
        System.out.println("Deadlock between three threads");  
        startSignal = new CountDownLatch(1);
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,false);
        AccessThread thread2 = new AccessThread(2,2,3,-1,false);
        AccessThread thread3 = new AccessThread(3,3,1,-1,false);
        thread1.start();
        thread2.start();
        thread3.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
        thread3.join();
    }
    
    public void doThreeThreadsNoInteraction()
        throws InterruptedException {
        
        System.out.println("Three threads do not have any interaction");  
        startSignal = new CountDownLatch(1);
        totalTxns = factor * totalTxns;
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,false);
        AccessThread thread2 = new AccessThread(2,3,4,-1,false);
        AccessThread thread3 = new AccessThread(3,5,6,-1,false);
        thread1.start();
        thread2.start();
        thread3.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
        thread3.join();
        totalTxns = totalTxns / factor;
    }
    
    public void doThreeThreadsPartInteraction()
        throws InterruptedException {
        
        System.out.println("Three threads have part interaction");  
        startSignal = new CountDownLatch(1);
        totalTxns = factor * totalTxns;
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,false);
        AccessThread thread2 = new AccessThread(2,1,3,-1,false);
        AccessThread thread3 = new AccessThread(3,4,1,-1,false);
        thread1.start();
        thread2.start();
        thread3.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
        thread3.join();
        totalTxns = totalTxns / factor;
    }
    
    public void doDeadlockOnOneRecord()
        throws InterruptedException {
        System.out.println("Deadlock formed on one record");     
        startSignal = new CountDownLatch(1);
        
        AccessThread thread1 = new AccessThread(1,1,1,-1,true);
        AccessThread thread2 = new AccessThread(2,1,1,-1,true);
        thread1.start();
        thread2.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
    }
    
    public void noDeadlockOnOneRecord()
        throws InterruptedException {
        System.out.println("No Deadlock formed on one record");     
        startSignal = new CountDownLatch(1);
        totalTxns = factor * totalTxns;
        
        AccessThread thread1 = new AccessThread(1,1,1,-1,false);
        AccessThread thread2 = new AccessThread(2,1,1,-1,false);
        thread1.start();
        thread2.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
        totalTxns = totalTxns / factor;
    }
    
    public void doDeadlockOneCommonLocker()
        throws InterruptedException {
        
        System.out.println("Deadlock with one common locker");  
        startSignal = new CountDownLatch(1);
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,true);
        AccessThread thread2 = new AccessThread(2,3,2,1,false);
        AccessThread thread3 = new AccessThread(3,1,3,-1,true);
        thread1.start();
        thread2.start();
        thread3.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
        thread3.join();
    }
    
    public void doDeadlockTwoCommonLockers()
        throws InterruptedException {
        
        System.out.println("Deadlock with two common lockers");  
        startSignal = new CountDownLatch(1);
        
        AccessThread thread1 = new AccessThread(1,1,2,-1,true);
        AccessThread thread2 = new AccessThread(2,4,2,3,false);
        AccessThread thread3 = new AccessThread(3,3,1,-1,false);
        AccessThread thread4 = new AccessThread(4,1,4,-1,true);
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        
        startSignal.countDown();
        
        thread1.join();
        thread2.join(); 
        thread3.join();
        thread4.join();
    }

    public void doMixedOperationWithDeadlock() {
        System.out.println("Mix access mode with possible Deadlock");
        int[] distribution = new int[] {25, 25, 25, 25};
        MixedAccessThread[] mixedThreads = new MixedAccessThread[threadNum];

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i] =
                new MixedAccessThread(i, distribution, false, false);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].start();
        }

        try {
            Thread.sleep(runtime);
        } catch (InterruptedException e) {
            
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].setDone(true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            try {
                mixedThreads[i].join();
            } catch (InterruptedException e) {

            }
        }
    }
    
    public void doMixedOperationSortedToNoDeadlock() {
        System.out.println("Mix access mode sorted to no deadlock");
        int[] distribution = new int[] {25, 25, 25, 25};
        MixedAccessThread[] mixedThreads = new MixedAccessThread[threadNum];

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i] =
                new MixedAccessThread(i, distribution, true, false);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].start();
        }

        try {
            Thread.sleep(runtime);
        } catch (InterruptedException e) {
            
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].setDone(true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            try {
                mixedThreads[i].join();
            } catch (InterruptedException e) {

            }
        }
    }
    
    public void doMixedOperationNoInteraction() {
        System.out.println("Mix access mode no interaction to no deadlock");
        int[] distribution = new int[] {25, 25, 25, 25};
        MixedAccessThread[] mixedThreads = new MixedAccessThread[threadNum];

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i] =
                new MixedAccessThread(i, distribution, false, true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].start();
        }

        try {
            Thread.sleep(runtime);
        } catch (InterruptedException e) {
            
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].setDone(true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            try {
                mixedThreads[i].join();
            } catch (InterruptedException e) {

            }
        }
    }
    
    public void doMixedOperationWithDeadlockSecondary() {
        System.out.println("Mix access mode with possible Deadlock Secondary");
        int[] distribution = new int[] {25, 25, 25, 25};
        SecondaryAccessThread[] mixedThreads =
            new SecondaryAccessThread[threadNum];
        
        SecondaryDatabase sdb =
            openSecondary(env, db, "secDb", new SecondaryConfig());
        
        boolean secondary;
        Database usedDb;

        for (int i = 0; i < mixedThreads.length; i++) {
            if ( i % 2 == 0) {
                secondary = false;
                usedDb = db;
            } else {
                secondary = true;
                usedDb = sdb;
            }
            mixedThreads[i] = new SecondaryAccessThread(
                i, distribution, false, false, secondary, usedDb);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].start();
        }

        try {
            Thread.sleep(runtime);
        } catch (InterruptedException e) {
            
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].setDone(true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            try {
                mixedThreads[i].join();
            } catch (InterruptedException e) {

            }
        }
        
        if (sdb != null) {
            sdb.close();
        }
    }
    
    public void doMixedOperationSortedToNoDeadlockSecondary() {
        System.out.println("Mix access mode sorted to no deadlock Secondary");
        int[] distribution = new int[] {25, 25, 25, 25};
        SecondaryAccessThread[] mixedThreads =
            new SecondaryAccessThread[threadNum];
        
        SecondaryDatabase sdb =
            openSecondary(env, db, "secDb", new SecondaryConfig());
        
        boolean secondary;
        Database usedDb;

        for (int i = 0; i < mixedThreads.length; i++) {
            if ( i % 2 == 0) {
                secondary = false;
                usedDb = db;
            } else {
                secondary = true;
                usedDb = sdb;
            }
            mixedThreads[i] = new SecondaryAccessThread(
                i, distribution, true, false, secondary, usedDb);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].start();
        }

        try {
            Thread.sleep(runtime);
        } catch (InterruptedException e) {
            
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].setDone(true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            try {
                mixedThreads[i].join();
            } catch (InterruptedException e) {

            }
        }
        
        if (sdb != null) {
            sdb.close();
        }
    }
    
    public void doMixedOperationNoInteractionSecondary() {
        System.out.println("Mix access mode no interaction Secondary");
        int[] distribution = new int[] {25, 25, 25, 25};
        SecondaryAccessThread[] mixedThreads =
            new SecondaryAccessThread[threadNum];
        
        SecondaryDatabase sdb =
            openSecondary(env, db, "secDb", new SecondaryConfig());
        
        boolean secondary;
        Database usedDb;

        for (int i = 0; i < mixedThreads.length; i++) {
            if ( i % 2 == 0) {
                secondary = false;
                usedDb = db;
            } else {
                secondary = true;
                usedDb = sdb;
            }
            mixedThreads[i] = new SecondaryAccessThread(
                i, distribution, false, true, secondary, usedDb);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].start();
        }

        try {
            Thread.sleep(runtime);
        } catch (InterruptedException e) {
            
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            mixedThreads[i].setDone(true);
        }

        for (int i = 0; i < mixedThreads.length; i++) {
            try {
                mixedThreads[i].join();
            } catch (InterruptedException e) {

            }
        }
        
        if (sdb != null) {
            sdb.close();
        }
    }

    private SecondaryDatabase openSecondary(
        Environment env,
        Database priDb,
        String dbName,
        SecondaryConfig dbConfig) {
        
        dbConfig.setAllowPopulate(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setKeyCreator(new MyKeyCreator());
        return env.openSecondaryDatabase(null, dbName,
                                         priDb, dbConfig);
    }
    
    class SecondaryAccessThread extends Thread {
        boolean done = false;
        
        private int id;
        private CRUDGenerator cg;
        private int opsNumEachThread = dbSize / threadNum;
        private boolean secondary;
        private Database usedDb;
        
        /*
         * The records involved in each Txn of different threads may contain
         * the same record(s), but in order to avoid deadlock, we sort these
         * records by their int key and at the same time, guarantee that in
         * each txn, the int keys of records are different. 
         */
        private boolean sort = false; 
        
        /*
         * In order to avoid deadlock, the records involved in each Txn of
         * different threads do not have intersection.
         */
        private boolean noInteraction = false;
        
        SecondaryAccessThread(int id,
                              int[] distribution,
                              boolean sort,
                              boolean noInteraction,
                              boolean secondary,
                              Database usedDb) {
            this.id = id;
            this.sort = sort;
            this.noInteraction = noInteraction;
            this.secondary = secondary;
            this.usedDb = usedDb;
            cg = new CRUDGenerator(id, distribution);
        }
        
        public void run() {
            long startTime = System.currentTimeMillis();
            long count = 0;
            while (!done) {
                doOneTxnWithRetry();
                count++;
            }
            
            long endTime = System.currentTimeMillis();
            float elapsedSec = (float) ((endTime - startTime) / 1e3);
            float throughput = ((float) count) / elapsedSec;
            System.out.println
                ("Thread " + id + " finishes " + count +
                 " iterations in: " + elapsedSec +
                 " sec, average throughput: " + throughput + " op/sec.");
        }
        
        @SuppressWarnings("unchecked")
        public void doOneTxnWithRetry() {
            ArrayList<CursorOperation> ops = new ArrayList<>();
            ArrayList<Integer> keyInts = new ArrayList<>();
 
            for (int i = 0; i < opNum; i++) {
                int keyInt = generateKeyInt(keyInts);
                DatabaseEntry key = new DatabaseEntry();
                IntegerBinding.intToEntry(keyInt, key);
                
                CRUDTYPE op = cg.nextRandomCRUD();
                switch (op) {
                    case CREATE:
                        ops.add(new CursorCreate(key, secondary));
                        break;
                    case READ:
                        ops.add(new CursorRead(key, secondary));
                        break;
                    case UPDATE:
                        ops.add(new CursorUpdate(key, secondary));
                        break;
                    case DELETE:
                        ops.add(new CursorDelete(key, secondary));
                        break;
                    default:
                        throw new IllegalStateException("Unknown op: " + op);
                }
            }
            
            if (sort) {
                final CursorOperation[] coArray =
                    ops.toArray(new CursorOperation[0]);
                final CursorComparator cc = new CursorComparator();
                Arrays.sort(coArray, cc);
                ops.clear();
                ops.addAll(Arrays.asList(coArray));
                
            }

            int tries = 0;
            while (tries < maxRetry) {
                Transaction txn = env.beginTransaction(null, null);
                Cursor c = usedDb.openCursor(txn, null);
                try {
                    for (CursorOperation cursorOp: ops) {
                        cursorOp.execute(txn, c);
                    }

                    if (c != null) {
                        c.close();
                    }

                    txn.commit();
                    break;
                } catch (LockConflictException e) {
                    if (c != null) {
                        c.close();
                    }
                    txn.abort();
                    tries++;
                } catch (OperationFailureException ofe) {
                    ofe.printStackTrace();
                }
 
            }
            //if (tries == maxRetry) {
            //if (tries > 0) {
            //    System.out.println("Thread: " + id + " Retry times: " + tries);
            //}
        }
        
        private int generateKeyInt(ArrayList<Integer> keyInts) {
            if (sort) {
                while (true) {
                    boolean repeated = false;
                    int tmp = cg.nextRandomKeyInt(dbSize);
                    for (Integer I : keyInts) {
                        if (I.intValue() ==  tmp) {
                            repeated = true;
                            break;
                        }
                    }
                    
                    if (!repeated) {
                        keyInts.add(new Integer(tmp));
                        return tmp;
                    }
                }
            } else if (noInteraction) {
                int tmp = cg.nextRandomKeyInt(opsNumEachThread);
                return tmp + opsNumEachThread * id;
            } else {
                return cg.nextRandomKeyInt(dbSize);
            }
        }
        
        public synchronized void setDone(boolean done) {
            this.done = done;
        }
    } 
    
    class MyKeyCreator implements SecondaryKeyCreator {
        @Override
        public boolean createSecondaryKey(SecondaryDatabase secondary,
                DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
            result.setData(key.getData());

            return true;
        }
    }
    
    abstract class CursorOperation {
        protected final DatabaseEntry key;
        protected final boolean secondary;
        
        public CursorOperation(DatabaseEntry key,
                               boolean secondary) {
            this.key = key;
            this.secondary = secondary;
        }

        abstract OperationStatus execute(Transaction txn, Cursor c)
            throws DatabaseException;
        
        public int getKeyInt() {
            return IntegerBinding.entryToInt(key);
        }
    }
    
    /*
     * Create.
     * 
     * For secondary database, we can not create record. So we actually do
     * read actions with different search mode: getSearchBothRange.
     */
    class CursorCreate extends CursorOperation {
        CursorCreate(DatabaseEntry key, boolean secondary) {
            super(key, secondary);
        }

        @Override
        OperationStatus execute(Transaction txn, Cursor c)
            throws DatabaseException {
            if (txn.isValid()) {
                if (secondary) {
                     return ((SecondaryCursor)c).getSearchBothRange(
                         key, key, new DatabaseEntry(), null);
                } else {
                    return c.put(key, new DatabaseEntry(new byte[10]));
                }
            } else {
                return null;
            }
        }
    }
    
    /* Read */
    class CursorRead extends CursorOperation {
        CursorRead(DatabaseEntry key, boolean secondary) {
            super(key, secondary);
        }

        @Override
        OperationStatus execute(Transaction txn, Cursor c)
            throws DatabaseException {
            if (txn.isValid()) {
                if (secondary) {
                    return ((SecondaryCursor)c).getSearchKey(
                        key, new DatabaseEntry(), new DatabaseEntry(), null);
               } else {
                   return c.getSearchKey(key, new DatabaseEntry(), null);
               }
               
            } else {
                return null;
            }
        }
    }
    
    /*
     * Update.
     * 
     * For secondary database, we can not create record. So we actually do
     * read actions with different search mode: getSearchKeyRange.
     */
    class CursorUpdate extends CursorOperation {
        CursorUpdate(DatabaseEntry key, boolean secondary) {
            super(key, secondary);
        }

        @Override
        OperationStatus execute(Transaction txn, Cursor c)
            throws DatabaseException {
            if (txn.isValid()) {
                if (secondary) {
                    return ((SecondaryCursor)c).getSearchKeyRange(
                        key, new DatabaseEntry(), new DatabaseEntry(), null);
               } else {
                   return c.getSearchKey(key, new DatabaseEntry(), null);
               }
            } else {
                return null;
            }
        }
    }

    /* Delete */
    class CursorDelete extends CursorOperation {
        CursorDelete(DatabaseEntry key, boolean secondary) {
            super(key, secondary);
        }

        @Override
        OperationStatus execute(Transaction txn, Cursor c)
            throws DatabaseException {
            if (txn.isValid()) {
                if (secondary) {
                    if (((SecondaryCursor)c).getSearchKey(
                        key, new DatabaseEntry(), new DatabaseEntry(), null) ==
                        OperationStatus.SUCCESS) {
                        return ((SecondaryCursor)c).delete();
                    }
               } else {
                   if (c.getSearchKey(key, new DatabaseEntry(), null) ==
                       OperationStatus.SUCCESS) {
                       return c.delete();    
                   }
               }
            } else {
                return null;
            }
            return null;
        }
    }
    
    class CursorComparator implements Comparator {
        @Override
        public int compare (Object obj1, Object obj2) {
            final CursorOperation cop1 = (CursorOperation) obj1;
            final CursorOperation cop2 = (CursorOperation) obj2;
            return (cop1.getKeyInt() - cop2.getKeyInt());
        }
    }
    
    class MixedAccessThread extends Thread {
        boolean done = false;
        
        private int id;
        private CRUDGenerator cg;
        private int opsNumEachThread = dbSize / threadNum;
        
        /*
         * The records involved in each Txn of different threads may contain
         * the same record(s), but in order to avoid deadlock, we sort these
         * records by their int key and at the same time, guarantee that in
         * each txn, the int keys of records are different. 
         */
        private boolean sort = false; 
        
        /*
         * In order to avoid deadlock, the records involved in each Txn of
         * different threads do not have intersection.
         */
        private boolean noInteraction = false;
        
        MixedAccessThread(int id,
                          int[] distribution,
                          boolean sort,
                          boolean noInteraction) {
            this.id = id;
            this.sort = sort;
            this.noInteraction = noInteraction;
            cg = new CRUDGenerator(id, distribution);
        }
        
        public void run() {
            long startTime = System.currentTimeMillis();
            long count = 0;
            while (!done) {
                doOneTxnWithRetry();
                count++;
            }
            
            long endTime = System.currentTimeMillis();
            float elapsedSec = (float) ((endTime - startTime) / 1e3);
            float throughput = ((float) count) / elapsedSec;
            System.out.println
                ("Thread " + id + " finishes " + count +
                 " iterations in: " + elapsedSec +
                 " sec, average throughput: " + throughput + " op/sec.");
        }
        
        @SuppressWarnings("unchecked")
        public void doOneTxnWithRetry() {
            ArrayList<CRUDOperation> ops = new ArrayList<>();
            ArrayList<Integer> keyInts = new ArrayList<>();
            
            for (int i = 0; i < opNum; i++) {
                int keyInt = generateKeyInt(keyInts);
                DatabaseEntry key = new DatabaseEntry();
                IntegerBinding.intToEntry(keyInt, key);
                
                CRUDTYPE op = cg.nextRandomCRUD();
                switch (op) {
                    case CREATE:
                        ops.add(new Create(db, key));
                        break;
                    case READ:
                        ops.add(new Read(db, key));
                        break;
                    case UPDATE:
                        ops.add(new Update(db, key));
                        break;
                    case DELETE:
                        ops.add(new Delete(db, key));
                        break;
                    default:
                        throw new IllegalStateException("Unknown op: " + op);
                }
            }
            
            if (sort) {
                final CRUDOperation[] coArray =
                    ops.toArray(new CRUDOperation[0]);
                final OpsComparator oc = new OpsComparator();
                Arrays.sort(coArray, oc);
                ops.clear();
                ops.addAll(Arrays.asList(coArray));
                
            }
            
            int tries = 0;
            while (tries < maxRetry) {
                Transaction txn = env.beginTransaction(null, null);
                try {
                    for (CRUDOperation crudOp: ops) {
                        crudOp.execute(txn);
                    }
                    txn.commit();
                    break;
                } catch (LockConflictException e) {
                    txn.abort();
                    tries++;
                }
 
            }
            //if (tries == maxRetry) {
            //if (tries > 0) {
            //    System.out.println("Thread: " + id + " Retry times: " + tries);
            //}
        }
        
        private int generateKeyInt(ArrayList<Integer> keyInts) {
            if (sort) {
                while (true) {
                    boolean repeated = false;
                    int tmp = cg.nextRandomKeyInt(dbSize);
                    for (Integer I : keyInts) {
                        if (I.intValue() ==  tmp) {
                            repeated = true;
                            break;
                        }
                    }
                    
                    if (!repeated) {
                        keyInts.add(new Integer(tmp));
                        return tmp;
                    }
                }
            } else if (noInteraction) {
                int tmp = cg.nextRandomKeyInt(opsNumEachThread);
                return tmp + opsNumEachThread * id;
            } else {
                return cg.nextRandomKeyInt(dbSize);
            }
        }
        
        public synchronized void setDone(boolean done) {
            this.done = done;
        }
    } 
    
    class OpsComparator implements Comparator {
        @Override
        public int compare (Object obj1, Object obj2) {
            final CRUDOperation op1 = (CRUDOperation) obj1;
            final CRUDOperation op2 = (CRUDOperation) obj2;
            return (op1.getKeyInt() - op2.getKeyInt());
        }
    }
    
    /* The type of possible operations. */
    enum CRUDTYPE {
        CREATE,
        READ,
        UPDATE,
        DELETE;
    }
    
    class CRUDGenerator {
        
        int[] distribution;
        int id;
        Random opRandom;
        
        CRUDGenerator(int id, int distribution[]) {
            this.id = id;
            this.distribution = distribution;
            opRandom = new Random(System.currentTimeMillis() * id);
            
            int total = 0;
            for (int i = 0; i < distribution.length; i++) {
                total += distribution[i];
            }
            
            if (total != 100) {
                throw new IllegalArgumentException(
                    "Distribution should add to 100 not " + total);
            }
        }

        /*
         * Returns the next random CRUDTYPE based on the current
         * distribution setup.
         */
        public CRUDTYPE nextRandomCRUD() {
            int rpercent = opRandom.nextInt(100);
            int total = 0;
            for (int i = 0; i < distribution.length; i++) {
                total += distribution[i];
                if (rpercent < total) {
                    switch (i) {
                    case 0:
                        return CRUDTYPE.CREATE;
                    case 1:
                        return CRUDTYPE.READ;
                    case 2:
                        return CRUDTYPE.UPDATE;
                    case 3:
                        return CRUDTYPE.DELETE;
                    }
                }
            } 
            throw new IllegalArgumentException("Something is wrong");
        }
        
        public int nextRandomKeyInt(int range) {
            return opRandom.nextInt(range);
        }
    }
 
    abstract class CRUDOperation {
        protected final Database db;
        protected final DatabaseEntry key;
        
        public CRUDOperation(Database db, DatabaseEntry key) {
            this.db=db;
            this.key = key;
            
        }

        abstract OperationStatus execute(Transaction txn)
            throws DatabaseException;
        
        public int getKeyInt() {
            return IntegerBinding.entryToInt(key);
        }
    }
    
    /* Create */
    class Create extends CRUDOperation {
        Create(Database db, DatabaseEntry key) {
            super(db, key);
        }

        @Override
        OperationStatus execute(Transaction txn) throws DatabaseException {
            DatabaseEntry dataEntry = new DatabaseEntry(new byte[10]);
            if (txn.isValid()) {
                return db.put(txn, key, dataEntry);
            } else {
                return null;
            }
        }
    }
    
    /* Read */
    class Read extends CRUDOperation {
        Read(Database db, DatabaseEntry key) {
            super(db, key);
        }

        @Override
        OperationStatus execute(Transaction txn) throws DatabaseException {
            DatabaseEntry dataEntry = new DatabaseEntry();
            if (txn.isValid()) {
                return db.get(txn, key, dataEntry, null);
            } else {
                return null;
            }
        }
    }
    
    /* Update */
    class Update extends CRUDOperation {
        Update(Database db, DatabaseEntry key) {
            super(db, key);
        }

        @Override
        OperationStatus execute(Transaction txn) throws DatabaseException {
            DatabaseEntry dataEntry = new DatabaseEntry(new byte[10]);
            if (txn.isValid()) {
                return db.put(txn, key, dataEntry);
            } else {
                return null;
            }
        }
    }

    /* Delete */
    class Delete extends CRUDOperation {
        Delete(Database db, DatabaseEntry key) {
            super(db, key);
        }

        @Override
        OperationStatus execute(Transaction txn) throws DatabaseException {
            if (txn.isValid()) {
                return db.delete(txn, key);
            } else {
                return null;
            }
        }
    }
    
    class AccessThread extends Thread {
        /** The identifier of the current thread. */
        private int id;
        
        private int key1;
        private int key2;
        private int key3;
        
        /*
         * Determine whether do the read access or write access.
         * 
         * 1. When deadlock is formed on one record, i.e. two threads first
         * read and then write. We need to let the first operation be
         * read access.
         * 
         * 2. When two deadlock cycles involove the same locker, two threads
         * need to own the lock on one same record, so now we also need to
         * do the read request to let two threads own the read lock on the
         * same record.
         */
        boolean firstRead;

        public AccessThread(
            int id,
            int key1,
            int key2,
            int key3,
            boolean firstRead) {

            this.id = id;
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = key3;
            this.firstRead = firstRead;
        }

        /**
         * This thread is responsible for executing transactions.
         */
        public void run() {
            try {
                startSignal.await();
                long startTime = System.currentTimeMillis();
                for (int op = 0; op < totalTxns; op++) {
                    int tries = 0;
                    while (tries < maxRetry) {
                        Transaction txn = env.beginTransaction(null, null);
                        try {   
                            DatabaseEntry key = new DatabaseEntry();
                            DatabaseEntry data = new DatabaseEntry();
                            IntegerBinding.intToEntry(key1, key);   
                            if (firstRead) {
                                db.get(txn, key, data, LockMode.DEFAULT);
                            } else {
                                IntegerBinding.intToEntry(key1, data);
                                db.put(txn, key, data);
                            }
                            
                            
                            IntegerBinding.intToEntry(key2, key);
                            IntegerBinding.intToEntry(key2, data); 
                            db.put(txn, key, data);
                            
                            if (key3 > 0) {
                                IntegerBinding.intToEntry(key3, key);
                                IntegerBinding.intToEntry(key3, data); 
                                db.put(txn, key, data);
                            }
                            
                            txn.commit();
                            break;
                        } catch (LockConflictException e) {
                            txn.abort();
                            tries++;
                        }
                    }
                    /*
                    if (verbose && tries > 0) {
                        System.out.println(
                            "Thread: " + id + " Retry times: " + tries);
                    }
                    */
                }
                long endTime = System.currentTimeMillis();
                float elapsedSec = (float) ((endTime - startTime) / 1e3);
                float throughput = ((float) totalTxns) / elapsedSec;
                System.out.println
                    ("Thread " + id + " finishes " + totalTxns +
                     " iterations in: " + elapsedSec +
                     " sec, average throughput: " + throughput + " op/sec.");
            } catch  (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    class AccessThreadBreakWhenDeadlock extends Thread {
        
        /** The identifier of the current thread. */
        private int id;
        
        private int key1;
        private int key2;
        private int key3;
        
        /*
         * Determine whether do the read access or write access.
         * 
         * 1. When deadlock is formed on one record, i.e. two threads first
         * read and then write. We need to let the first operation be
         * read access.
         * 
         * 2. When two deadlock cycles involove the same locker, two threads
         * need to own the lock on one same record, so now we also need to
         * do the read request to let two threads own the read lock on the
         * same record.
         */
        boolean firstRead;

        public AccessThreadBreakWhenDeadlock(
            int id,
            int key1,
            int key2,
            int key3,
            boolean firstRead) {

            this.id = id;
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = key3;
            this.firstRead = firstRead;
        }

        /**
         * This thread is responsible for executing transactions.
         */
        public void run() {
            try {
                startSignal.await();
                for (int op = 0; op < totalTxns && !deadlockDone; op++) {
                    Transaction txn = env.beginTransaction(null, null);
                    try {   
                        DatabaseEntry key = new DatabaseEntry();
                        DatabaseEntry data = new DatabaseEntry();
                        IntegerBinding.intToEntry(key1, key);   
                        if (firstRead) {
                            db.get(txn, key, data, LockMode.DEFAULT);
                        } else {
                            IntegerBinding.intToEntry(key1, data);
                            db.put(txn, key, data);
                        }
                        
                        
                        IntegerBinding.intToEntry(key2, key);
                        IntegerBinding.intToEntry(key2, data); 
                        db.put(txn, key, data);
                        
                        if (key3 > 0) {
                            IntegerBinding.intToEntry(key3, key);
                            IntegerBinding.intToEntry(key3, data); 
                            db.put(txn, key, data);
                        }

                        txn.commit();
                    } catch (LockConflictException e) {
                        System.out.println(e.getMessage());
                        deadlockDone = true;
                        txn.abort();
                    }
                }
            } catch  (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
