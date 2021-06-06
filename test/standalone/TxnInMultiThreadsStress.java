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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;

/**
 * Transaction objects are advertised to work when used from multiple threads.
 * This stress test serves to confirm sucn concurrency.
 *
 * This stress test simulates such a scenario:
 * There are three threads. One is CRUD thread, which create a new txn object 
 * and processes CRUD operations within this txn. The other two threas are
 * commit thread and abort thread, which commits/aborts the same txn created by 
 * CRUD thread.
 * [#19513]
 */

public class TxnInMultiThreadsStress {
    private String homeDir = "tmp";
    private Environment env;
    private Database db;
    private Transaction txn;
    private BlockingQueue<CRUDOperation> CRUDOps;
    private int preInsertNum = 5000;
    private int NumOperations = 200000;
    private int numThreads = 1;
    private boolean commitThread = false;
    private boolean abortThread = false;
    private Random random = new Random();
    private int keySize = 3000;
    private List<OperationThread> threads = new ArrayList<OperationThread>();
    private List<DatabaseEntry> keyEntrys = new ArrayList<DatabaseEntry>();
    private List<Transaction> txnContainer = new ArrayList<Transaction>();
    private List<Exception> unExpectedExceptions = new ArrayList<Exception>();
    private Map<String, RuntimeException> exceptionCollection = 
        new HashMap<String, RuntimeException>();

    void openEnv() {  
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        //envConfig.setConfigParam
            //(EnvironmentParams.CLEANER_REMOVE.getName(), "false");
        envConfig.setConfigParam
            (EnvironmentParams.LOCK_TIMEOUT.getName(), "1000 ms");
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
        dbConfig.setSortedDuplicates(true);
        db = env.openDatabase(null, "testDB", dbConfig);
    }

    void closeEnv() {
        for (int i = 0; i < txnContainer.size(); i++) {
            Transaction txn = txnContainer.get(i);
            if (env.isValid()) {
                txn.abort();
            }
        }
        try {
            if (env.isValid()) {
                db.close();
            }
            env.close();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
        
    public static void main(String args[])
        throws Exception {
        //TxnInMultiThreadsStress app = new TxnInMultiThreadsStress();
        //app.runTest();
        
        try {
            new TxnInMultiThreadsStress().runTest(args);
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

    private void usage(String error) {

        if (error != null) {
            System.err.println(error);
        }
        System.err.println
            ("java " + getClass().getName() + '\n' +
             "      [-h <homeDir>] [-c <commit>] [-a <abort>] [-ops]\n" +
             "      [-keySize]\n");
        System.exit(1);
    }
    
    private void runTest(String args[]) 
        throws Exception {
        
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException();
            }
            /* Parse command-line input arguments. */
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                boolean moreArgs = i < args.length - 1;
                if (arg.equals("-c")) {
                    commitThread = true;
                    numThreads++;
                } else if (arg.equals("-a")) {
                    abortThread = true;
                    numThreads++;
                } else if (arg.equals("-h") && moreArgs) {
                    homeDir = args[++i];
                } else if (arg.equals("-ops") && moreArgs) {
                    NumOperations = Integer.parseInt(args[++i]);
                } else if (arg.equals("-keySize") && moreArgs) {
                    keySize = Integer.parseInt(args[++i]);
                } else {
                    usage("Unknown arg: " + arg);
                }
            }
            printArgs(args);
        } catch (IllegalArgumentException e) {
            usage("IllegalArguments! ");
            e.printStackTrace();
            System.exit(1);
        }
        
        openEnv();
        CRUDOps = createOperations(NumOperations);
        txn = env.beginTransaction(null, null);
        txnContainer.add(txn);
        CountDownLatch endSignal = new CountDownLatch(numThreads);
        //insertData(preInsertNum);
        createAndStartThreads(endSignal);
        closeEnv();
        if(unExpectedExceptions.isEmpty()) {
            System.out.println("Successful completion.");
        } else {
            for (int i = 0; i < unExpectedExceptions.size(); i++) {
                unExpectedExceptions.get(i).printStackTrace();
            }
            System.out.println("Test failed.");
            System.exit(1);
        }
    }

    /* Initialize the threads that will be used to run the tests. */
    private void createAndStartThreads(CountDownLatch endSignal) {
        OperationThread abort = null;
        OperationThread commit = null;
        if (abortThread) {
            abort= new AbortThread(endSignal);
            abort.setName("Abort_thread");
            threads.add(abort);
        } 
        if (commitThread) {
            commit= new CommitThread(endSignal);
            commit.setName("Commit_thread");
            threads.add(commit);
        }
        
        CRUDThread crud= new CRUDThread(endSignal);
        crud.setName("CRUD_thread");
        threads.add(crud);
        crud.start();
        
        if (abortThread) {
            abort.start();
        }

        if (commitThread) {
            commit.start();
        }
        
        try {
            endSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private void insertData(int num) {
        for (int i = 0; i < num; i++) {
            DatabaseEntry key = genNewKey();
            DatabaseEntry dataEntry = new DatabaseEntry(new byte[1]);
            Transaction txn = env.beginTransaction(null, null);
            db.put(txn, key, dataEntry);
            txn.commit();
        }
    }

    private ArrayBlockingQueue<CRUDOperation> createOperations(int NumOps) {
        ArrayBlockingQueue<CRUDOperation> ops = 
            new ArrayBlockingQueue<CRUDOperation>(NumOperations);
        CRUD.setDistribution(new int[] {40, 20, 20, 20});
        CRUD op;
        for (int i = 0; i < NumOps; i++) {
            op = CRUD.nextRandom();
            DatabaseEntry key;
            switch (op) {
            case CREATE:
                key = genNewKey();
                ops.add(new Create(db, key));
                break;
            case READ:
                key = genExistingKey();
                ops.add(new Read(db, key));
                break;
            case UPDATE:
                key = genExistingKey();
                ops.add(new Update(db, key));
                break;
            case DELETE:
                key =  genExistingKey();
                ops.add(new Delete(db, key));
                break;
            default:
                throw new IllegalStateException("Unknown op: " + op);
            }
        }
        return ops;
    }

    private DatabaseEntry genNewKey() {
        byte[] key = new byte[keySize];
        random.nextBytes(key);
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        keyEntrys.add(keyEntry);
        return keyEntry;
    }
    
    private DatabaseEntry genExistingKey() {
        DatabaseEntry keyEntry;
        if (keyEntrys.size() > 0) {
            int index = random.nextInt(keyEntrys.size());
            keyEntry = keyEntrys.get(index);
        } else {
            keyEntry = new DatabaseEntry(new byte[keySize]);
        }
        return keyEntry;
    }
    
    private DatabaseEntry genData(int length) {
        return new DatabaseEntry(new byte[1]);
    }
    
    enum CRUD {
        /* The operations */
        CREATE, READ, UPDATE, DELETE;

        /* The distribution of CRUD operations -- should add up to 100% */
        private int percent;

        /* The threshold values used to guide randomization, range from 0-100 */
        private int threshold;
 
        final static int size = CRUD.values().length;

        private static Random opRandom = new Random(1);

        /*
         * Sets the distribution that will be used for the random generation of
         * CRUD operations.
         */
        static void setDistribution(int distribution[]) {
            if (distribution.length != size) {
                throw new IllegalArgumentException
                    ("incorrect argument length: " + distribution.length);
            }

            int threshold = 0;
            int i = 0;
            for (CRUD op : CRUD.values()) {
                op.percent = distribution[i++];
                threshold = (op.threshold = op.percent + threshold);
            }

            if (threshold != 100) {
                throw new IllegalArgumentException
                    ("Percentage should add to 100 not " + threshold);
            }
        }

        /*
         * Returns the next random CRUD operation based on the current distribution
         * setup.
         */
        static CRUD nextRandom() {
            int rpercent = opRandom.nextInt(100);
            for (CRUD op : CRUD.values()) {
                if (rpercent < op.threshold) {
                    return op;
                }
            }
            assert (false);
            return null;
        }
    }

    abstract class CRUDOperation {
        protected final Database db;
        protected final DatabaseEntry key;
        protected Random random = new Random();
        
        public CRUDOperation(Database db, DatabaseEntry key) {
            this.db=db;
            this.key = key;
        }
        abstract OperationStatus execute(Transaction txn)
            throws DatabaseException;
        
        
    }
    
    /* Create */
    class Create extends CRUDOperation {
        Create(Database db, DatabaseEntry key) {
            super(db, key);
        }

        @Override
        OperationStatus execute(Transaction txn) throws DatabaseException {
            DatabaseEntry dataEntry = genData(1);
            if (txn.isValid()) {
                return db.putNoOverwrite(txn, key, dataEntry);
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
            DatabaseEntry dataEntry = genData(2);
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
    
    
    abstract class OperationThread extends Thread {
        Random random = new Random();
        CountDownLatch endSignal;
        
        public OperationThread(CountDownLatch endSignal) {
            this.endSignal = endSignal;
        }
        
        @Override
        public abstract void run();

    }
    
    class CRUDThread extends OperationThread {
        public CRUDThread(CountDownLatch endSignal) {
            super(endSignal);
        }
        
        
        @Override
        public void run() {
            boolean ifRun = true;
            try {
                while(!CRUDOps.isEmpty() && ifRun) {
                    try {
                        if (!env.isValid()) {
                            break;
                        }
                        if (!txn.isValid()) {
                            txn = env.beginTransaction(null, null); 
                            txnContainer.add(txn);
                        } 
                        CRUDOps.take().execute(txn);
                    } catch (IllegalStateException e) {
                        exceptionCollection.put
                            (e.getStackTrace()[0].toString() + 
                             e.getStackTrace().length, e);
                    } catch (IllegalArgumentException e) {
                        exceptionCollection.put
                        (e.getStackTrace()[0].toString() + 
                         e.getStackTrace().length, e);
                    } catch (EnvironmentFailureException e) {
                        exceptionCollection.put
                            (e.getStackTrace()[0].toString() + 
                             e.getStackTrace().length, e);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (RuntimeException e) {
                        unExpectedExceptions.add(e);
                        throw e;
                    }
                    if (endSignal.getCount() < 3) {
                        ifRun = false;
                    }
                } 
            }finally {
                this.endSignal.countDown();
            }
        }
    }
    
    class AbortThread extends OperationThread{
        public AbortThread(CountDownLatch endSignal) {
            super(endSignal);
        }
        
        @Override
        public void run() {
            try {
                boolean ifRun = true;
                while (ifRun) {
                    try {
                        int waitTime = random.nextInt(25) + 150;
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (!env.isValid()) {
                        break;
                    }
                    try {
                        txn.abort();
                    } catch (IllegalStateException e) {
                        exceptionCollection.put
                            (e.getStackTrace()[0].toString() + 
                             e.getStackTrace().length, e);
                    } catch (EnvironmentFailureException e) {
                        exceptionCollection.put
                            (e.getStackTrace()[0].toString() + 
                             e.getStackTrace().length, e);
                    } catch (RuntimeException e) {
                        unExpectedExceptions.add(e);
                        throw e;
                    }
                    if (endSignal.getCount() < 3) {
                        ifRun = false;
                    }
                }
            } finally {
                this.endSignal.countDown();
            }
        }
    }
    
    class CommitThread extends OperationThread{
        public CommitThread(CountDownLatch endSignal) {
            super(endSignal);
        }

        @Override
        public void run() {
            try {
                boolean ifRun = true;
                while (ifRun) {
                    try {
                        int waitTime = random.nextInt(5) + 5;
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (!env.isValid()) {
                        break;
                    }
                    if (txn.isValid()) {
                        try {
                            txn.commit();
                        } catch (IllegalStateException e) {
                            exceptionCollection.put
                                (e.getStackTrace()[0].toString() + 
                                 e.getStackTrace().length, e);
                            
                        } catch (EnvironmentFailureException e) {
                            exceptionCollection.put
                                (e.getStackTrace()[0].toString() + 
                                 e.getStackTrace().length, e);
                            
                        } catch (RuntimeException e) {
                            unExpectedExceptions.add(e);
                            throw e;
                        }
                    }
                    if (endSignal.getCount() < 3) {
                        ifRun = false;
                    }
                }
            } finally {
                this.endSignal.countDown();
            }
        }
    }
}
