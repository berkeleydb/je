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

package com.sleepycat.je.tree;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.util.DualTestCase;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.util.test.SharedTestUtils;

/*********************************************************************
  Exercise a race condition in split processing. The case requires a
  at least 3 level btree where the root has maxEntries-1 children.
  i.e suppose node max = 4. Our test case will start with data like this:

                        RootIN
                 +--------+----------+
                 /        |           \
              INa        INb           INc
                      /   |   \      /   |   \
                     BIN BIN BINx   BIN BIN BINy
                             /||\           /||\

  Note that it takes some finagling to make the data look this way. An insert
  of sequentially ascending values won't look like this, because opportunistic
  splitting prevents all but the righitmost BIN from being completely full.

  At this point, suppose that thread1 wants to insert into BINx and thread2
  wants to insert into BINy. Our split code looks like this:

  Body of Tree.searchSplitsAllowed()

     rootLatch.acquire()
     fetch rootIN
     rootIN.latch
     opportunitically split root (dropping and re-acquiring rootINlatches)
      splitting the root requires updating the dbmapping tree
     rootLatch.release()

     // leave this block of code owning the rootIN latch.
     call searchSubTreeSplitsAllowed()

  Body of Tree.searchSubTreeSplitsAllowed()
     while (true) {
       try {
          // throws if finds a node that needs splitting
          return searchSubTreeUntilSplit()
       } catch (SplitRequiredException e) {
          // acquire latches down the depth of the tree
          forceSplit();
       }
     }

  If code is executed in this order:

  thread 1 executes searchSplitsAllowed(), root doesn't need splitting
  thread 1 executes searchSubTreeUntilSplit(), throws out because of BINx
  thread 1 hold no latches before executing forceSplit()
  thread 2 executes searchSplitsAllowed(), root doesn't need splitting
  thread 2 executes searchSubTreeUntilSplit(), throws out because of BINy
  thread 2 hold no latches before executing forceSplit()
  thread 1 executes forceSplit, splits BINx, which ripples upward,
               adding a new level 2 IN. The root is full
  thread 2 executes forceSplit, splits BINy, which ripples upward,
               adding a new level 2 IN. The root can't hold the new child!

 The root split is done this way, outside forceSplit, because it's special
 because you must hold the rootLatch.

 This case does not exist for duplicates because:
   a. in 1 case, the owning BIN (the equivalent of the root) stays latched
   b. in a 2nd case, the caller is recovery, which is single threaded.

 The solution was to check for root fullness in forceSplit(), before
 latching down the whole depth of the tree. In that case, we throw out
 and re-execute the rootLatch latching.

********************************************************************/

public class SplitRace_SR11144Test extends DualTestCase {
    private static final boolean DEBUG = false;
    private final File envHome;
    private Environment env = null;
    private Database db = null;

    public SplitRace_SR11144Test() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testSplitRootRace()
        throws Throwable {

        /* Create tree topology described in header comments. */
        initData();

        /*
         * Create two threads, and hold them in a barrier at the
         * designated point in Tree.java. They'll insert keys which
         * will split BINx and BINy.
         */

        InsertThread a = new InsertThread(92, db);
        InsertThread b = new InsertThread(202, db);
        setWaiterHook();
        b.start();
        a.start();

        a.join();
        b.join();

        close();
    }

    /**
     * Create this:
     *                   RootIN
     *            +--------+----------+
     *            /        |           \
     *         INa        INb           INc
     *                 /   |   \      /   |   \
     *                BIN BIN BINx   BIN BIN BINy
     *                        /||\           /||\
     *
     */
    private void initData() {
        try {
            initEnvInternal(true);

            /*
             * Opportunistic splitting will cause the following inserts to
             * add three child entries per parent.
             */
            int value = 0;
            for (int i = 0; i < 23; i++) {
                put(db, value);
                value += 10;
            }

            /* Add a fourth child to BINx and BINy */
            put(db, 91);
            put(db, 201);

            if (DEBUG) {
                dump();
            }
        } catch (DatabaseException DBE) {
            throw new RuntimeException(DBE);
        }
    }

    private static void put(Database db, int value)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        /* put the value in the key. */
        IntegerBinding.intToEntry(11, data);
        IntegerBinding.intToEntry(value, key);

        OperationStatus status = db.putNoOverwrite(null, key, data);
        if (status != OperationStatus.SUCCESS) {
            throw new RuntimeException("status=" + status);
        }
    }

    private void close() {
        try {
            db.close();
            close(env);
        } catch (DatabaseException DBE) {
            throw new RuntimeException(DBE);
        }
    }

    private void dump() {
        try {
            Cursor cursor = db.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while (cursor.getNext(key, data, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {
                System.out.println("<rec key=\"" +
                                   IntegerBinding.entryToInt(key) +
                                   "\" data=\"" +
                                   IntegerBinding.entryToInt(data) +
                                   "\"/>");
            }
            DbInternal.getDbImpl(db).getTree().dump();
            cursor.close();
        } catch (DatabaseException DBE) {
            throw new RuntimeException(DBE);
        }
    }

    private void initEnvInternal(boolean create)
        throws DatabaseException {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(create);
        envConfig.setConfigParam("je.nodeMaxEntries", "4");
        envConfig.setConfigParam("je.nodeDupTreeMaxEntries", "4");
        env = create(envHome, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(create);
        dbConfig.setTransactional(true);
        dbConfig.setExclusiveCreate(create);
        db = env.openDatabase(null, "foo", dbConfig);
    }

    private void setWaiterHook() {
        TestHook hook = new WaiterHook();
        DbInternal.getDbImpl(db).getTree().setWaitHook(hook);
    }

    /*
     * This hook merely acts as a barrier. 2 threads enter and cannot
     * proceed until both have arrived at that point.
     */
    static class WaiterHook implements TestHook {
        private int numArrived;
        private final Object block;

        WaiterHook() {
            numArrived = 0;
            block = new Object();
        }
        public void doHook() {
            synchronized (block) {
                if (numArrived == 0) {
                    numArrived = 1;
                    try {
                        block.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (numArrived == 1) {
                    numArrived = 2;
                    block.notify();
                }
            }
        }
        public Object getHookValue() {
            throw new UnsupportedOperationException();
        }
        public void doIOHook() {
            throw new UnsupportedOperationException();
        }
        public void hookSetup() {
            throw new UnsupportedOperationException();
        }
        public void doHook(Object obj) {
            throw new UnsupportedOperationException();            
        }
    }

    /* This thread merely inserts the specified value. */
    static class InsertThread extends Thread {
        private final int value;
        private final Database db;

        InsertThread(int value, Database db) {
            this.value = value;
            this.db = db;
        }

        @Override
        public void run() {
            try {
                put(db, value);
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }
}
