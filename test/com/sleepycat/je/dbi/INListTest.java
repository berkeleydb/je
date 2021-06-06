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

package com.sleepycat.je.dbi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.junit.JUnitThread;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

public class INListTest extends TestBase {
    private static String DB_NAME = "INListTestDb";
    private File envHome;
    private volatile int sequencer = 0;
    private Environment env;
    private EnvironmentImpl envImpl;
    private Database db;
    private DatabaseImpl dbImpl;

    public INListTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        sequencer = 0;
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
                                 "false");
        envConfig.setConfigParam(EnvironmentConfig.VERIFY_BTREE, "false");
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, DB_NAME, dbConfig);
        dbImpl = DbInternal.getDbImpl(db);
    }

    private void close()
        throws DatabaseException {

        if (db != null) {
            db.close();
        }
        if (env != null) {
            env.close();
        }
        db = null;
        dbImpl = null;
        env = null;
        envImpl = null;
    }

    @After
    public void tearDown() {
        
        try {
            close();
        } catch (Exception e) {
            System.out.println("During tearDown: " + e);
        }
        envHome = null;
    }

    /**
     * This test was originally written when the INList had a major and minor
     * latch.  It was used to test the addition of INs holding the minor latch
     * while another thread holds the major latch.  Now that we're using
     * ConcurrentHashMap this type of testing is not important, but I've left
     * the test in place (without the latching) since it does exercise the
     * INList API a little.
     */
    @Test
    public void testConcurrentAdditions()
        throws Throwable {

        final INList inList1 = new INList(envImpl);
        inList1.enable();

        JUnitThread tester1 =
            new JUnitThread("testConcurrentAdditions-Thread1") {
                @Override
                public void testBody() {

                    try {
                        /* Create two initial elements. */
                        for (int i = 0; i < 2; i++) {
                            IN in = new IN(dbImpl, null, 1, 1);
                            inList1.add(in);
                        }

                        /* Wait for tester2 to try to acquire the
                           /* minor latch */
                        sequencer = 1;
                        while (sequencer <= 1) {
                            Thread.yield();
                        }

                        /*
                         * Sequencer is now 2. There should be three elements
                         * in the list right now because thread 2 added a third
                         * one.
                         */
                        int count = 0;
                        Iterator iter = inList1.iterator();
                        while (iter.hasNext()) {
                            iter.next();
                            count++;
                        }

                        assertEquals(3, count);

                        /*
                         * Allow thread2 to run again.  It will
                         * add another element and throw control
                         * back to thread 1.
                         */
                        sequencer++;   // now it's 3
                        while (sequencer <= 3) {
                            Thread.yield();
                        }

                        /*
                         * Check that the entry added by tester2 was really
                         * added.
                         */
                        count = 0;
                        iter = inList1.iterator();
                        while (iter.hasNext()) {
                            iter.next();
                            count++;
                        }

                        assertEquals(4, count);
                    } catch (Throwable T) {
                        T.printStackTrace(System.out);
                        fail("Thread 1 caught some Throwable: " + T);
                    }
                }
            };

        JUnitThread tester2 =
            new JUnitThread("testConcurrentAdditions-Thread2") {
                @Override
                public void testBody() {

                    try {
                        /* Wait for tester1 to start */
                        while (sequencer < 1) {
                            Thread.yield();
                        }

                        assertEquals(1, sequencer);

                        inList1.add(new IN(dbImpl, null, 1, 1));
                        sequencer++;

                        /* Sequencer is now 2. */

                        while (sequencer < 3) {
                            Thread.yield();
                        }

                        assertEquals(3, sequencer);
                        /* Add one more element. */
                        inList1.add(new IN(dbImpl, null, 1, 1));
                        sequencer++;
                    } catch (Throwable T) {
                        T.printStackTrace(System.out);
                        fail("Thread 2 caught some Throwable: " + T);
                    }
                }
            };

        tester1.start();
        tester2.start();
        tester1.finishTest();
        tester2.finishTest();
    }

    /*
     * Variations of this loop are used in the following tests to simulate the
     * INList memory budget recalculation that is performed by the same loop
     * construct in DirtyINMap.selectDirtyINsForCheckpoint.
     *
     *  inList.memRecalcBegin();
     *  boolean completed = false;
     *  try {
     *      for (IN in : inList) {
     *          inList.memRecalcIterate(in);
     *      }
     *      completed = true;
     *  } finally {
     *      inList.memRecalcEnd(completed);
     *  }
     */

    /**
     * Scenario #1: IN size is unchanged during the iteration
     *  begin
     *   iterate -- add total IN size, mark processed
     *  end
     */
    @Test
    public void testMemBudgetReset1()
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();
        MemoryBudget mb = envImpl.getMemoryBudget();

        long origTreeMem = getActualTreeMemoryUsage(mb, inList);
        inList.memRecalcBegin();
        boolean completed = false;
        try {
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem, mb.getTreeMemoryUsage());

        close();
    }

    /**
     * Scenario #2: IN size is updated during the iteration
     *  begin
     *   update  -- do not add delta because IN is not yet processed
     *   iterate -- add total IN size, mark processed
     *   update  -- do add delta because IN was already processed
     *  end
     */
    @Test
    public void testMemBudgetReset2()
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();
        MemoryBudget mb = envImpl.getMemoryBudget();

        /*
         * Size changes must be greater than IN.ACCUMULATED_LIMIT to be
         * counted in the budget, and byte array lengths should be a multiple
         * of 4 to give predictable sizes, since array sizes are allowed in
         * multiples of 4.
         */
        final int SIZE = IN.ACCUMULATED_LIMIT + 100;
        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        db.put(null, key, new DatabaseEntry(new byte[SIZE * 1]));

        /* Test increasing size. */
        long origTreeMem = getActualTreeMemoryUsage(mb, inList);
        inList.memRecalcBegin();
        boolean completed = false;
        try {
            db.put(null, key, new DatabaseEntry(new byte[SIZE * 2]));
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            db.put(null, key, new DatabaseEntry(new byte[SIZE * 3]));
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem + SIZE * 2, mb.getTreeMemoryUsage());

        /* Test decreasing size. */
        inList.memRecalcBegin();
        completed = false;
        try {
            db.put(null, key, new DatabaseEntry(new byte[SIZE * 2]));
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            db.put(null, key, new DatabaseEntry(new byte[SIZE * 1]));
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem, mb.getTreeMemoryUsage());

        close();
    }

    /**
     * Scenario #3: IN is added during the iteration but not iterated
     *  begin
     *   add -- add IN size, mark processed
     *  end
     */
    @Test
    public void testMemBudgetReset3()
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();
        MemoryBudget mb = envImpl.getMemoryBudget();

        IN newIn = new IN(dbImpl, null, 1, 1);
        long size = newIn.getBudgetedMemorySize();

        long origTreeMem = getActualTreeMemoryUsage(mb, inList);
        inList.memRecalcBegin();
        boolean completed = false;
        try {
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            inList.add(newIn);
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem + size, mb.getTreeMemoryUsage());

        close();
    }

    /**
     * Scenario #4: IN is added during the iteration and is iterated
     *  begin
     *   add     -- add IN size, mark processed
     *   iterate -- do not add size because IN was already processed
     *  end
     */
    @Test
    public void testMemBudgetReset4()
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();
        MemoryBudget mb = envImpl.getMemoryBudget();

        IN newIn = new IN(dbImpl, null, 1, 1);
        long size = newIn.getBudgetedMemorySize();

        long origTreeMem = getActualTreeMemoryUsage(mb, inList);
        inList.memRecalcBegin();
        boolean completed = false;
        try {
            inList.add(newIn);
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem + size, mb.getTreeMemoryUsage());

        close();
    }

    /**
     * Scenario #5: IN is removed during the iteration but not iterated
     *  begin
     *   remove  -- do not add delta because IN is not yet processed
     *  end
     */
    @Test
    public void testMemBudgetReset5()
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();
        MemoryBudget mb = envImpl.getMemoryBudget();

        IN oldIn = inList.iterator().next();
        long size = oldIn.getInMemorySize();

        long origTreeMem = getActualTreeMemoryUsage(mb, inList);
        inList.memRecalcBegin();
        boolean completed = false;
        try {
            inList.remove(oldIn);
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem - size, mb.getTreeMemoryUsage());

        close();
    }

    /**
     * Scenario #6: IN is removed during the iteration and is iterated
     *  begin
     *   iterate -- add total IN size, mark processed
     *   remove  -- add delta because IN was already processed
     *  end
     */
    @Test
    public void testMemBudgetReset6()
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();
        MemoryBudget mb = envImpl.getMemoryBudget();

        IN oldIn = inList.iterator().next();
        long size = oldIn.getInMemorySize();

        long origTreeMem = getActualTreeMemoryUsage(mb, inList);
        inList.memRecalcBegin();
        boolean completed = false;
        try {
            for (IN in : inList) {
                inList.memRecalcIterate(in);
            }
            inList.remove(oldIn);
            completed = true;
        } finally {
            inList.memRecalcEnd(completed);
        }
        assertEquals(origTreeMem - size, mb.getTreeMemoryUsage());

        close();
    }

    private long getActualTreeMemoryUsage(final MemoryBudget mb,
                                          final INList inList) {
        long actual = 0;
        long budgeted = 0;
        for (IN in : inList) {
            budgeted += in.getBudgetedMemorySize();
            actual += in.getInMemorySize();
        }
        assertEquals(budgeted, mb.getTreeMemoryUsage());
        return actual;
    }
}
