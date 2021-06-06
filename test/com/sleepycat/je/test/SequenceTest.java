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

package com.sleepycat.je.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.SequenceStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.util.test.TxnTestCase;

@RunWith(Parameterized.class)
public class SequenceTest extends TxnTestCase {

    @Parameters
    public static List<Object[]> genParams() {
        return getTxnParams(null, false);
    }
    
    public SequenceTest(String type){
        initEnvConfig();
        txnType = type;
        isTransactional = (txnType != TXN_NULL);
        customName = txnType;
    }

    @Test
    public void testIllegal()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        SequenceConfig config = new SequenceConfig();
        config.setAllowCreate(true);

        /* Duplicates not allowed. */

        Database db = openDb("dups", true);
        Transaction txn = txnBegin();
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (UnsupportedOperationException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("duplicates") >= 0);
        }
        txnCommit(txn);
        db.close();

        db = openDb("foo");
        txn = txnBegin();

        /* Range min must be less than max. */

        config.setRange(0, 0);
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (IllegalArgumentException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("less than the maximum") >= 0);
        }

        /* Initial value must be within range. */

        config.setRange(-10, 10);
        config.setInitialValue(-11);
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (IllegalArgumentException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("out of range") >= 0);
        }
        config.setInitialValue(11);
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (IllegalArgumentException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("out of range") >= 0);
        }

        /* Cache size must be within range. */

        config.setRange(-10, 10);
        config.setCacheSize(21);
        config.setInitialValue(0);
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (IllegalArgumentException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("cache size is larger") >= 0);
        }

        /* Create with legal range values. */

        config.setRange(1, 2);
        config.setInitialValue(1);
        config.setCacheSize(0);
        Sequence seq = db.openSequence(txn, key, config);

        /* Key must not exist if ExclusiveCreate=true. */

        config.setExclusiveCreate(true);
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (DatabaseException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("already exists") >= 0);
        }
        config.setExclusiveCreate(false);
        seq.close();

        /* Key must exist if AllowCreate=false. */

        db.removeSequence(txn, key);
        config.setAllowCreate(false);
        try {
            db.openSequence(txn, key, config);
            fail();
        } catch (DatabaseException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("does not exist") >= 0);
        }

        /* Check wrapping not allowed. */

        db.removeSequence(txn, key);
        config.setAllowCreate(true);
        config.setRange(-5, 5);
        config.setInitialValue(-5);
        seq = db.openSequence(txn, key, config);
        for (long i = config.getRangeMin(); i <= config.getRangeMax(); i++) {
            assertEquals(i, seq.get(txn, 1));
        }
        try {
            seq.get(txn, 1);
            fail();
        } catch (DatabaseException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("overflow") >= 0);
        }

        /* Check wrapping not allowed, decrement. */

        db.removeSequence(txn, key);
        config.setAllowCreate(true);
        config.setAllowCreate(true);
        config.setRange(-5, 5);
        config.setInitialValue(5);
        config.setDecrement(true);
        seq = db.openSequence(txn, key, config);
        for (long i = config.getRangeMax(); i >= config.getRangeMin(); i--) {
            assertEquals(i, seq.get(txn, 1));
        }
        try {
            seq.get(txn, 1);
            fail();
        } catch (DatabaseException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("overflow") >= 0);
        }

        /* Check delta less than one. */
        try {
            seq.get(txn, 0);
            fail();
        } catch (IllegalArgumentException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("greater than zero") >= 0);
        }

        /* Check delta greater than range. */
        try {
            seq.get(txn, 11);
            fail();
        } catch (IllegalArgumentException expected) {
            String msg = expected.getMessage();
            assertTrue(msg, msg.indexOf("larger than the range") >= 0);
        }

        seq.close();
        txnCommit(txn);
        db.close();
    }

    @Test
    public void testBasic()
        throws DatabaseException {

        Database db = openDb("foo");
        DatabaseEntry key = new DatabaseEntry(new byte[0]);
        DatabaseEntry data = new DatabaseEntry();

        SequenceConfig config = new SequenceConfig();
        config.setAllowCreate(true);

        Transaction txn = txnBegin();
        Sequence seq = db.openSequence(txn, key, config);
        txnCommit(txn);

        txn = txnBegin();

        /* Check default values before calling get(). */

        SequenceStats stats = seq.getStats(null);
        assertEquals(0, stats.getCurrent());
        assertEquals(0, stats.getCacheSize());
        assertEquals(0, stats.getNGets());
        assertEquals(Long.MIN_VALUE, stats.getMin());
        assertEquals(Long.MAX_VALUE, stats.getMax());

        /* Get the first value. */

        long val = seq.get(txn, 1);
        assertEquals(0, val);
        stats = seq.getStats(null);
        assertEquals(1, stats.getCurrent());
        assertEquals(1, stats.getValue());
        assertEquals(0, stats.getLastValue());
        assertEquals(1, stats.getNGets());

        /* Use deltas greater than one. */

        assertEquals(1, seq.get(txn, 2));
        assertEquals(3, seq.get(txn, 3));
        assertEquals(6, seq.get(txn, 1));
        assertEquals(7, seq.get(txn, 1));

        /* Remove a sequence and expect the key to be deleted. */

        seq.close();
        db.removeSequence(txn, key);
        assertEquals(OperationStatus.NOTFOUND, db.get(txn, key, data, null));
        txnCommit(txn);
        assertEquals(OperationStatus.NOTFOUND, db.get(null, key, data, null));

        db.close();
    }

    @Test
    public void testMultipleHandles()
        throws DatabaseException {

        Database db = openDb("foo");
        DatabaseEntry key = new DatabaseEntry(new byte[0]);

        /* Create a sequence. */

        SequenceConfig config = new SequenceConfig();
        config.setAllowCreate(true);
        config.setDecrement(true);
        config.setRange(1, 3);
        config.setInitialValue(3);

        Transaction txn = txnBegin();
        Sequence seq = db.openSequence(txn, key, config);
        assertEquals(3, seq.get(txn, 1));
        txnCommit(txn);

        /* Open another handle on the same sequence -- config should match. */

        txn = txnBegin();
        Sequence seq2 = db.openSequence(txn, key, config);
        assertEquals(2, seq2.get(txn, 1));
        txnCommit(txn);

        SequenceStats stats = seq2.getStats(null);
        assertEquals(1, stats.getCurrent());
        assertEquals(1, stats.getMin());
        assertEquals(3, stats.getMax());

        /* Values are assigned from a single sequence for both handles. */

        assertEquals(1, seq.get(null, 1));

        seq.close();
        seq2.close();
        db.close();
    }

    @Test
    public void testRanges()
        throws DatabaseException {

        Database db = openDb("foo");

        /* Positive and negative ranges. */

        doRange(db, 1, 10, 1, 0);
        doRange(db, -10, -1, 1, 0);
        doRange(db, -10, 10, 1, 0);

        /* Extreme min/max values. */

        doRange(db, Integer.MIN_VALUE, Integer.MIN_VALUE + 10, 1, 0);
        doRange(db, Integer.MAX_VALUE - 10, Integer.MAX_VALUE, 1, 0);

        doRange(db, Long.MIN_VALUE, Long.MIN_VALUE + 10, 1, 0);
        doRange(db, Long.MAX_VALUE - 10, Long.MAX_VALUE, 1, 0);

        /* Deltas greater than one. */

        doRange(db, -10, 10, 2, 0);
        doRange(db, -10, 10, 3, 0);
        doRange(db, -10, 10, 5, 0);
        doRange(db, -10, 10, 10, 0);
        doRange(db, -10, 10, 20, 0);

        /*
         * Cache sizes.  We cheat a little by making the cache size an even
         * multiple of the delta whenever the cache size is greater than the
         * delta; otherwise, it is too difficult to predict caching.
         */

        doRange(db, -10, 10, 1, 1);
        doRange(db, -10, 10, 1, 2);
        doRange(db, -10, 10, 1, 3);
        doRange(db, -10, 10, 1, 7);
        doRange(db, -10, 10, 1, 20);
        doRange(db, -10, 10, 3, 1);
        doRange(db, -10, 10, 3, 2);
        doRange(db, -10, 10, 3, 3);
        doRange(db, -10, 10, 3, 9);
        doRange(db, -10, 10, 3, 18);

        db.close();
    }

    private void doRange(Database db, long min, long max, int delta, int cache)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry(new byte[1]);
        boolean incr;
        boolean wrap;

        for (int option = 0; option < 4; option += 1) {
            switch (option) {
            case 0:
                incr = true;
                wrap = false;
                break;
            case 1:
                incr = true;
                wrap = false;
                break;
            case 2:
                incr = true;
                wrap = false;
                break;
            case 3:
                incr = true;
                wrap = false;
                break;
            default:
                throw new IllegalStateException();
            }

            SequenceConfig config = new SequenceConfig();
            config.setAllowCreate(true);
            config.setInitialValue(incr ? min : max);
            config.setWrap(wrap);
            config.setDecrement(!incr);
            config.setRange(min, max);
            config.setCacheSize(cache);

            String msg =
                "incr=" + incr +
                " wrap=" + wrap +
                " min=" + min +
                " max=" + max +
                " delta=" + delta +
                " cache=" + cache;

            Transaction txn = txnBegin();
            db.removeSequence(txn, key);
            Sequence seq = db.openSequence(txn, key, config);
            txnCommit(txn);

            txn = txnBegin();

            if (incr) {
                for (long i = min;; i += delta) {

                    boolean expectCached = false;
                    if (cache != 0) {
                        expectCached = delta < cache && i != max &&
                            (((i - min) % cache) != 0);
                    }

                    doOne(msg, seq, txn, delta, i, expectCached);

                    /* Test for end without causing long overflow. */
                    if (i > max - delta) {
                        if (delta == 1) {
                            assertEquals(msg, i, max);
                        }
                        break;
                    }
                }
                if (wrap) {
                    assertEquals(msg, min, seq.get(txn, delta));
                    assertEquals(msg, min + delta, seq.get(txn, delta));
                }
            } else {
                for (long i = max;; i -= delta) {

                    boolean expectCached = false;
                    if (cache != 0) {
                        expectCached = delta < cache && i != min &&
                            (((max - i) % cache) != 0);
                    }

                    doOne(msg, seq, txn, delta, i, expectCached);

                    /* Test for end without causing long overflow. */
                    if (i < min + delta) {
                        if (delta == 1) {
                            assertEquals(msg, i, min);
                        }
                        break;
                    }
                }
                if (wrap) {
                    assertEquals(msg, max, seq.get(txn, delta));
                    assertEquals(msg, max - delta, seq.get(txn, delta));
                }
            }

            if (!wrap) {
                try {
                    seq.get(txn, delta);
                    fail(msg);
                } catch (DatabaseException expected) {
                    String emsg = expected.getMessage();
                    assertTrue(emsg, emsg.indexOf("overflow") >= 0);
                }
            }

            txnCommit(txn);
            seq.close();
        }
    }

    private void doOne(String msg,
                       Sequence seq,
                       Transaction txn,
                       int delta,
                       long expectValue,
                       boolean expectCached)
        throws DatabaseException {

        msg += " value=" + expectValue;

        try {
            assertEquals(msg, expectValue, seq.get(txn, delta));
        } catch (DatabaseException e) {
            fail(msg + ' ' + e);
        }

        StatsConfig clearConfig = new StatsConfig();
        clearConfig.setFast(true);
        clearConfig.setClear(true);
        SequenceStats stats = seq.getStats(clearConfig);

        assertEquals(msg, 1, stats.getNGets());
        assertEquals(msg, expectCached ? 1 : 0, stats.getNCachedGets());
    }

    private Database openDb(String name)
        throws DatabaseException {

        return openDb(name, false);
    }

    private Database openDb(String name, boolean duplicates)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(isTransactional);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(duplicates);

        Transaction txn = txnBegin();
        try {
            return env.openDatabase(txn, name, dbConfig);
        } finally {
            txnCommit(txn);
        }
    }
}
