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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Random;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class INTest extends TestBase {
    static private final int N_BYTES_IN_KEY = 3;
    private int initialINCapacity;
    private DatabaseImpl db = null;
    static private long FAKE_LSN = DbLsn.makeLsn(0, 0);
    private Environment noLogEnv;
    private final File envHome;

    public INTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setConfigParam(EnvironmentParams.NODE_MAX.getName(), "6");
        envConfig.setAllowCreate(true);
        noLogEnv = new Environment(envHome, envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(noLogEnv);
        initialINCapacity = envImpl.getConfigManager().
            getInt(EnvironmentParams.NODE_MAX);
        db = new DatabaseImpl(null,
                              "foo", new DatabaseId(11), envImpl,
                              new DatabaseConfig());
    }

    @After
    public void tearDown() {

        db.releaseTreeAdminMemory();
        noLogEnv.close();
    }

    @Test
    public void testFindEntry()
        throws DatabaseException {

        IN in = new IN(db, new byte[0], initialINCapacity, 7);
        in.latch();

        byte[] zeroBytes = new byte[N_BYTES_IN_KEY];
        for (int i = 0; i < N_BYTES_IN_KEY; i++) {
            zeroBytes[i] = 0x00;
        }

        byte[] maxBytes = new byte[N_BYTES_IN_KEY];
        for (int i = 0; i < N_BYTES_IN_KEY; i++) {

            /*
             * Use FF since that sets the sign bit negative on a byte.  This
             * checks the Key.compareTo routine for proper unsigned
             * comparisons.
             */
            maxBytes[i] = (byte) 0xFF;
        }

        assertTrue(in.findEntry(zeroBytes, false, false) == -1);
        assertTrue(in.findEntry(maxBytes, false, false) == -1);
        assertTrue(in.findEntry(zeroBytes, false, true) == -1);
        assertTrue(in.findEntry(maxBytes, false, true) == -1);
        assertTrue(in.findEntry(zeroBytes, true, false) == -1);
        assertTrue(in.findEntry(maxBytes, true, false) == -1);
        assertTrue(in.findEntry(zeroBytes, true, true) == -1);
        assertTrue(in.findEntry(maxBytes, true, true) == -1);
        for (int i = 0; i < initialINCapacity; i++) {

            /*
             * Insert a key and check that we get the same index in return from
             * the binary search.  Check the next highest and next lowest keys
             * also.
             */
            byte[] keyBytes = new byte[N_BYTES_IN_KEY];
            byte[] nextKeyBytes = new byte[N_BYTES_IN_KEY];
            byte[] prevKeyBytes = new byte[N_BYTES_IN_KEY];
            nextKeyBytes[0] = prevKeyBytes[0] = keyBytes[0] = 0x01;
            nextKeyBytes[1] = prevKeyBytes[1] = keyBytes[1] = (byte) i;
            nextKeyBytes[2] = prevKeyBytes[2] = keyBytes[2] = 0x10;
            nextKeyBytes[2]++;
            prevKeyBytes[2]--;

            int insertionFlags = in.insertEntry1(
                null, keyBytes, null, FAKE_LSN, false);

            assertTrue((insertionFlags & IN.INSERT_SUCCESS) != 0);
            assertEquals(i, insertionFlags & ~IN.INSERT_SUCCESS);
            assertTrue(in.findEntry(zeroBytes, false, false) == 0);
            assertTrue(in.findEntry(maxBytes, false, false) == i);
            assertTrue(in.findEntry(zeroBytes, false, true) == -1);
            assertTrue(in.findEntry(maxBytes, false, true) == -1);
            assertTrue(in.findEntry(zeroBytes, true, false) == -1);
            assertTrue(in.findEntry(maxBytes, true, false) == i);
            assertTrue(in.findEntry(zeroBytes, true, true) == -1);
            assertTrue(in.findEntry(maxBytes, true, true) == -1);
            for (int j = 1; j < in.getNEntries(); j++) { // 0th key is virtual
                assertTrue(in.findEntry(in.getKey(j), false, false)
                           == j);
                assertTrue(in.findEntry(in.getKey(j), false, true)
                           == j);
                assertTrue(in.findEntry(in.getKey(j), true, false) ==
                           (j | IN.EXACT_MATCH));
                assertTrue(in.findEntry(in.getKey(j), true, true) ==
                           (j | IN.EXACT_MATCH));
                assertTrue(in.findEntry(nextKeyBytes, false, false) == i);
                assertTrue(in.findEntry(prevKeyBytes, false, false) == i - 1);
                assertTrue(in.findEntry(nextKeyBytes, false, true) == -1);
                assertTrue(in.findEntry(prevKeyBytes, false, true) == -1);
            }
        }
        in.releaseLatch();
    }

    @Test
    public void testInsertEntry()
        throws DatabaseException {

        for (int i = 0; i < 10; i++) {          // cwl: consider upping this
            doInsertEntry(false);
            doInsertEntry(true);
        }
    }

    private void doInsertEntry(boolean withMinMax)
        throws DatabaseException {

        IN in = new IN(db, new byte[0], initialINCapacity, 7);
        in.latch();

        byte[] zeroBytes = new byte[N_BYTES_IN_KEY];
        for (int i = 0; i < N_BYTES_IN_KEY; i++) {
            zeroBytes[i] = 0x00;
        }

        byte[] maxBytes = new byte[N_BYTES_IN_KEY];
        for (int i = 0; i < N_BYTES_IN_KEY; i++) {
            maxBytes[i] = (byte) 0xFF;
        }

        if (withMinMax) {
            try {
                in.insertEntry(null, zeroBytes, FAKE_LSN);
                in.insertEntry(null, maxBytes, FAKE_LSN);
            } catch (Exception e) {
                fail("caught " + e);
            }

            assertTrue(in.findEntry(zeroBytes, false, false) == 0);
            assertTrue(in.findEntry(maxBytes, false, false) == 1);
            /* Shadowed by the virtual 0'th key. */
            assertTrue(in.findEntry(zeroBytes, false, true) == 0);
            assertTrue(in.findEntry(maxBytes, false, true) == 1);

            assertTrue(in.findEntry(zeroBytes, true, false) == IN.EXACT_MATCH);
            assertTrue(in.findEntry(maxBytes, true, false) ==
                       (1 | IN.EXACT_MATCH));
            /* Shadowed by the virtual 0'th key. */
            assertTrue(in.findEntry(zeroBytes, true, true) == IN.EXACT_MATCH);
            assertTrue(in.findEntry(maxBytes, true, true) ==
                       (1 | IN.EXACT_MATCH));
        }

        Random rnd = new Random();

        try {
            for (int i = 0;
                 i < initialINCapacity - (withMinMax ? 2 : 0);
                 i++) {

                /*
                 * Insert a key and check that we get the same index in return
                 * from the binary search.  Check the next highest and next
                 * lowest keys also.
                 */
                byte[] keyBytes = new byte[N_BYTES_IN_KEY];

                /*
                 * There's a small chance that we may generate the same
                 * sequence of bytes that are already present.
                 */
                while (true) {
                    rnd.nextBytes(keyBytes);
                    int index = in.findEntry(keyBytes, true, false);
                    if ((index & IN.EXACT_MATCH) != 0 &&
                        index >= 0) {
                        continue;
                    }
                    break;
                }

                in.insertEntry(null, keyBytes, FAKE_LSN);

                if (withMinMax) {
                    assertTrue(in.findEntry(zeroBytes, false, false) == 0);
                    assertTrue(in.findEntry(maxBytes, false, false) ==
                               in.getNEntries() - 1);
                    /* Shadowed by the virtual 0'th key. */
                    assertTrue(in.findEntry(zeroBytes, false, true) == 0);
                    assertTrue(in.findEntry(maxBytes, false, true) ==
                               in.getNEntries() - 1);

                    assertTrue(in.findEntry(zeroBytes, true, false) ==
                               IN.EXACT_MATCH);
                    assertTrue(in.findEntry(maxBytes, true, false) ==
                               ((in.getNEntries() - 1) | IN.EXACT_MATCH));
                    /* Shadowed by the virtual 0'th key. */
                    assertTrue(in.findEntry(zeroBytes, true, true) ==
                               IN.EXACT_MATCH);
                    assertTrue(in.findEntry(maxBytes, true, true) ==
                               ((in.getNEntries() - 1) | IN.EXACT_MATCH));
                } else {
                    assertTrue(in.findEntry(zeroBytes, false, false) == 0);
                    assertTrue(in.findEntry(maxBytes, false, false) ==
                               in.getNEntries() - 1);
                    assertTrue(in.findEntry(zeroBytes, false, true) == -1);
                    assertTrue(in.findEntry(maxBytes, false, true) == -1);

                    assertTrue(in.findEntry(zeroBytes, true, false) == -1);
                    assertTrue(in.findEntry(maxBytes, true, false) ==
                               in.getNEntries() - 1);
                }

                for (int j = 1; j < in.getNEntries(); j++) {
                    assertTrue(in.findEntry(in.getKey(j), false, false) == j);
                    assertTrue(in.findEntry(in.getKey(j), false, true) == j);

                    assertTrue(in.findEntry(in.getKey(j), false, true) == j);
                    assertTrue(in.findEntry(in.getKey(j), true, false) ==
                               (j | IN.EXACT_MATCH));
                }
            }
        } catch (Exception e) {
            fail("caught " + e);
        }

        /* Should be full so insertEntry should return false. */
        byte[] keyBytes = new byte[N_BYTES_IN_KEY];
        rnd.nextBytes(keyBytes);

        try {
            in.insertEntry(null, keyBytes, FAKE_LSN);
            fail("should have caught UNEXPECTED_STATE, but didn't");
        } catch (EnvironmentFailureException e) {
            assertSame(EnvironmentFailureReason.
                       UNEXPECTED_STATE_FATAL, e.getReason());
        }
        in.releaseLatch();
    }

    @Test
    public void testDeleteEntry()
        throws DatabaseException {

        for (int i = 0; i < 10; i++) {           // cwl: consider upping this
            doDeleteEntry(true);
            doDeleteEntry(false);
        }
    }

    private void doDeleteEntry(boolean withMinMax)
        throws DatabaseException {

        IN in = new IN(db, new byte[0], initialINCapacity, 7);
        in.latch();

        byte[] zeroBytes = new byte[N_BYTES_IN_KEY];
        for (int i = 0; i < N_BYTES_IN_KEY; i++) {
            zeroBytes[i] = 0x00;
        }

        byte[] maxBytes = new byte[N_BYTES_IN_KEY];
        for (int i = 0; i < N_BYTES_IN_KEY; i++) {
            maxBytes[i] = (byte) 0xFF;
        }

        if (withMinMax) {
            try {
                in.insertEntry(null, zeroBytes, FAKE_LSN);
                in.insertEntry(null, maxBytes, FAKE_LSN);
            } catch (Exception e) {
                fail("caught " + e);
            }

            assertTrue(in.findEntry(zeroBytes, false, false) == 0);
            assertTrue(in.findEntry(maxBytes, false, false) == 1);
            /* Shadowed by the virtual 0'th key. */
            assertTrue(in.findEntry(zeroBytes, false, true) == 0);
            assertTrue(in.findEntry(maxBytes, false, true) == 1);

            assertTrue(in.findEntry(zeroBytes, true, false) == IN.EXACT_MATCH);
            assertTrue(in.findEntry(maxBytes, true, false) ==
                       (1 | IN.EXACT_MATCH));
            /* Shadowed by the virtual 0'th key. */
            assertTrue(in.findEntry(zeroBytes, true, true) == IN.EXACT_MATCH);
            assertTrue(in.findEntry(maxBytes, true, true) ==
                       (1 | IN.EXACT_MATCH));
        }

        Random rnd = new Random();

        try {
            /* Fill up the IN with random entries. */
            for (int i = 0;
                 i < initialINCapacity - (withMinMax ? 2 : 0);
                 i++) {

                /*
                 * Insert a key and check that we get the same index in return
                 * from the binary search.  Check the next highest and next
                 * lowest keys also.
                 */
                byte[] keyBytes = new byte[N_BYTES_IN_KEY];

                /*
                 * There's a small chance that we may generate the same
                 * sequence of bytes that are already present.
                 */
                while (true) {
                    rnd.nextBytes(keyBytes);
                    int index = in.findEntry(keyBytes, true, false);
                    if ((index & IN.EXACT_MATCH) != 0 &&
                        index >= 0) {
                        continue;
                    }
                    break;
                }

                in.insertEntry(null, keyBytes, FAKE_LSN);
            }

            if (withMinMax) {
                assertTrue(in.findEntry(zeroBytes, false, false) == 0);
                assertTrue(in.findEntry(maxBytes, false, false) ==
                           in.getNEntries() - 1);
                /*
                 * zeroBytes is in the 0th entry, but that's the virtual key so
                 * it's not an exact match.
                 */
                assertTrue(in.findEntry(zeroBytes, false, true) == 0);
                assertTrue(in.findEntry(maxBytes, false, true) ==
                           in.getNEntries() - 1);

                assertTrue(in.findEntry(zeroBytes, false, true) == 0);
                assertTrue(in.findEntry(maxBytes, false, true) ==
                           in.getNEntries() - 1);
                assertTrue(in.findEntry(zeroBytes, true, false) ==
                           IN.EXACT_MATCH);
                assertTrue(in.findEntry(maxBytes, true, false) ==
                           ((in.getNEntries() - 1) | IN.EXACT_MATCH));
            }

            while (in.getNEntries() > 1) {
                int i = rnd.nextInt(in.getNEntries() - 1) + 1;
                assertTrue(deleteEntry(in, in.getKey(i)));
            }

            /*
             * We should only be able to delete the zero Key if it was inserted
             * in the first place.
             */
            assertEquals(withMinMax, deleteEntry(in, zeroBytes));
        } catch (Exception e) {
            e.printStackTrace();
            fail("caught " + e);
        }
        in.releaseLatch();
    }

    private boolean deleteEntry(IN in, byte[] key)
        throws DatabaseException {

        assert(!in.isBINDelta());

        if (in.getNEntries() == 0) {
            return false;
        }

        int index = in.findEntry(key, false, true);
        if (index < 0) {
            return false;
        }

        /* We cannot validate because the FAKE_LSN is present. */
        in.deleteEntry(index, true /*makeDirty*/, false /*validate*/);
        return true;
    }
}
