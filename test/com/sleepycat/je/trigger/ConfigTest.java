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
package com.sleepycat.je.trigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.rep.dual.trigger.InvokeTest;

public class ConfigTest extends TestBase {

    @Test
    public void testConflictingTypes() {
        DatabaseConfig dc = new DatabaseConfig();
        try {
            dc.setTriggers(Arrays.asList((Trigger) new DBT("t1"),
                           (Trigger) new InvokeTest.RDBT("t2")));
            fail("IAE expected");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testConflictingNames() {
        DatabaseConfig dc = new DatabaseConfig();
        try {
            dc.setTriggers(Arrays.asList((Trigger) new DBT("t1"),
                           (Trigger) new DBT("t1")));
            fail("IAE expected");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testSecondaryConfig() {
        SecondaryConfig sc = new SecondaryConfig();

        try {
            sc.setTriggers(Arrays.asList((Trigger) new DBT("t1"),
                           (Trigger) new DBT("t2")));
            fail("IAE expected");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        try {
            sc.setOverrideTriggers(true);
            fail("IAE expected");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        assertEquals(0,sc.getTriggers().size());
        assertFalse(sc.getOverrideTriggers());
    }
}
