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
package com.sleepycat.je.rep.dual.trigger;

import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.trigger.Trigger;

public class ConfigTest extends com.sleepycat.je.trigger.ConfigTest {

    Environment env;

    @Before
    public void setUp() 
       throws Exception {

        super.setUp();
        env = create(envRoot, envConfig);
    }

    @After
    public void tearDown() 
        throws Exception {
        
        close(env);
        super.tearDown();
    }

    @Test
    public void testTriggerConfigOnEnvOpen() {
        dbConfig.setTriggers(Arrays.asList((Trigger) new InvokeTest.DBT("t1"),
                             (Trigger) new InvokeTest.DBT("t2")));

        /* Implementing ReplicatedDatabaseTrigger (RDBT) is expected. */
        try {
            env.openDatabase(null, "db1", dbConfig).close();
            fail("IAE expected");
        } catch (IllegalArgumentException iae) {
            // expected
        }

    }
}
