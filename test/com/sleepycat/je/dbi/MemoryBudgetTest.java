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
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.Test;

public class MemoryBudgetTest extends TestBase {
    private final File envHome;

    public MemoryBudgetTest() {
        envHome = SharedTestUtils.getTestDir();
    }

    @Test
    public void testDefaults()
        throws Exception {

        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        Environment env = new Environment(envHome, envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        MemoryBudget testBudget = envImpl.getMemoryBudget();

        /*
        System.out.println("max=    " + testBudget.getMaxMemory());
        System.out.println("log=    " + testBudget.getLogBufferBudget());
        System.out.println("thresh= " + testBudget.getEvictorCheckThreshold());
        */

        assertTrue(testBudget.getMaxMemory() > 0);
        assertTrue(testBudget.getLogBufferBudget() > 0);

        assertTrue(testBudget.getMaxMemory() <=
                   JVMSystemUtils.getRuntimeMaxMemory());

        env.close();
    }

    /* Verify that the proportionally based setting works. */
    @Test
    public void testCacheSizing()
        throws Exception {

        long jvmMemory = JVMSystemUtils.getRuntimeMaxMemory();

        /*
         * Runtime.maxMemory() may return Long.MAX_VALUE if there is no
         * inherent limit.
         */
        if (jvmMemory == Long.MAX_VALUE) {
            jvmMemory = 1 << 26;
        }

        /* The default cache size ought to be percentage based. */
        EnvironmentConfig envConfig = TestUtils.initEnvConfig();
        envConfig.setAllowCreate(true);
        Environment env = new Environment(envHome, envConfig);
        EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        long percentConfig = envImpl.getConfigManager().
            getInt(EnvironmentParams.MAX_MEMORY_PERCENT);

        EnvironmentConfig c = env.getConfig();
        long expectedMem = (jvmMemory * percentConfig) / 100;
        assertEquals(expectedMem, c.getCacheSize());
        assertEquals(expectedMem, envImpl.getMemoryBudget().getMaxMemory());
        env.close();

        /* Try setting the percentage.*/
        expectedMem = (jvmMemory * 30) / 100;
        envConfig = TestUtils.initEnvConfig();
        envConfig.setCachePercent(30);
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);
        c = env.getConfig();
        assertEquals(expectedMem, c.getCacheSize());
        assertEquals(expectedMem, envImpl.getMemoryBudget().getMaxMemory());
        env.close();

        /* Try overriding */
        envConfig = TestUtils.initEnvConfig();
        envConfig.setCacheSize(MemoryBudget.MIN_MAX_MEMORY_SIZE + 10);
        env = new Environment(envHome, envConfig);
        envImpl = DbInternal.getNonNullEnvImpl(env);
        c = env.getConfig();
        assertEquals(MemoryBudget.MIN_MAX_MEMORY_SIZE + 10, c.getCacheSize());
        assertEquals(MemoryBudget.MIN_MAX_MEMORY_SIZE + 10,
                     envImpl.getMemoryBudget().getMaxMemory());
        env.close();
    }
}
