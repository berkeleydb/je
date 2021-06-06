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

package com.sleepycat.je.config;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.util.test.TestBase;

public class EnvironmentParamsTest extends TestBase {

    private IntConfigParam intParam =
        new IntConfigParam("param.int",
                           new Integer(2),
                           new Integer(10),
                           new Integer(5),
                           false, // mutable
                           false);// for replication

    private LongConfigParam longParam =
        new LongConfigParam("param.long",
                            new Long(2),
                            new Long(10),
                            new Long(5),
                            false, // mutable
                            false);// for replication

    private ConfigParam mvParam =
        new ConfigParam("some.mv.param.#", null, true /* mutable */,
                        false /* for replication */);

    /**
     * Test param validation.
     */
    @Test
    public void testValidation() {
        assertTrue(mvParam.isMultiValueParam());

        try {
            new ConfigParam(null, "foo", false /* mutable */,
                            false /* for replication */);
            fail("should disallow null name");
        } catch (EnvironmentFailureException e) {
            // expected.
        }

        /* Test bounds. These are all invalid and should fail */
        checkValidateParam(intParam, "1");
        checkValidateParam(intParam, "11");
        checkValidateParam(longParam, "1");
        checkValidateParam(longParam, "11");
    }

    /**
     * Check that an invalid parameter isn't mistaken for a multivalue
     * param.
     */
    @Test
    public void testInvalidVsMultiValue() {
        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setConfigParam("je.maxMemory.stuff", "true");
            fail("Should throw exception");
        } catch (IllegalArgumentException IAE) {
            // expected
        }
    }

    /* Helper to catch expected exceptions */
    private void checkValidateParam(ConfigParam param, String value) {
        try {
            param.validateValue(value);
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // expect this exception
        }
    }
}
