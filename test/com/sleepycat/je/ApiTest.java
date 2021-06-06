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

package com.sleepycat.je;

import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Test parameter handling for api methods.
 */
public class ApiTest {

    @Test
    public void testBasic() {
        try {
            new Environment(null, null);
            fail("Should get exception");
        } catch (IllegalArgumentException e) {
            // expected exception
        } catch (Exception e) {
            fail("Shouldn't get other exception");
        }
    }
}
