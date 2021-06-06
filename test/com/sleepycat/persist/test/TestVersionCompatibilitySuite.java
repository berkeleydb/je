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

package com.sleepycat.persist.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test that the catalog and data records created with a different version of
 * the DPL are compatible with this version.  This test is run as follows:
 *
 * 1) Run EvolveTest with version X of JE.  For example:
 *
 *    cd /jeX
 *    ant -Dtestcase=com.sleepycat.persist.test.EvolveTest test
 * or
 *    ant -Dsuite=persist/test test
 * or
 *    ant test
 *
 * Step (1) leaves the log files from all tests in the testevolve directory.
 *
 * 2) Run TestVersionCompatibility with version Y of JE, passing the JE
 * testevolve directory from step (1).  For example:
 *
 *    cd /jeY
 *    ant -Dtestcase=com.sleepycat.persist.test.TestVersionCompatibilitySuite \
 *        -Dunittest.testevolvedir=/jeX/build/test/testevolve \
 *        test
 *
 * Currently there are 2 sets of X and Y that can be tested, one set for the
 * CVS branch and one for the CVS trunk:
 *
 *  CVS     Version X   Version Y
 *  branch  je-3_2_56   je-3_2_57 or greater
 *  trunk   je-3_3_41   je-3_3_42 or greater
 *
 * This test is not run along with the regular JE test suite run, because the
 * class name does not end with Test.  It must be run separately as described
 * above.
 */

@RunWith(Suite.class)
@SuiteClasses({TestVersionCompatibility.class, EvolveTest.class})
public class TestVersionCompatibilitySuite {
    /*
     * Run TestVersionCompatibility tests first to check previously evolved
     * data without changing it.  Then run the EvolveTest to try evolving
     * it.
     */
}
