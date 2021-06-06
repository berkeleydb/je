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
package com.sleepycat.je.rep.util;

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;

import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.util.test.TestBase;

public abstract class ServiceDispatcherTestBase extends TestBase {

    protected ServiceDispatcher dispatcher = null;
    private static final int TEST_PORT = 5000;
    protected InetSocketAddress dispatcherAddress;

    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        dispatcherAddress = new InetSocketAddress("localhost", TEST_PORT);
        dispatcher = new ServiceDispatcher(
            dispatcherAddress,
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig()));
        dispatcher.start();
    }

    @After
    public void tearDown()
        throws Exception {

        dispatcher.shutdown();
        dispatcher = null;
    }
}
