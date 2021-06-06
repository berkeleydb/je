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

package com.sleepycat.je.jca.ra;

import java.io.Serializable;

import javax.resource.Referenceable;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.TransactionConfig;

/**
 * An application may obtain a {@link JEConnection} in this manner:
 * <pre>
 *    InitialContext iniCtx = new InitialContext();
 *    Context enc = (Context) iniCtx.lookup("java:comp/env");
 *    Object ref = enc.lookup("ra/JEConnectionFactory");
 *    JEConnectionFactory dcf = (JEConnectionFactory) ref;
 *    JEConnection dc = dcf.getConnection(envDir, envConfig);
 * </pre>
 *
 * See &lt;JEHOME&gt;/examples/jca/HOWTO-**.txt and
 * &lt;JEHOME&gt;/examples/jca/simple/SimpleBean.java for more information
 * on how to build the resource adapter and use a JEConnection.
 */
public interface JEConnectionFactory
    extends Referenceable, Serializable {

    public JEConnection getConnection(String jeRootDir,
                                      EnvironmentConfig envConfig)
        throws JEException;

    public JEConnection getConnection(String jeRootDir,
                                      EnvironmentConfig envConfig,
                                      TransactionConfig transConfig)
        throws JEException;
}
