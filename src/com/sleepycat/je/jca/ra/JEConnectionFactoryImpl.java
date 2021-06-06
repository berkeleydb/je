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

import java.io.File;

import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.TransactionConfig;

public class JEConnectionFactoryImpl implements JEConnectionFactory {

    private static final long serialVersionUID = 410682596L;

    /*
     * These are not transient because SJSAS seems to need to serialize
     * them when leaving them in JNDI.
     */
    private final /* transient */ ConnectionManager manager;
    private final /* transient */ ManagedConnectionFactory factory;
    private Reference reference;

    /* Make the constructor public for serializability testing. */
    public JEConnectionFactoryImpl(ConnectionManager manager,
                            ManagedConnectionFactory factory) {
        this.manager = manager;
        this.factory = factory;
    }

    public JEConnection getConnection(String jeRootDir,
                                      EnvironmentConfig envConfig)
        throws JEException {

        return getConnection(jeRootDir, envConfig, null);
    }

    public JEConnection getConnection(String jeRootDir,
                                      EnvironmentConfig envConfig,
                                      TransactionConfig transConfig)
        throws JEException {

        JEConnection dc = null;
         JERequestInfo jeInfo =
             new JERequestInfo(new File(jeRootDir), envConfig, transConfig);
        try {
            dc = (JEConnection) manager.allocateConnection(factory, jeInfo);
        } catch (ResourceException e) {
            throw new JEException("Unable to get Connection: " + e);
        }

        return dc;
    }

    public void setReference(Reference reference) {
        this.reference = reference;
    }

    public Reference getReference() {
        return reference;
    }
}
