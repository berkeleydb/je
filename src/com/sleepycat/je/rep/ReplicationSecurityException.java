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

package com.sleepycat.je.rep;

import com.sleepycat.je.DatabaseException;

/**
 * Exception that is thrown when a security failure has occurred which
 * may terminate the current replication stream. When it is raised, the
 * replication stream consumer is no longer eligible to consume the stream.
 * <p>
 *
 * This exception covers following security failure during streaming:
 * <ul>
 * <li> The user attempted to contact the feeder of a secure store without
 * authenticating. It will be raised during client does service handshake
 * with server in this case;
 *
 * <li> There was a problem authenticating the stream client during stream, say
 * because the token provided by client is no longer valid during streaming;
 *
 * <li> Stream client attempted to perform an operation that they were not
 * authorized to perform, say, the stream client is trying to stream data
 * from a table that she is not eligible to read.
 *
 * </ul>
 * @hidden For internal use only
 */
public class ReplicationSecurityException extends DatabaseException {

    private static final long serialVersionUID = 1;

    /* consumer of replication stream */
    private final String consumer;

    public ReplicationSecurityException(String msg,
                                        String consumer,
                                        Throwable cause) {
        super(msg, cause);
        this.consumer = consumer;
    }

    /**
     * Gets the replication stream consumer name
     *
     * @return the replication stream consumer name
     */
    public String getConsumer() {
        return consumer;
    }
}
