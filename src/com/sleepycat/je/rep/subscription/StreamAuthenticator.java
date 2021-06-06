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


package com.sleepycat.je.rep.subscription;

/**
 * Object represents an interface to authenticate stream consumer and check its
 * access privilege.
 */
public interface StreamAuthenticator {

    /**
     * Specifies the login token.
     *
     * @param token login token in bytes
     */
    void setToken(byte[] token);

    /**
     * Specifies the table Ids. The table Ids are passed from stream consumer
     * as string form. Each of the table id strings uniquely identifies a
     * subscribed table.
     *
     * @param tableIds set of subscribed table id strings
     */
    void setTableIds(String[] tableIds);

    /**
     * Returns whether the current token is valid.
     *
     * @return true if currently stored token is valid, false otherwise.
     */
    boolean authenticate();

    /**
     * Returns whether the current token is valid and grants access to the
     * current table Ids.
     *
     * @return true if owner of current token is valid and has enough
     * privileges to stream updates from subscribed tables, false otherwise.
     */
    boolean checkAccess();

    /**
     * Gets the time stamp of last check. Implementation of this interface
     * shall remember the time stamp of each check, regardless of the check
     * result. It shall return 0 if no previous check has been performed. The
     * caller can determine if a security check has been performed in the
     * last certain milliseconds by subtracting this value from the current
     * time.
     *
     * @return the time stamp of last check in milliseconds, 0 if no previous
     * check has been performed.
     */
    long getLastCheckTimeMs();
}
