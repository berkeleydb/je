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

package com.sleepycat.collections;

import com.sleepycat.util.ExceptionUnwrapper;

/**
 * The interface implemented to perform the work within a transaction.
 * To run a transaction, an instance of this interface is passed to the
 * {@link TransactionRunner#run} method.
 *
 * @author Mark Hayes
 */
public interface TransactionWorker {

    /**
     * Perform the work for a single transaction.
     *
     * @throws Exception the exception to be thrown to the caller of
     * {@link TransactionRunner#run(TransactionWorker)}. The exception will
     * first be unwrapped by calling {@link ExceptionUnwrapper#unwrap}, and the
     * transaction will be aborted.
     *
     * @see TransactionRunner#run
     */
    void doWork()
        throws Exception;
}
