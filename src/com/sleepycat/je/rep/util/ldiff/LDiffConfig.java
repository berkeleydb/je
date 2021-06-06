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

package com.sleepycat.je.rep.util.ldiff;

public class LDiffConfig {
    private static final int DEFAULT_BLOCK_SIZE = 1 << 13; // 8k
    private static final int DEFAULT_MAX_ERRORS = 0;

    private int maxErrors = DEFAULT_MAX_ERRORS;
    private boolean diffAnalysis = false;
    private int blockSize = DEFAULT_BLOCK_SIZE;
    private boolean waitIfBusy = false;
    private int maxConnectionAttempts = 1;
    private int reconnectDelay = 0;
    public boolean verbose = false;

    /**
     * Return the maximum number of errors to analyze before ending the LDiff
     * operation.
     *
     * @return the maximum number of errors to analyze before throwing
     * MismatchException.
     */
    public int getMaxErrors() {
        return maxErrors;
    }

    /**
     * Configure the maximum number of errors to be analyzed before ending the
     * LDiff operation. A value of zero forces the algorithm to run to
     * completion. The default value is 0.
     *
     * @param max the maximum number of errors to be analyzed before ending the
     * LDiff operation.
     */
    public LDiffConfig setMaxErrors(int max) {
        setMaxErrorsVoid(max);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxErrorsVoid(int max) {
        this.maxErrors = max;
    }

    /**
     * Return whether an LDiff object will provide detailed analysis about diff
     * failures.
     *
     * @return true if an LDiff object will provide detailed analysis about
     * diff failures.
     */
    public boolean getDiffAnalysis() {
        return diffAnalysis;
    }

    /**
     * Configure an LDiff object to provide detailed analysis about diff
     * failures. The default value is false.
     *
     * @param analysis if true, provides detailed analysis about the reason why
     * the diff failed. The detailed analysis can be time consuming.
     */
    public LDiffConfig setDiffAnalysis(boolean analysis) {
        setDiffAnalysisVoid(analysis);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setDiffAnalysisVoid(boolean analysis) {
        diffAnalysis = analysis;
    }

    /**
     * Return the number of records to include in each block analyzed by the
     * LDiff operation.
     *
     * @return the number of records to include in each block analyzed by the
     * LDiff operation.
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Configure the number of records to include in each block analyzed by the
     * LDiff operation. The default is 10240.
     *
     * @param size the number of records to include in each block analyzed by
     * the LDiff operation.
     */
    public LDiffConfig setBlockSize(int size) {
        setBlockSizeVoid(size);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setBlockSizeVoid(int size) {
        blockSize = size;
    }

    /**
     * Return whether or not the operation will wait for the remote service to
     * become available if the remote service is busy.
     *
     * @return true if the LDiff operation will block until the remote service
     * becomes available
     */
    public boolean getWaitIfBusy() {
        return waitIfBusy;
    }

    /**
     * Return the maximum number of times the operation will attempt to connect
     * to the remote service before aborting.  A value of -1 means the operation
     * will never abort.
     *
     * @return the maximum number of times the operation will attempt to connect
     * to the remote service before aborting.
     */
    public int getMaxConnectionAttempts() {
        return maxConnectionAttempts;
    }

    /**
     * Return the delay, in milliseconds, between reconnect attempts.
     *
     * @return the amount of time, in milliseconds, between reconnection
     * attempts
     */
    public int getReconnectDelay() {
        return reconnectDelay;
    }

    /**
     * Configure whether or not the operation should wait for the remote
     * service to become available, if the remote service is busy.
     *
     * @param wait if true, the LDiff operation will block until the remote
     * node is available
     * @param maxAttempts the number of times to attempt connecting to
     * the service before aborting.  Pass -1 to never abort.
     * @param delay the number of milliseconds to wait between connection
     * attempts.
     */
    public LDiffConfig setWaitIfBusy(boolean wait, int maxAttempts, int delay) {
        waitIfBusy = wait;
        maxConnectionAttempts = maxAttempts;
        reconnectDelay = delay;
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setWaitIfBusyVoid(boolean wait) {
        this.waitIfBusy = wait;
    }
    
    /**
     * @hidden
     * For the completement of setter methods.
     */
    public LDiffConfig setMaxConnectionAttempts(int maxAttempts) {
        setMaxConnectionAttemptsVoid(maxAttempts);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setMaxConnectionAttemptsVoid(int maxAttempts) {
        this.maxConnectionAttempts = maxAttempts;
    }
    
    /**
     * @hidden
     * For the completement of setter methods.
     */
    public LDiffConfig setReconnectDelay(int delay) {
        setReconnectDelayVoid(delay);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setReconnectDelayVoid(int delay) {
        this.reconnectDelay = delay;
    }

    /**
     * Return whether or not the operation will output information on its
     * success or failure.
     *
     * @return true if the operation will output information
     */
    public boolean getVerbose() {
        return verbose;
    }

    /**
     * Configure whether or not the operation will output information on its
     * success or failure.
     *
     * @param verbose if true, the LDiff operation will output information
     * as it compares databases
     */
    public LDiffConfig setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setVerboseVoid(boolean verbose) {
        this.verbose = verbose;
    }
}
