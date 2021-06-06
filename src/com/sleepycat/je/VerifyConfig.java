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

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.utilint.PropUtil;

/**
 * Specifies the attributes of a verification operation.
 */
public class VerifyConfig implements Cloneable {

    /*
     * For internal use, to allow null as a valid value for the config
     * parameter.
     */
    public static final VerifyConfig DEFAULT = new VerifyConfig();

    private boolean propagateExceptions = false;
    private boolean aggressive = false;
    private boolean printInfo = false;
    private PrintStream showProgressStream = null;
    private int showProgressInterval = 0;
    private boolean verifySecondaries = true;
    private boolean verifyDataRecords = false;
    private boolean verifyObsoleteRecords = false;
    private int batchSize = 1000;
    private int batchDelayMs = 10;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public VerifyConfig() {
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to propagate exceptions found during verification.
     *
     * <p>By default this is false and exception information is printed to
     * System.out for notification but does not stop the verification activity,
     * which continues on for as long as possible.</p>
     *
     * <p>Note: Currently this method has no effect.</p>
     *
     * @param propagate If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to propagate
     * exceptions found during verification.
     *
     * @return this
     */
    public VerifyConfig setPropagateExceptions(boolean propagate) {
        setPropagateExceptionsVoid(propagate);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setPropagateExceptionsVoid(boolean propagate) {
        propagateExceptions = propagate;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to propagate exceptions found during
     * verification.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to propagate exceptions found during
     * verification.
     */
    public boolean getPropagateExceptions() {
        return propagateExceptions;
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to perform fine granularity consistency checking that
     * includes verifying in memory constructs.
     *
     * <p>This level of checking should only be performed while the database
     * environment is quiescent.</p>
     *
     * <p>By default this is false.</p>
     *
     * <p>Note: Currently, enabling aggressive verification has no additional
     * effect.</p>
     *
     * @param aggressive If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to perform fine
     * granularity consistency checking that includes verifying in memory
     * constructs.
     *
     * @return this
     */
    public VerifyConfig setAggressive(boolean aggressive) {
        setAggressiveVoid(aggressive);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setAggressiveVoid(boolean aggressive) {
        this.aggressive = aggressive;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to perform fine granularity consistency
     * checking that includes verifying in memory constructs.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to perform fine granularity consistency
     * checking that includes verifying in memory constructs.
     */
    public boolean getAggressive() {
        return aggressive;
    }

    /**
     * Configures {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} to print basic verification information.
     *
     * <p>Information is printed to the {@link #getShowProgressStream()} if it
     * is non-null, and otherwise to System.err.</p>
     *
     * <p>By default this is false.</p>
     *
     * @param printInfo If set to true, configure {@link
     * com.sleepycat.je.Environment#verify Environment.verify} and {@link
     * com.sleepycat.je.Database#verify Database.verify} to print basic
     * verification information.
     *
     * @return this
     */
    public VerifyConfig setPrintInfo(boolean printInfo) {
        setPrintInfoVoid(printInfo);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setPrintInfoVoid(boolean printInfo) {
        this.printInfo = printInfo;
    }

    /**
     * Returns true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to print basic verification information.
     *
     * <p>This method may be called at any time during the life of the
     * application.</p>
     *
     * @return true if the {@link com.sleepycat.je.Environment#verify
     * Environment.verify} and {@link com.sleepycat.je.Database#verify
     * Database.verify} are configured to print basic verification information.
     */
    public boolean getPrintInfo() {
        return printInfo;
    }

    /**
     * Configures the verify operation to display progress to the PrintStream
     * argument.  The accumulated statistics will be displayed every N records,
     * where N is the value of showProgressInterval.
     *
     * @return this
     */
    public VerifyConfig setShowProgressStream(PrintStream showProgressStream) {
        setShowProgressStreamVoid(showProgressStream);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setShowProgressStreamVoid(PrintStream showProgressStream) {
        this.showProgressStream = showProgressStream;
    }

    /**
     * Returns the PrintStream on which the progress messages will be displayed
     * during long running verify operations.
     */
    public PrintStream getShowProgressStream() {
        return showProgressStream;
    }

    /**
     * When the verify operation is configured to display progress the
     * showProgressInterval is the number of LNs between each progress report.
     *
     * @return this
     */
    public VerifyConfig setShowProgressInterval(int showProgressInterval) {
        setShowProgressIntervalVoid(showProgressInterval);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setShowProgressIntervalVoid(int showProgressInterval) {
        this.showProgressInterval = showProgressInterval;
    }

    /**
     * Returns the showProgressInterval value, if set.
     */
    public int getShowProgressInterval() {
        return showProgressInterval;
    }

    /**
     * Configures verification to verify secondary database integrity. This is
     * equivalent to verifying secondaries in the background Btree verifier,
     * when {@link EnvironmentConfig#VERIFY_SECONDARIES} is set to true.
     *
     * <p>By default this is true.</p>
     *
     * @return this
     */
    public VerifyConfig setVerifySecondaries(boolean verifySecondaries) {
        setVerifySecondariesVoid(verifySecondaries);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setVerifySecondariesVoid(boolean verifySecondaries) {
        this.verifySecondaries = verifySecondaries;
    }

    /**
     * Returns the verifySecondaries value.
     */
    public boolean getVerifySecondaries() {
        return verifySecondaries;
    }

    /**
     * Configures verification to read and verify the leaf node (LN) of a
     * primary data record. This is equivalent to verifying data records in the
     * background Btree verifier, when
     * {@link EnvironmentConfig#VERIFY_DATA_RECORDS} is set to true.
     *
     * <p>By default this is false.</p>
     *
     * @return this
     */
    public VerifyConfig setVerifyDataRecords(boolean verifyDataRecords) {
        setVerifyDataRecordsVoid(verifyDataRecords);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setVerifyDataRecordsVoid(boolean verifyDataRecords) {
        this.verifyDataRecords = verifyDataRecords;
    }

    /**
     * Returns the verifyDataRecords value.
     */
    public boolean getVerifyDataRecords() {
        return verifyDataRecords;
    }

    /**
     * @hidden
     * Configures verification to verify the obsolete record metadata. This is
     * equivalent to verifying obsolete metadata in the background Btree
     * verifier, when {@link EnvironmentConfig#VERIFY_OBSOLETE_RECORDS} is set
     * to true.
     *
     * <p>By default this is false.</p>
     *
     * @return this
     */
    public VerifyConfig setVerifyObsoleteRecords(
        boolean verifyObsoleteRecords) {
        setVerifyObsoleteRecordsVoid(verifyObsoleteRecords);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setVerifyObsoleteRecordsVoid(boolean verifyObsoleteRecords) {
        this.verifyObsoleteRecords = verifyObsoleteRecords;
    }

    /**
     * @hidden
     * Returns the verifyObsoleteRecords value.
     */
    public boolean getVerifyObsoleteRecords() {
        return verifyObsoleteRecords;
    }

    /**
     * Configures the number of records verified per batch. In order to give
     * database remove/truncate the opportunity to execute, records are
     * verified in batches and there is a {@link #setBatchDelay delay}
     * between batches.
     *
     * <p>By default the batch size is 1000.</p>
     *
     * <p>Note that when using the {@link EnvironmentConfig#ENV_RUN_VERIFIER
     * background data verifier}, the batch size is
     * {@link EnvironmentConfig#VERIFY_BTREE_BATCH_SIZE}.</p>
     *
     * @return this
     */
    public VerifyConfig setBatchSize(int batchSize) {
        setBatchSizeVoid(batchSize);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setBatchSizeVoid(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Returns the batchSize value.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Configures the delay between batches. In order to give database
     * remove/truncate the opportunity to execute, records are verified in
     * {@link #setBatchSize batches} and there is a delay between batches.
     *
     * <p>By default the batch delay is 10 ms.</p>
     *
     * <p>Note that when using the {@link EnvironmentConfig#ENV_RUN_VERIFIER
     * background data verifier}, the batch delay is
     * {@link EnvironmentConfig#VERIFY_BTREE_BATCH_DELAY}.</p>
     *
     * @param delay the delay between batches.
     *
     * @param unit the {@code TimeUnit} of the delay value. May be
     * null only if delay is zero.
     *
     * @return this
     */
    public VerifyConfig setBatchDelay(long delay, TimeUnit unit) {
        setBatchDelayVoid(delay, unit);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setBatchDelayVoid(long delayDuration, TimeUnit unit) {
        batchDelayMs = PropUtil.durationToMillis(delayDuration, unit);
    }

    /**
     * Returns the batch delay.
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     */
    public long getBatchDelay(TimeUnit unit) {
        return PropUtil.millisToDuration(batchDelayMs, unit);
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public VerifyConfig clone() {
        try {
            return (VerifyConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        // TODO: add new properties here.
        StringBuilder sb = new StringBuilder();
        sb.append("propagateExceptions=").append(propagateExceptions);
        return sb.toString();
    }
}
