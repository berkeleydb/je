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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests to test different EntryRequest type. Each test verifies
 * behavior of one EntryRequest type if the start VLSN to subscribe the
 * replication stream has been cleaned by cleaner and no longer available in
 * the VLSN index.
 */
public class EntryRequestTypeTest extends SubscriptionTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = 1 + numReplicas + (hasMonitor ? 1 : 0);
        super.setUp();
        keys = new ArrayList<>();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(),
                                                  "EntryRequestTypeTest");
        /* to be created in each test */
        subscription = null;
        monitor = null;
    }

    @Override
    @After
    public void tearDown()
        throws Exception {
        if (subscription != null) {
            subscription.shutdown();
        }
        subscription = null;

        if (monitor != null) {
            monitor.shutdown();
        }
        super.tearDown();
    }


    /**
     * Without any cleaning, all modes should succeed.
     *
     * @throws Exception if test fails
     */
    @Test
    public void testNoCleaning() throws Exception {

        final boolean cleanLog = false;
        createEnv(cleanLog);

        /* all modes should succeed */
        for (EntryRequestType mode : EntryRequestType.values()) {
            logger.info("Test stream mode: " + mode);
            testLow(mode, new TestCallback());
            testInRange(mode, new TestCallback());
            testHigh(mode, new TestCallback());
            logger.info("Done test mode: " + mode + "\n");
        }
    }

    /**
     * With log cleaning
     *
     * @throws Exception if test fails
     */
    @Test
    public void testCleaning() throws Exception {

        final boolean cleanLog = true;
        createEnv(cleanLog);

        /* ensure log cleaned */
        final VLSN lower = getVLSNRange().getFirst();
        assertTrue("Expect log cleaned", lower.compareTo(VLSN.FIRST_VLSN) > 0);

        /* now and available modes should always succeed */
        final EntryRequestType[] modes =
            new EntryRequestType[]{EntryRequestType.NOW,
                EntryRequestType.AVAILABLE};
        for (EntryRequestType mode : modes) {
            logger.info("Test stream mode: " + mode);
            testLow(mode, new TestCallback());
            testInRange(mode, new TestCallback());
            testHigh(mode, new TestCallback());
            logger.info("Done test mode: " + mode + "\n");
        }

        /* default mode */
        final EntryRequestType mode = EntryRequestType.DEFAULT;
        /* in range and high should be good */
        testInRange(mode, new TestCallback());
        testHigh(mode, new TestCallback());

        /* low start vlsn should raise ILE */
        try {
            testLow(mode, new TestCallback());
            fail("In default mode, subscription should fail if requested vlsn" +
                 " is lower than the range");
        } catch (InsufficientLogException exp) {
            logger.info("Expected exception: " + exp.getMessage());
        } finally {
            subscription.shutdown();
        }
    }

    private void testLow(EntryRequestType mode, TestCallback cbk)
        throws Exception {

        logger.info("Test low start, mode: " + mode);
        logger.info("VLSN index range: " + getVLSNRange());

        final VLSN reqVLSN = VLSN.FIRST_VLSN;
        final VLSN expVLSN;
        switch (mode) {
            case DEFAULT:
                expVLSN = reqVLSN;
                break;
            case AVAILABLE:
                expVLSN = getVLSNRange().getFirst();
                break;
            case NOW:
                expVLSN = getVLSNRange().getLast();
                break;
            default:
                expVLSN = VLSN.NULL_VLSN;
        }
        testSub(mode, cbk, reqVLSN, expVLSN);
    }

    private void testInRange(EntryRequestType mode, TestCallback cbk)
        throws Exception {

        logger.info("Test in-range start, mode: " + mode);
        logger.info("VLSN index range: " + getVLSNRange());

        final VLSN lower = getVLSNRange().getFirst();
        final VLSN upper = getVLSNRange().getLast();
        final long mid = (lower.getSequence() + upper.getSequence()) / 2;
        final VLSN reqVLSN = new VLSN(mid);
        final VLSN expVLSN;
        switch (mode) {
            case DEFAULT:
                expVLSN = reqVLSN;
                break;
            case AVAILABLE:
                expVLSN = reqVLSN;
                break;
            case NOW:
                expVLSN = upper;
                break;
            default:
                expVLSN = VLSN.NULL_VLSN;
        }
        testSub(mode, cbk, reqVLSN, expVLSN);
    }

    private void testHigh(EntryRequestType mode, TestCallback cbk)
        throws Exception {

        logger.info("Test high start, mode: " + mode);
        logger.info("VLSN index range: " + getVLSNRange());

        final VLSN reqVLSN = new VLSN(Long.MAX_VALUE);
        final VLSN expVLSN;
        switch (mode) {
            case DEFAULT:
                expVLSN = getVLSNRange().getLastSync();
                break;
            case AVAILABLE:
                expVLSN = getVLSNRange().getLast();
                break;
            case NOW:
                expVLSN = getVLSNRange().getLast();
                break;
            default:
                expVLSN = VLSN.NULL_VLSN;
        }
        testSub(mode, cbk, reqVLSN, expVLSN);
    }

    private void testSub(EntryRequestType mode, TestCallback cbk,
                         VLSN requestVLSN, VLSN expectedVLSN)
        throws Exception {

        createSubscription(mode, cbk);

        /* start subscription at high VLSN */
        logger.info("Req VLSN: " + requestVLSN +
                           ", exp VLSN " + expectedVLSN);
        subscription.start(requestVLSN);
        try {
            waitFor(new PollCondition(TEST_POLL_INTERVAL_MS,
                                      TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final VLSN vlsn = cbk.getFirstVLSN();
                    return vlsn.equals(expectedVLSN);
                }
            });
        } catch (TimeoutException e) {
            failTimeout();
        }
        logger.info("Subscription successfully started");
        subscription.shutdown();
    }

    private void createSubscription(EntryRequestType mode, TestCallback cbk)
        throws Exception {

        /* create a subscription */
        final ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        final SubscriptionConfig config = createSubConfig(masterEnv, false);
        config.setStreamMode(mode);
        config.setCallback(cbk);
        config.setFeederFilter(new TestFilter());
        subscription = new Subscription(config, logger);
    }


    /* gets a vlsn in available range of index */
    private VLSNRange getVLSNRange() {
        final ReplicatedEnvironment master = repEnvInfo[0].getEnv();
        final VLSNIndex vlsnIndex =
            RepInternal.getNonNullRepImpl(master).getVLSNIndex();
        return vlsnIndex.getRange();
    }

    private void createEnv(boolean cleaning)
        throws Exception {

        /* Use small log files so we can set a small disk limit. */
        repEnvInfo[0].getEnvConfig().setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX, String.valueOf(1L << 20));

        /* create and verify a replication group */
        prepareTestEnv();

        /* populate some data and verify */
        final ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

        if (cleaning) {
            /* cause VLSN range being to advance past zero */
            advanceVLSNRange(masterEnv);
        }

        logger.info("Env created, log cleaning? " + cleaning);
    }

    /* Wait for test done */
    private void waitFor(final PollCondition condition)
        throws TimeoutException {
        boolean success = condition.await();
        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    class TestCallback implements SubscriptionCallback {

        private VLSN firstVLSN;

        TestCallback() {
            firstVLSN = VLSN.NULL_VLSN;
        }

        @Override
        public void processPut(VLSN vlsn, byte[] key, byte[] value,
                               long txnId) {
            processVLSN(vlsn);
        }

        @Override
        public void processDel(VLSN vlsn, byte[] key, long txnId) {
            processVLSN(vlsn);
        }

        @Override
        public void processCommit(VLSN vlsn, long txnId) {
            processVLSN(vlsn);
        }

        @Override
        public void processAbort(VLSN vlsn, long txnId) {
            processVLSN(vlsn);
        }

        @Override
        public void processException(Exception exception) {
        }

        VLSN getFirstVLSN() {
            return firstVLSN;
        }

        private void processVLSN(VLSN vlsn) {
            /* record the first VLSN received from feeder */
            if (firstVLSN.isNull()) {
                firstVLSN = vlsn;
            }
        }

    }

    /* no op filter to ensure VLSN=1 is not filtered */
    static class TestFilter implements FeederFilter, Serializable {

        private static final long serialVersionUID = 1L;

        TestFilter() {
            super();
        }

        @Override
        public OutputWireRecord execute(OutputWireRecord record,
                                        RepImpl repImpl) {
            return record;
        }

        @Override
        public String[] getTableIds() {
            return new String[0];
        }
    }
}
