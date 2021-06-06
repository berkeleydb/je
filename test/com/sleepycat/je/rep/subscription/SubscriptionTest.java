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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.TestHookAdapter;
import com.sleepycat.je.utilint.VLSN;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * test subscription API to receive a replication stream
 * from a start VLSN
 */
public class SubscriptionTest extends SubscriptionTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = 1 + numReplicas + (hasMonitor ? 1 : 0);
        super.setUp();
        keys = new ArrayList<>();
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(),
                "SubscriptionTest");
        logger.setLevel(Level.FINE);

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
     * Testcase of subscribing from the very beginning of VLSN. Assume the
     * VLSN index should have all entries available given the small set of
     * test data. This is the test case of basic subscription api usage.
     *
     * @throws Exception if test fails
     */
    @Test
    public void testSubscriptionBasic() throws Exception {

        /* a slightly bigger db */
        numKeys =  1024*100;

        testGroupUUIDhelper(false);
    }

    /**
     * Similar test case to testSubscriptionBasic except that a rep group
     * uuid is tested in configuration. This is to test that subscription can
     * only succeed if the specified group uuid matches that of feeder.
     *
     * @throws Exception if test fails
     */
    @Test
    public void testSubscriptionGroupUUID() throws Exception {

        /* a slightly bigger db */
        numKeys =  1024*100;

        /* same test as above test but use a matching group uuid */
        testGroupUUIDhelper(true);

        /* now test a invalid random group uuid, subscription should fail */
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        SubscriptionConfig config = createSubConfig(masterEnv, true);
        config.setGroupUUID(UUID.randomUUID());
        Subscription failSubscription = new Subscription(config, logger);
         /* start streaming from the very beginning, should succeed */
        try {
            failSubscription.start();
            fail("Did not see exception due to mismatch group uuid");
        } catch (InternalException e) {
            logger.info("Expected exception due to mismatch group uuid: " +
                        e.getMessage());
        }

        failSubscription.shutdown();
    }

    /**
     * Test that the dummy env created by subscription is a secondary node
     *
     * @throws Exception if test fails
     */
    @Test
    public void testDummyEnvNodeType() throws Exception {

        /* a slightly bigger db */
        numKeys =  100;
         /* turn off data cleaner on master */
        turnOffMasterCleaner();

        /* create and verify a replication group */
        prepareTestEnv();

        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

         /* create a subscription */
        SubscriptionConfig config = createSubConfig(masterEnv, false);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeys);
        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);
    
        final ReplicatedEnvironment dummyEnv = subscription.getDummyRepEnv();
        assert(!dummyEnv.getRepConfig().getNodeType().isElectable());
        assert(dummyEnv.getRepConfig().getNodeType().isSecondary());
    }
    
    /**
     * Testcase of subscribing from a particular VLSN. Assume the
     * VLSN index should have all entries available given the small set of
     * test data
     *
     * @throws Exception if test fails
     */
    @Test
    public void testSubscriptionFromVLSN() throws Exception {

        /* emtpy security property */
        testSubscriptionFromVLSNHelper(new Properties());
    }

    /**
     * Tests Subscription configured with SSL. It is similar to above test
     * testSubscriptionFromVLSN except the env is configured with security
     * property
     *
     * @throws Exception if test fails
     */
    @Test
    public void testSubscriptionSSL() throws Exception {

        /* set ssl in the config. */
        final Properties sslProps = new Properties();
        RepTestUtils.setUnitTestSSLProperties(sslProps);
        final ReplicationSSLConfig sslConf = new ReplicationSSLConfig(sslProps);

        /* dump security properties */
        for (Object key : sslProps.keySet()) {
            final String trace = key + ": " + sslProps.get(key);
            logger.info(trace);
        }

        /* update each rep env with ssl config */
        for (RepTestUtils.RepEnvInfo envInfo : repEnvInfo) {
            ReplicationConfig rc = envInfo.getRepConfig();
            rc.setRepNetConfig(sslConf);
        }

        testSubscriptionFromVLSNHelper(sslProps);
    }

    /**
     * Testcase of subscribing from a NULL VLSN.
     *
     * @throws Exception if test fails
     */
    @Test
    public void testInvalidStartVLSN() throws Exception {

        numKeys = 200;

        /* turn off data cleaner on master */
        turnOffMasterCleaner();

        /* the start VLSN to subscribe*/
        final VLSN startVLSN = VLSN.NULL_VLSN;
        /* number of transactions we subscribe */
        final int numKeysToStream = 10;

        /* create and verify a replication group */
        prepareTestEnv();

        /* populate some data and verify */
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

        /* create a subscription */
        SubscriptionConfig config = createSubConfig(masterEnv, false);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeysToStream);
        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);

        logger.info("subscription created at home " +
                    config.getSubscriberHome() +
                    ", start streaming " + numKeys + " items from feeder");

        try {
            subscription.start(startVLSN);
            fail("Expect IllegalArgumentException raised from subscription");
        } catch (IllegalArgumentException iae) {
            /* expected exception */
            logger.info("Expected exception " + iae.getMessage());
        } finally {
            subscription.shutdown();
        }
    }

    /**
     * Testcase that if the start VLSN to subscribe the replication stream
     * has been cleaned by cleaner and no longer available in the VLSN
     * index at the time of subscription. We expect an entry-not-found message
     * raised in sync-up phase
     *
     * @throws Exception if test fails
     */
    @Test
    public void testSubscriptionUnavailableVLSN() throws Exception {

        /* Use small log files so we can set a small disk limit. */
        repEnvInfo[0].getEnvConfig().setConfigParam(
            EnvironmentConfig.LOG_FILE_MAX, String.valueOf(1L << 20));

        /* create and verify a replication group */
        prepareTestEnv();

        /* populate some data and verify */
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

        /* cause VLSN range being to advance past zero */
        advanceVLSNRange(masterEnv);

        /* create a subscription */
        SubscriptionConfig config = createSubConfig(masterEnv, false);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeys);
        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);

        /* start subscription at VLSN 0 and expect InsufficientLogException */
        try {
            subscription.start();
            fail("Expect InsufficientLogException since start VLSN is not " +
                 "available at feeder");
        } catch (InsufficientLogException ile){
            logger.info("Expected InsufficientLogException: " +
                        ile.getMessage());
        } finally {
            subscription.shutdown();
        }
    }

    /**
     * Testcase that subscription callback is able to process commits and
     * aborts from feeder.
     */
    @Test
    public void testTxnCommitsAndAborts() throws Exception {

        numKeys = 1024;
        final int numAborts = 5;

        /* create and verify a replication group */
        prepareTestEnv();
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);
        /* now create some aborts */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setDurability
            (new Durability(Durability.SyncPolicy.NO_SYNC,
                            Durability.SyncPolicy.NO_SYNC,
                            Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));
        try (Database db = masterEnv.openDatabase(null, dbName, dbConfig)) {
            for (int i = 1; i <= numAborts; i++) {
                IntegerBinding.intToEntry(i, key);
                Transaction txn = masterEnv.beginTransaction(null, txnConfig);
                db.put(txn, key, data);
                txn.abort();
            }
        }

        /* start streaming from the very beginning, should succeed */
        SubscriptionConfig config = createSubConfig(masterEnv, false);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeys);
        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);
        subscription.start();

        try {
            waitForTestDone(testCbk);
        } catch (TimeoutException e) {
            failTimeout();
        }

        /* expect multiple commits */
        assertTrue("expect commits from loading " + numKeys + " keys",
                   (testCbk.getNumCommits() > 0));
        assertEquals("expect aborts", numAborts, testCbk.getNumAborts());


        /* shutdown test verify we receive all expected keys */
        subscription.shutdown();
    }

    /**
     * Testcase that subscription callback is able to process an exception.
     */
    @Test
    public void testExceptionHandling() throws Exception {

        numKeys = 100;

        /* create and verify a replication group */
        prepareTestEnv();
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

        /* start streaming from the very beginning, should succeed */
        SubscriptionConfig config = createSubConfig(masterEnv, false);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeys,
                                         true); /* allow exception */
        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);
        subscription.start();

        try {
            waitForTestDone(testCbk);
        } catch (TimeoutException e) {
            failTimeout();
        }

        /* now inject an exception into queue */
        testCbk.resetTestDone();
        final String token = "test internal exception";
        Exception exp = new InternalException(token);
        subscription.setExceptionHandlingTestHook(new ExceptionTestHook(exp));

        try {
            waitForTestDone(testCbk);
        } catch (TimeoutException e) {
            failTimeout();
        }

        /* verify injected exception is processed in callback */
        if (testCbk.getNumExceptions() != 1) {
            testCbk.getFirstException().printStackTrace();
            testCbk.getLastException().printStackTrace();
        }
        assertEquals("Expect one exception", 1, testCbk.getNumExceptions());
        final Exception lastExp = testCbk.getLastException();
        assertTrue("Expect InternalException",
                   (lastExp != null) && (lastExp instanceof InternalException));
        assertTrue("Expect same token", token.equals(exp.getMessage()));
        subscription.shutdown();
    }

    /* Wait for test done */
    private void waitForTestDone(final SubscriptionTestCallback callBack)
            throws TimeoutException {

        boolean success = new PollCondition(TEST_POLL_INTERVAL_MS,
                                            TEST_POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
               return callBack.isTestDone();
            }
        }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    /* Verify received correct test data from feeder */
    private void verifyTestResults(SubscriptionTestCallback mycbk) {
        List<byte[]> receivedKeys = mycbk.getAllKeys();
        int numKeys = keys.size();

        assertTrue("expect some commits", (mycbk.getNumCommits() > 0));
        assertTrue("expect no aborts", (mycbk.getNumAborts() == 0));

        logger.info("number of keys to verify: " + numKeys);
        assertEquals("number of keys mismatch!", numKeys,
                     receivedKeys.size());

        IntegerBinding binding = new IntegerBinding();
        for (int i = 0; i < keys.size(); i++){
            Integer expectedKey = keys.get(i);
            byte[] receivedKeyByte = receivedKeys.get(i);

            TupleInput tuple = new TupleInput(receivedKeyByte);
            Integer receivedKey = binding.entryToObject(tuple);
            assertEquals("mismatched key!", expectedKey.longValue(),
                         receivedKey.longValue());
        }
        logger.info("successfully verified all " + numKeys + "keys" +
                    ", # commits: " + mycbk.getNumCommits() +
                    ", # aborts: " + mycbk.getNumAborts());
    }

    /*
     * Turn off data cleaner on master to prevent start VLSN from being
     * cleaned. It can be turned on or called explicitly in unit test.
     */
    private void turnOffMasterCleaner() {

        EnvironmentConfig econfig = repEnvInfo[0].getEnvConfig();
        /*
         * Turn off the cleaner, since it's controlled explicitly
         * by the test.
         */
        econfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
                               "false");
    }

    /* test helper to test subscription with or without group uuid */
    private void testGroupUUIDhelper(boolean useGroupUUID) throws Exception {

        /* turn off data cleaner on master */
        turnOffMasterCleaner();

        /* create and verify a replication group */
        prepareTestEnv();

        /* populate test db and verify */
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

        /* create a subscription with a valid group uuid */
        SubscriptionConfig config = createSubConfig(masterEnv, useGroupUUID);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeys);

        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);

        /* start streaming from the very beginning, should succeed */
        subscription.start();

        try {
            waitForTestDone(testCbk);
        } catch (TimeoutException e) {
            failTimeout();
        }
        /* shutdown test verify we receive all expected keys */
        subscription.shutdown();
        verifyTestResults(testCbk);
    }

    private class ExceptionTestHook
        extends TestHookAdapter<SubscriptionThread> {

        private Exception e;

        ExceptionTestHook(Exception e) {
            this.e = e;
        }

        @Override
        public void doHook(final SubscriptionThread subscriptionThread) {
            try {
                subscriptionThread.offer(e);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

        @Override
        public SubscriptionThread getHookValue() {
            throw new UnsupportedOperationException();
        }
    }

    private void testSubscriptionFromVLSNHelper(Properties sslProps)
        throws Exception {

        /* a small db is enough for this test */
        numKeys = 1024;

        /* turn off data cleaner on master */
        turnOffMasterCleaner();

        /* the start VLSN to subscribe*/
        final VLSN startVLSN = new VLSN(100);
        /* number of transactions we subscribe */
        final int numKeysToStream = 10;

        /* create and verify a replication group */
        prepareTestEnv();

        /* populate some data and verify */
        ReplicatedEnvironment masterEnv = repEnvInfo[0].getEnv();
        populateDataAndVerify(masterEnv);

        /* create a subscription */
        SubscriptionConfig config = createSubConfig(masterEnv, false, sslProps);
        SubscriptionTestCallback testCbk =
            new SubscriptionTestCallback(numKeysToStream);
        config.setCallback(testCbk);
        subscription = new Subscription(config, logger);

        logger.info("subscription created at home " +
                    config.getSubscriberHome() +
                    ", start streaming " + numKeys + " items from feeder");


        subscription.start(startVLSN);

        /* let subscription run to receive expected # of keys */
        try {
            waitForTestDone(testCbk);
        } catch (TimeoutException te) {
            failTimeout();
        }

        /* verify */
        assertEquals("Mismatch start VLSN!", startVLSN,
                     testCbk.getFirstVLSN());
        assertEquals("Mismatch start VLSN from statistics",
                     subscription.getStatistics().getStartVLSN(),
                     testCbk.getFirstVLSN());

        subscription.shutdown();

    }
}
