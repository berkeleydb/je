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

import java.io.File;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object to represent a subscription to receive and process replication
 * streams from Feeder. It defines the public subscription APIs which can
 * be called by clients.
 */
public class Subscription {

    /* configuration parameters */
    private final SubscriptionConfig configuration;
    /* logger */
    private final Logger logger;
    /* subscription dummy environment */
    private final ReplicatedEnvironment dummyRepEnv;
    /* subscription statistics */
    private final SubscriptionStat statistics;

    /* main subscription thread */
    private SubscriptionThread subscriptionThread;

    /**
     * Create an instance of subscription from configuration
     *
     * @param configuration configuration parameters
     * @param logger        logging handler
     *
     * @throws IllegalArgumentException  if env directory does not exist
     */
    public Subscription(SubscriptionConfig configuration, Logger logger)
        throws IllegalArgumentException {

        this.configuration = configuration;
        this.logger = logger;

        /* init environment and parameters */
        dummyRepEnv = createDummyRepEnv(configuration, logger);
        subscriptionThread = null;
        statistics = new SubscriptionStat();
    }

    /**
     * Start subscription main thread, subscribe from the very first VLSN
     * from the feeder. The subscriber will stay alive and consume all entries
     * until it shuts down.
     *
     * @throws InsufficientLogException if feeder is unable to stream from
     *                                  start VLSN
     * @throws GroupShutdownException   if subscription receives group shutdown
     * @throws InternalException        if internal exception
     * @throws TimeoutException         if subscription initialization timeout
    */
    public void start()
        throws IllegalArgumentException, InsufficientLogException,
        GroupShutdownException, InternalException, TimeoutException {

        start(VLSN.FIRST_VLSN);
    }

    /**
     * Start subscription main thread, subscribe from a specific VLSN
     * from the feeder. The subscriber will stay alive and consume all entries
     * until it shuts down.
     *
     * @param vlsn the start VLSN of subscription. It cannot be NULL_VLSN
     *             otherwise an IllegalArgumentException will be raised.
     *
     * @throws InsufficientLogException if feeder is unable to stream from
     *                                  start VLSN
     * @throws GroupShutdownException   if subscription receives group shutdown
     * @throws InternalException        if internal exception
     * @throws TimeoutException         if subscription initialization timeout
     */
    public void start(VLSN vlsn)
        throws IllegalArgumentException, InsufficientLogException,
        GroupShutdownException, InternalException, TimeoutException {

        if (vlsn.equals(VLSN.NULL_VLSN)) {
            throw new IllegalArgumentException("Start VLSN cannot be null");
        }

        subscriptionThread =
            new SubscriptionThread(dummyRepEnv, vlsn,
                                   configuration, statistics,
                                   logger);
        /* fire the subscription thread */
        subscriptionThread.start();

        if (!waitForSubscriptionInitDone(subscriptionThread)) {
            LoggerUtils.warning(logger,
                                RepInternal.getNonNullRepImpl(dummyRepEnv),
                                "Timeout in initialization, shut down " +
                                "subscription.");
            shutdown();
            throw new TimeoutException("Subscription initialization timeout " +
                                       "after " +
                                       configuration.getPollTimeoutMs() +
                                       " ms");
        }

        /* if not success, throw exception to caller */
        final Exception exp = subscriptionThread.getStoredException();
        switch (subscriptionThread.getStatus()) {
            case SUCCESS:
                break;

            case VLSN_NOT_AVAILABLE:
                /* shutdown and close env before throw exception to client */
                shutdown();
                throw (InsufficientLogException) exp;

            case GRP_SHUTDOWN:
                /* shutdown and close env before throw exception to client */
                shutdown();
                throw (GroupShutdownException) exp;

            case UNKNOWN_ERROR:
            case CONNECTION_ERROR:
            default:
                /* shutdown and close env before throw exception to client */
                shutdown();
                throw new InternalException("internal exception from " +
                                            "subscription thread, err:" +
                                            exp.getMessage(), exp);
        }
    }

    /**
     * Shutdown a subscription completely
     */
    public void shutdown() {
        if (subscriptionThread != null && subscriptionThread.isAlive()) {
            subscriptionThread.shutdown();
        }
        subscriptionThread = null;

        if (dummyRepEnv != null) {
            final NodeType nodeType = configuration.getNodeType();
            if (nodeType.hasTransientId() && !dummyRepEnv.isClosed()) {
                RepInternal.getNonNullRepImpl(dummyRepEnv)
                           .getNameIdPair()
                           .revertToNull();
            }
            dummyRepEnv.close();
            logger.fine("Closed env " + dummyRepEnv.getNodeName() +
                        "(forget transient id? " +
                        nodeType.hasTransientId() + ")");
        }
    }

    /**
     * Get subscription thread status, if thread does not exit,
     * return subscription not yet started.
     *
     * @return status of subscription
     */
    public SubscriptionStatus getSubscriptionStatus() {
        if (subscriptionThread == null) {
            return SubscriptionStatus.INIT;
        } else {
            return subscriptionThread.getStatus();
        }
    }

    /**
     * Get subscription statistics
     *
     * @return  statistics
     */
    public SubscriptionStat getStatistics() {
        return statistics;
    }

    /**
     * For unit test only
     *
     * @return dummy env
     */
    ReplicatedEnvironment getDummyRepEnv() {
        return dummyRepEnv;
    }

    /**
     * For unit test only
     *
     * @param testHook test hook
     */
    void setExceptionHandlingTestHook(TestHook<SubscriptionThread> testHook) {
        if (subscriptionThread != null) {
            subscriptionThread.setExceptionHandlingTestHook(testHook);
        }
    }

    /**
     * Create a dummy replicated env used by subscription. The dummy env will
     * be used in the SubscriptionThread, SubscriptionProcessMessageThread and
     * SubscriptionOutputThread to connect to feeder.
     *
     * @param conf   subscription configuration
     * @param logger logger
     * @return a replicated environment
     * @throws IllegalArgumentException if env directory does not exist
     */
    private static ReplicatedEnvironment
    createDummyRepEnv(SubscriptionConfig conf, Logger logger)
        throws IllegalArgumentException {

        final ReplicatedEnvironment ret;
        final File envHome = new File(conf.getSubscriberHome());
        if (!envHome.exists()) {
            throw new IllegalArgumentException("Env directory " +
                                               envHome.getAbsolutePath() +
                                               " does not exist.");
        }

        ret =
            RepInternal.createInternalEnvHandle(envHome,
                                                conf.createReplicationConfig(),
                                                conf.createEnvConfig());

        /*
         * A safety check and clear id if necessary, to prevent env with
         * existing id from failing the subscription
         */
        final NameIdPair pair = RepInternal.getNonNullRepImpl(ret)
                                           .getNameIdPair();
        if (conf.getNodeType().hasTransientId() && !pair.hasNullId()) {
            logger.fine("Env has a non-null id, clear its id(name id: " +
                        pair + ")");
            pair.revertToNull();
        }
        logger.fine("Env created with name id pair " + pair);
        return ret;
    }

    /**
     * Wait for subscription thread to finish initialization
     *
     * @param t thread of subscription
     * @return true if init done successfully, false if timeout
     */
    private boolean waitForSubscriptionInitDone(final SubscriptionThread t) {
        return new PollCondition(configuration.getPollIntervalMs(),
                                 configuration.getPollTimeoutMs()) {
            @Override
            protected boolean condition() {
                return t.getStatus() != SubscriptionStatus.INIT;
            }

        }.await();
    }
}
