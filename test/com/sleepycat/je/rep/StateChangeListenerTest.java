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

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.DETACHED;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.REPLICA;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.UNKNOWN;
import static java.util.logging.Level.INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.utilint.Timestamp;

public class StateChangeListenerTest extends RepTestBase {

    private volatile CountDownLatch listenerLatch = null;

    /*
     * Verify that a ReplicaStateException is correctly associated with the
     * state change event that established it as such.
     */
    @Test
    public void testEventIdentity() {
        ReplicatedEnvironment rep0 = repEnvInfo[0].openEnv();
        rep0.setStateChangeListener(new PassiveListener(rep0));

        ReplicatedEnvironment rep1 = repEnvInfo[1].openEnv();
        rep1.setStateChangeListener(new PassiveListener(rep1));
        assertTrue(rep1.getState().isReplica());
        try {
            rep1.openDatabase(null,"db", dbconfig);
            fail("expected exception");
        } catch (ReplicaWriteException e) {
            final PassiveListener passiveListener =
                (PassiveListener)rep1.getStateChangeListener();
            assertEquals(e.getEvent(), passiveListener.currentEvent);
        }
    }

    /*
     * Verify that an exception leaking out of a listener invalidates the
     * environment.
     */
    @Test
    public void testExceptionInStateChangeNotifier() {
        ReplicatedEnvironment rep = repEnvInfo[0].openEnv();
        BadListener listener = new BadListener();
        try {
            rep.setStateChangeListener(listener);
            fail("Expected exception");
        } catch (EnvironmentFailureException e) {
            assertTrue(e.getCause() instanceof NullPointerException);
            assertTrue(!rep.isValid());
        }
        repEnvInfo[0].closeEnv();
    }

    @Test
    public void testListenerReplacement() {
        ReplicatedEnvironment rep = repEnvInfo[0].openEnv();

        final Listener listener1 = new Listener(rep);
        rep.setStateChangeListener(listener1);
        assertEquals(listener1, rep.getStateChangeListener());
        final Listener listener2 = new Listener(rep);
        rep.setStateChangeListener(listener2);
        assertEquals(listener2, rep.getStateChangeListener());
        repEnvInfo[0].closeEnv();
    }

    @Test
    public void testBasic()
        throws Exception {
        List<Listener> listeners = new LinkedList<Listener>();

        /* Verify that initial notification is always sent. */
        for (int i=0; i < repEnvInfo.length; i++) {
            ReplicatedEnvironment rep = repEnvInfo[i].openEnv();
            State state = rep.getState();
            State expectedState = (i == 0) ? MASTER : REPLICA;
            assertEquals(expectedState, state);
            Listener listener = new Listener(rep);
            listeners.add(listener);
            rep.setStateChangeListener(listener);
            /* Check that there was an immediate callback. */
            assertEquals(1, listener.events.size());
            StateChangeEvent event = listener.events.get(0);
            assertEquals(expectedState, event.getState());
            assertEquals(repEnvInfo[0].getRepConfig().getNodeName(),
                         event.getMasterNodeName());
            listener.events.clear();
        }

        /*
         * Verify that notifications are sent on master transitions. 2
         * transitions per node, except for the node being shutdown.
         */
        listenerLatch = new CountDownLatch(repEnvInfo.length*2);
        repEnvInfo[0].closeEnv();
        /* Wait 60s to ensure events can be delivered */
        awaitEvents(listenerLatch, 60, TimeUnit.SECONDS,
                    listeners.get(0).events, 2, UNKNOWN, DETACHED);

        int masterIndex = -1;
        for (int i=1; i < repEnvInfo.length; i++) {
            /* Verify state transitions: UNKNOWN [MASTER | REPLICA] */
            assertEquals(2, listeners.get(i).events.size());

            final State handleState = repEnvInfo[i].getEnv().getState();
            assertEquals(UNKNOWN, listeners.get(i).events.get(0).getState());
            assertEquals(handleState,
                         listeners.get(i).events.get(1).getState());
            if (handleState == MASTER) {
                masterIndex = i;
            }
        }
        assertTrue(masterIndex > 0);

        /* Verify that notifications are sent on close. */
        for (int i=1; i < repEnvInfo.length; i++) {
            listeners.get(i).events.clear();
            int numExpectedEvents = (masterIndex==i) ? 2 : 1;
            listenerLatch = new CountDownLatch(numExpectedEvents);
            repEnvInfo[i].closeEnv();
            /* Wait 60s to ensure events can be delivered */
            awaitEvents(listenerLatch, 60, TimeUnit.SECONDS,
                        listeners.get(i).events, numExpectedEvents);
        }
    }

    /**
     * Test state changes when establishing a secondary node, having it lose
     * contact with the master, and then shutting it down.
     */
    @Test
    public void testSecondary()
        throws Exception {

        /* Set up environment with a secondary replica */
        ReplicatedEnvironment rep0 = repEnvInfo[0].openEnv();
        repEnvInfo[1].getRepConfig().setNodeType(NodeType.SECONDARY);
        ReplicatedEnvironment rep1 = repEnvInfo[1].openEnv();

        /* Listen for as many as three state events */
        listenerLatch = new CountDownLatch(3);
        Listener listener = new Listener(rep1);
        rep1.setStateChangeListener(listener);

        /* Close master, then replica */
        repEnvInfo[0].closeEnv();
        repEnvInfo[1].closeEnv();

        /* Check expected states */
        /* Wait 60s to ensure events can be delivered */
        /*
         * There should be either two or three events: REPLICA, UNKNOWN
         * (optional), DETACHED.  The UNKNOWN event is optional because it
         * depends on the timing of closing the environments.  It will be
         * generated only if the secondary notices the loss of the master
         * before it is closed down.
         */
        listenerLatch.await(60, TimeUnit.SECONDS);
        if (listener.events.size() == 2) {
            awaitEvents(listenerLatch, 0, TimeUnit.SECONDS,
                        listener.events, 2, REPLICA, DETACHED);
        } else {
            awaitEvents(listenerLatch, 0, TimeUnit.SECONDS,
                        listener.events, 3, REPLICA, UNKNOWN, DETACHED);
        }
    }

    /**
     * Assert that the count down latch reaches zero in the specified amount
     * of time, and confirm that the expected number of events were delivered.
     * If expectedStates are specified, check that the delivered events have
     * the expected states.
     */
    void awaitEvents(CountDownLatch latch,
                     long time,
                     TimeUnit timeUnit,
                     List<StateChangeEvent> events,
                     int numExpectedEvents,
                     State... expectedStates)
        throws InterruptedException {

        final long start = System.currentTimeMillis();
        latch.await(time, timeUnit);
        if (events.size() < numExpectedEvents) {
            fail("Expected " + numExpectedEvents + " events, found " +
                 events.size() + ": " + describeEvents(events));
        }

        if ((expectedStates != null) && (expectedStates.length > 0)) {
            assertEquals("Number of expected states", numExpectedEvents,
                         expectedStates.length);
            for (int i = 0; i < numExpectedEvents; i++) {
                if (!expectedStates[i].equals(events.get(i).getState())) {
                    fail("Expected event " + i + " state " +
                         expectedStates[i] + ", found " +
                         events.get(i).getState() + ", for events: " +
                         describeEvents(events));
                }
            }
        }
        if (logger.isLoggable(INFO)) {
            logger.info("Received awaited events" +
                        ", startTime: " + new Timestamp(start) +
                        ", events: " + describeEvents(events));
        }
    }

    private String describeEvents(final List<StateChangeEvent> events) {
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final StateChangeEvent event : events) {
            if (!first) {
                sb.append(", ");
            } else {
                first = false;
            }
            sb.append("StateChangeEvent[");
            sb.append("state=").append(event.getState());
            sb.append(", eventTime=");
            sb.append(new Timestamp(event.getEventTime()));
            sb.append("]");
        }
        return sb.toString();
    }

    class Listener implements StateChangeListener {

        final ReplicatedEnvironment rep;
        final List<StateChangeEvent> events =
            Collections.synchronizedList(new LinkedList<StateChangeEvent>());

        public Listener(ReplicatedEnvironment rep) {
            this.rep = rep;
        }

        @Override
        public void stateChange(StateChangeEvent stateChangeEvent) {
            events.add(stateChangeEvent);
            if (listenerLatch != null) {
                listenerLatch.countDown();
            }
        }
    }

    /* Always throw an exception upon notification. */
    class BadListener implements StateChangeListener {

        @Override
        public void stateChange
            (@SuppressWarnings("unused") StateChangeEvent stateChangeEvent) {

            throw new NullPointerException("Test exception");
        }
    }

    /**
     * A passive listener that simply remembers the last event.
     */
    class PassiveListener implements StateChangeListener {

        final ReplicatedEnvironment rep;
        volatile StateChangeEvent currentEvent = null;

        public PassiveListener(ReplicatedEnvironment rep) {
            this.rep = rep;
        }

        @Override
        public void stateChange(StateChangeEvent stateChangeEvent) {
            currentEvent = stateChangeEvent;
        }
    }
}
