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

package com.sleepycat.je.rep.monitor;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;

import com.sleepycat.je.rep.impl.RepTestBase;

public class MonitorTestBase extends RepTestBase {

    /* The monitor being tested. */
    protected Monitor monitor;

    @Override
    @Before
    public void setUp()
        throws Exception {

        super.setUp();
        monitor = createMonitor(100, "mon10000");
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        super.tearDown();
        monitor.shutdown();
    }

    protected class TestChangeListener implements MonitorChangeListener {
        String masterNodeName;

        volatile NewMasterEvent masterEvent;
        volatile GroupChangeEvent groupEvent;
        volatile JoinGroupEvent joinEvent;
        volatile LeaveGroupEvent leaveEvent;

        /* Statistics records how may events happen. */
        private final AtomicInteger masterEvents = new AtomicInteger(0);
        private final AtomicInteger groupAddEvents = new AtomicInteger(0);
        private final AtomicInteger groupRemoveEvents = new AtomicInteger(0);
        private final AtomicInteger joinGroupEvents = new AtomicInteger(0);
        private final AtomicInteger leaveGroupEvents = new AtomicInteger(0);

        /* Barrier to test whether event happens. */
        volatile CountDownLatch masterBarrier;
        volatile CountDownLatch groupBarrier;
        volatile CountDownLatch joinGroupBarrier;
        volatile CountDownLatch leaveGroupBarrier;

        public TestChangeListener() {}

        public void notify(NewMasterEvent newMasterEvent) {
            logger.info("notify " + newMasterEvent);
            masterEvents.incrementAndGet();
            masterNodeName = newMasterEvent.getNodeName();
            masterEvent = newMasterEvent;
            countDownBarrier(masterBarrier);
        }

        public void notify(GroupChangeEvent groupChangeEvent) {
            logger.info("notify " + groupChangeEvent);
            switch (groupChangeEvent.getChangeType()) {
                case ADD:
                    groupAddEvents.incrementAndGet();
                    break;
                case REMOVE:
                    groupRemoveEvents.incrementAndGet();
                    break;
                default:
                    throw new IllegalStateException("Unexpected change type.");
            }
            groupEvent = groupChangeEvent;
            countDownBarrier(groupBarrier);
        }

        public void notify(JoinGroupEvent joinGroupEvent) {
            logger.info("notify " + joinGroupEvent);
            joinGroupEvents.incrementAndGet();
            joinEvent = joinGroupEvent;
            countDownBarrier(joinGroupBarrier);
        }

        public void notify(LeaveGroupEvent leaveGroupEvent) {
            logger.info("notify " + leaveGroupEvent);
            leaveGroupEvents.incrementAndGet();
            leaveEvent = leaveGroupEvent;
            countDownBarrier(leaveGroupBarrier);
        }

        void awaitEvent(CountDownLatch latch)
            throws InterruptedException {

            awaitEvent(null, latch);
        }

        void awaitEvent(String message, CountDownLatch latch)
            throws InterruptedException {

            latch.await(30, TimeUnit.SECONDS);
            assertEquals(((message != null) ? (message + ": ") : "") +
                         "Events not received after timeout:",
                         0, latch.getCount());
        }

        private void countDownBarrier(CountDownLatch barrier) {
            if (barrier != null && barrier.getCount() > 0) {
                barrier.countDown();
            }
        }

        int getMasterEvents() {
            return masterEvents.get();
        }

        void clearMasterEvents() {
            masterEvents.set(0);
        }

        int getGroupAddEvents() {
            return groupAddEvents.get();
        }

        void clearGroupAddEvents() {
            groupAddEvents.set(0);
        }

        int getGroupRemoveEvents() {
            return groupRemoveEvents.get();
        }

        void clearGroupRemoveEvents() {
            groupRemoveEvents.set(0);
        }

        int getJoinGroupEvents() {
            return joinGroupEvents.get();
        }

        void clearJoinGroupEvents() {
            joinGroupEvents.set(0);
        }

        int getLeaveGroupEvents() {
            return leaveGroupEvents.get();
        }

        void clearLeaveGroupEvents() {
            leaveGroupEvents.set(0);
        }

    }
}
