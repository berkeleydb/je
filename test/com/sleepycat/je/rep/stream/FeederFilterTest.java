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

package com.sleepycat.je.rep.stream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.utilint.LoggerUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the generic filtering at the Feeder
 */
public class FeederFilterTest extends RepTestBase {

    private final Logger logger =
            LoggerUtils.getLoggerFixedPrefix(getClass(), "Test");


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test to verify the filter is actually passed to feeder and is running
     * at feeder. Filter used in test simply filter out all records checked,
     * thus Replica would be unable to achieve consistency.
     *
     * ReplicaConsistencyException will be raised if filter works as expected.
     */
    @Test
    public void testBlockFilter() {
        /* set timeout to 10 seconds to shorten test time */
        repEnvInfo[1].getRepConfig()
                     .setConfigParam(RepParams.ENV_CONSISTENCY_TIMEOUT
                             .getName(), "10 seconds");

        /* set a blocking filter for a replica */
        TestFilterBlockAll filter = new TestFilterBlockAll(1);
        repEnvInfo[1].getRepConfig().setFeederFilter(filter);
        try {
            createGroup(3);
        } catch (ReplicaConsistencyException e) {
            /*
             * expect consistency exception because all entries
             * are filtered out by feeder and replica is unable
             * to achieve consistency.
             */
            logger.info("expected exception: " + e.getLocalizedMessage());
            return;
        }

        fail("Test failed due to missing expected consistency exception");
    }

    /**
     * Test that uses a filter pass everything, so the rep group
     * is expected to be created normally.
     */
    @Test
    public void testNoOpFilter() {
        TestFilterPassAll filter = new TestFilterPassAll(2);
        /* set filter for a replica */
        repEnvInfo[1].getRepConfig().setFeederFilter(filter);
        createGroup(3);
    }

    /**
     * Test that uses a filter pass everything, and also tracks the statistics
     * which are used for verification.
     */
    @Test
    public void testFilterWithStatistics() {
        /* timeout is 10 seconds to shorten test time */
        repEnvInfo[1].getRepConfig()
                     .setConfigParam(RepParams.ENV_CONSISTENCY_TIMEOUT
                             .getName(), "10 seconds");

        /* set a blocking filter for a replica */
        String filterID = "TestFilterWithStat";
        TestFilterPassAllWithStat filter =
                new TestFilterPassAllWithStat(filterID);
        repEnvInfo[1].getRepConfig().setFeederFilter(filter);

        createGroup(3);

        /* verification by looking at the de-serialized filter */
        List<TestFilterPassAllWithStat> deserializedFilters =
                filter.getDeserializedFilter();
        assertTrue("Expect at least one de-serialized filter",
                deserializedFilters.isEmpty() ==  false);
        boolean foundFilter =  false;
        for (TestFilterPassAllWithStat f : deserializedFilters) {
            if (f.getFilterId().equals(filterID)) {
                foundFilter = true;
                f.dumpFilterStatistics(logger);

                assertTrue("Expect non-zero number of checked records",
                        f.getNumCheckedRecords() > 0);
                assertTrue("Expect non-zero internal records",
                        f.getNumInternalRecords() > 0);
                assertTrue("Expect non-zero commits",
                        f.getNumNumCommits() > 0);
            }
        }
        assertTrue("Expect find de-serialized filter", foundFilter);
    }

    /* a test filter which filters out every entry */
    public static class TestFilterBlockAll
        implements FeederFilter, Serializable {
        private static final long serialVersionUID = 1L;
        private final int filterID;

        TestFilterBlockAll(int id) {
            super();
            filterID = id;
        }

        @Override
        public OutputWireRecord execute(final OutputWireRecord outputRecord,
                                        final RepImpl repImpl) {
            /* block every entry! */
            return null;
        }

        @Override
        public String[] getTableIds() {
            return null;
        }

    }
    
    /* a test filter which passes every entry */
    public static class TestFilterPassAll
            implements FeederFilter, Serializable {
        private static final long serialVersionUID = 1L;
        private final int filterID;

        TestFilterPassAll(int id) {
            super();
            filterID = id;
        }

        @Override
        public OutputWireRecord execute(final OutputWireRecord outputRecord,
                                        final RepImpl repImpl) {
            /* no-op filter */
            return outputRecord;
        }

        @Override
        public String[] getTableIds() {
            return null;
        }

    }

    /* another pass-all test filter, with tracking of statistics */
    public static class TestFilterPassAllWithStat
            implements FeederFilter, Serializable {

        private static final long serialVersionUID = 1L;
        private static final List<TestFilterPassAllWithStat>
                deserializedFilters = Collections.synchronizedList(
                        new ArrayList<TestFilterPassAllWithStat>());

        private final String filterID;
        private int numCheckedRecords;
        private int numInternalRecords;
        private int numNumCommits;

        TestFilterPassAllWithStat(String id) {
            super();
            filterID = id;
            numCheckedRecords = 0;
            numInternalRecords = 0;
            numNumCommits = 0;
        }

        public String getFilterId() {
            return filterID;
        }

        public int getNumCheckedRecords() {
            return numCheckedRecords;
        }

        public int getNumInternalRecords() {
            return numInternalRecords;
        }

        public int getNumNumCommits() {
            return numNumCommits;
        }

        public List<TestFilterPassAllWithStat> getDeserializedFilter() {
            return deserializedFilters;
        }

        public void dumpFilterStatistics(Logger logger) {
            logger.info("feeder filter id: " + filterID +
                    " number checked entries:" + numCheckedRecords +
                    " number internal nameln entries: " + numInternalRecords +
                    " number of commits: " + numNumCommits +
                    " number of de-serialized filters: " +
                    deserializedFilters.size());
        }

        @Override
        public OutputWireRecord execute(final OutputWireRecord outputRecord,
                                        final RepImpl repImpl) {
            final byte type = outputRecord.getEntryType();
            numCheckedRecords++;
            if (LogEntryType.LOG_NAMELN_TRANSACTIONAL.equalsType(type) ||
                    LogEntryType.LOG_UPD_LN_TRANSACTIONAL.equalsType(type) ||
                    LogEntryType.LOG_INS_LN_TRANSACTIONAL.equalsType(type)) {
                numInternalRecords++;
            } else if (LogEntryType.LOG_TXN_COMMIT.equalsType(type)) {
                numNumCommits++;
            } else {
                /* do not count other types if seen */
            }

            /* pass every entry! */
            return outputRecord;
        }

        @Override
        public String[] getTableIds() {
            return null;
        }

        /* get and store the de-serialized filter at feeder thread */
        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {

            in.defaultReadObject();
            deserializedFilters.add(this);
        }
    }
}
