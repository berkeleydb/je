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

import static com.sleepycat.je.rep.stream.ArbiterFeederStatDefinition.QUEUE_FULL;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;

/**
 * Implementation of a master node acting as a FeederSource for an Arbiter.
 */
public class ArbiterFeederSource implements FeederSource {


    private final BlockingQueue<LogItem> queue;
    private final EnvironmentImpl envImpl;
    private final StatGroup stats;
    private final LongStat nQueueFull;

    public ArbiterFeederSource(EnvironmentImpl envImpl)
        throws DatabaseException {

        int queueSize =
            envImpl.getConfigManager().getInt
            (RepParams.ARBITER_OUTPUT_QUEUE_SIZE);
        queue = new ArrayBlockingQueue<LogItem>(queueSize);
        this.envImpl = envImpl;
        stats =
            new StatGroup(ArbiterFeederStatDefinition.GROUP_NAME,
                          ArbiterFeederStatDefinition.GROUP_DESC);
        nQueueFull = new LongStat(stats, QUEUE_FULL);
    }

    public void addCommit(LogItem commitItem) {

        if (!queue.offer(commitItem)) {

            /*
             * If the commit could not be added to the queue because
             * the queue is filled. Try to remove an item
             * and replace with the item with the higher VLSN.
             * The Arbiter ack for the higher VLSN is sufficient
             * for transactions with a lower commit VLSN.
             */
            nQueueFull.increment();
            try {
                LogItem queuedItem = queue.remove();
                VLSN vlsn = commitItem.header.getVLSN();
                if (queuedItem.header.getVLSN().compareTo(vlsn) > 0) {

                    /*
                     * The removed item has higher vlsn so use that one.
                     */
                    commitItem = queuedItem;
                }
            } catch (NoSuchElementException noe) {
                /* Queue was empty so try to insert one last time. */
            }

            /*
             * Attempt to put the item on the queue. If another
             * thread has inserted and the queue is full, we will
             * skip this transaction for an Arbiter ack attempt. The
             * transaction may still succeed in this case due to acks from
             * Replicas or other Arbiter acked transactions with a higher
             * VLSN.
             */
            queue.offer(commitItem);
        }
    }

    @Override
    public void shutdown(EnvironmentImpl envImpl) {
    }

    /*
     * @see com.sleepycat.je.rep.stream.FeederSource#getLogRecord
     * (com.sleepycat.je.utilint.VLSN, int)
     */
    @Override
    public OutputWireRecord getWireRecord(VLSN vlsn, int waitTime)
        throws DatabaseException, InterruptedException, IOException {

        LogItem commitItem = queue.poll(waitTime, TimeUnit.MILLISECONDS);
        if (commitItem != null) {
            return new OutputWireRecord(envImpl, commitItem) ;
        }
        return null;
    }

    public StatGroup loadStats(StatsConfig config)
            throws DatabaseException {
            StatGroup copyStats = stats.cloneGroup(config.getClear());
            return copyStats;
        }

    @Override
    public String dumpState() {
        return null;
    }
}
