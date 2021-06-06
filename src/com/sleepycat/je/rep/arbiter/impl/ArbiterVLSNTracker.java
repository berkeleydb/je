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

package com.sleepycat.je.rep.arbiter.impl;

import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_FSYNCS;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_WRITES;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_VLSN;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_DTVLSN;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;

/**
 * This class is used to maintain two pieces of persistent state. The
 * replication group node identifier of the Arbiter and a VLSN value that
 * represents the highest commit record VLSN the Arbiter has acknowledged.
 */
class ArbiterVLSNTracker {
    private static final int VERSION = 1;

    private RandomAccessFile raf;
    private final File dataFile;
    private VLSN currentVLSN = VLSN.NULL_VLSN;
    private volatile VLSN dtvlsn = VLSN.NULL_VLSN;
    private final int VERSION_OFFSET = 0;
    private final int NODEID_OFFSET = Integer.SIZE + VERSION_OFFSET;
    private final int DATA_OFFSET = Integer.SIZE + NODEID_OFFSET;
    private int nodeId = NameIdPair.NULL_NODE_ID;
    private final StatGroup stats;
    private final LongStat nWrites;
    private final LongStat nFSyncs;
    private final LongStat vlsnStat;
    private final LongStat dtVlsnStat;

    ArbiterVLSNTracker(File file) {
        dataFile = file;
        boolean fileExists = dataFile.exists();

        stats = new StatGroup(ArbiterStatDefinition.ARBIO_GROUP_NAME,
                              ArbiterStatDefinition.ARBIO_GROUP_DESC);
        nFSyncs = new LongStat(stats, ARB_N_FSYNCS);
        nWrites = new LongStat(stats, ARB_N_WRITES);
        vlsnStat = new LongStat(stats, ARB_VLSN);
        dtVlsnStat = new LongStat(stats, ARB_DTVLSN);
        try {
            raf = new RandomAccessFile(dataFile, "rw");
            if (fileExists) {
                final int readVersion = readVersion();
                if (readVersion > VERSION) {
                    throw new RuntimeException(
                        "Arbiter data file does not have a supported " +
                        "version field " +
                        dataFile.getAbsolutePath());
                }
                nodeId = readNodeId();
                if (raf.length() > DATA_OFFSET) {
                    raf.seek(DATA_OFFSET);
                    currentVLSN = new VLSN(raf.readLong());
                    dtvlsn =  new VLSN(raf.readLong());
                }
            } else {
                writeVersion(VERSION);
                writeNodeIdInternal(nodeId);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to read the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException(
                "Unable to open the Arbiter data file " +
                dataFile.getAbsolutePath() + " exception " + e.getMessage());
        }
    }

    public StatGroup loadStats(StatsConfig config) {
        vlsnStat.set(get().getSequence());
        dtVlsnStat.set(getDTVLSN().getSequence());
        return stats.cloneGroup(config.getClear());
    }

    public synchronized void writeNodeId(int id) {
        if (nodeId == id) {
            return;
        }
        writeNodeIdInternal(id);
    }

    public synchronized int getCachedNodeId() {
        return nodeId;
    }

    private void writeNodeIdInternal(int id) {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to write the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        try {
            raf.seek(NODEID_OFFSET);
            raf.writeInt(id);
            nWrites.increment();
            doFSync();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to write the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    private int readNodeId() {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to read the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        try {
            raf.seek(NODEID_OFFSET);
            return raf.readInt();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to read the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    public synchronized void writeVersion(int id) {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to write the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }

        if (nodeId == id) {
            return;
        }
        try {
            raf.seek(VERSION_OFFSET);
            raf.writeInt(id);
            nWrites.increment();
            doFSync();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to write the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    private int readVersion() {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to read the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        try {
            raf.seek(VERSION_OFFSET);
            return raf.readInt();
        } catch (IOException e) {
            throw new RuntimeException(
                "Unable to write the Arbiter data file " +
                dataFile.getAbsolutePath());
        }
    }

    public synchronized void write(VLSN nextCurrentVLSN,
                                   VLSN nextDTVLSN,
                                   boolean doFSync) {
        if (raf == null) {
            throw new RuntimeException(
                "Internal error: Unable to write the Arbiter data file " +
                " because the file is not open." +
                dataFile.getAbsolutePath());
        }
        if (nextCurrentVLSN.compareTo(currentVLSN) > 0) {
            this.currentVLSN = nextCurrentVLSN;
            this.dtvlsn = nextDTVLSN;
            try {
                raf.seek(DATA_OFFSET);
                raf.writeLong(nextCurrentVLSN.getSequence());
                raf.writeLong(nextDTVLSN.getSequence());
                nWrites.add(2);
                if (doFSync) {
                    doFSync();
                }
            } catch (IOException e) {
                throw new RuntimeException(
                    "Unable to write the Arbiter data file " +
                    dataFile.getAbsolutePath());
            }
        }
    }

    public synchronized void close() {
        if (raf != null) {
            try {
                doFSync();
                raf.close();
            } catch (IOException ignore) {
            } finally {
                raf = null;
            }
        }
    }

    public VLSN get() {
        return currentVLSN;
    }

    public VLSN getDTVLSN() {
        return dtvlsn;
    }

    public static StatGroup loadEmptyStats() {
        StatGroup tmpStats =
            new StatGroup(ArbiterStatDefinition.ARBIO_GROUP_NAME,
                          ArbiterStatDefinition.ARBIO_GROUP_DESC);
        new LongStat(tmpStats, ARB_N_FSYNCS);
        new LongStat(tmpStats, ARB_N_WRITES);
        new LongStat(tmpStats, ARB_VLSN);
        new LongStat(tmpStats, ARB_DTVLSN);
        return tmpStats;
    }

    private void doFSync() throws IOException {
        if (raf == null) {
            return;
        }
        raf.getFD().sync();
        nFSyncs.increment();

    }
}
