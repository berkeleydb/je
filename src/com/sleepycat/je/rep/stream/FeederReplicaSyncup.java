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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.cleaner.FileProtector.ProtectedFileSet;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequest;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.stream.BaseProtocol.RestoreRequest;
import com.sleepycat.je.rep.stream.BaseProtocol.StartStream;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * Establish where the replication stream should start for a feeder and replica
 * pair. The Feeder's job is to send the replica the parts of the replication
 * stream it needs, so that the two can determine a common matchpoint.
 *
 * If a successful matchpoint is found the feeder learns where to start the
 * replication stream for this replica.
 */
public class FeederReplicaSyncup {

    /* A test hook that is called after a syncup has started. */
    private static volatile TestHook<Feeder> afterSyncupStartedHook;

    /* A test hook that is called after a syncup has ended. */
    private static volatile TestHook<Feeder> afterSyncupEndedHook;

    private final Feeder feeder;
    private final RepNode repNode;
    private final NamedChannel namedChannel;
    private final Protocol protocol;
    private final VLSNIndex vlsnIndex;
    private final Logger logger;
    private FeederSyncupReader backwardsReader;

    public FeederReplicaSyncup(Feeder feeder,
                               NamedChannel namedChannel,
                               Protocol protocol) {
        this.feeder = feeder;
        this.repNode = feeder.getRepNode();
        logger = LoggerUtils.getLogger(getClass());
        this.namedChannel = namedChannel;
        this.protocol = protocol;
        this.vlsnIndex = repNode.getVLSNIndex();
    }

    /**
     * The feeder's side of the protocol. Find out where to start the
     * replication stream.
     *
     * @throws NetworkRestoreException if sync up failed and network store is
     * required
     * @throws ChecksumException if checksum validation failed
     */
    public void execute()
        throws DatabaseException, IOException, InterruptedException,
               NetworkRestoreException, ChecksumException {

        final long startTime = System.currentTimeMillis();
        RepImpl repImpl = repNode.getRepImpl();
        LoggerUtils.info(logger, repImpl,
                         "Feeder-replica " +
                         feeder.getReplicaNameIdPair().getName() +
                         " syncup started. Feeder range: " +
                         repNode.getVLSNIndex().getRange());

        /*
         * Prevent the VLSNIndex range from being changed and protect all files
         * in the range. To search the index and read files within this range
         * safely, VLSNIndex.getRange must be called after syncupStarted.
         */
        final ProtectedFileSet protectedFileSet =
            repNode.syncupStarted(feeder.getReplicaNameIdPair());

        try {
            assert TestHookExecute.doHookIfSet(afterSyncupStartedHook, feeder);

            /*
             * Wait for the replica to start the syncup message exchange. The
             * first message will always be an EntryRequest. This relies on the
             * fact that a brand new group always begins with a master that has
             * a few vlsns from creating the nameDb that exist before a replica
             * syncup. The replica will never issue a StartStream before doing
             * an EntryRequest.
             *
             * The first entry request has three possible types of message
             * responses - EntryNotFound, AlternateMatchpoint, or Entry.
             */
            VLSNRange range = vlsnIndex.getRange();
            EntryRequest firstRequest =
                (EntryRequest) protocol.read(namedChannel);
            Message response = makeResponseToEntryRequest(range,
                                                          firstRequest,
                                                          true);

            protocol.write(response, namedChannel);

            /*
             * Now the replica may send one of three messages:
             * - a StartStream message indicating that the replica wants to
             * start normal operations
             * - a EntryRequest message if it's still hunting for a
             * matchpoint. There's the possibility that the new EntryRequest
             * asks for a VLSN that has been log cleaned, so check that we can
             * supply it.
             * - a RestoreRequest message that indicates that the replica
             * has given up, and will want a network restore.
             */

            VLSN startVLSN;
            while (true) {
                Message message = protocol.read(namedChannel);
                if (logger.isLoggable(Level.FINEST)) {
                    LoggerUtils.finest(logger, repImpl,
                                       "Replica " +
                                       feeder.getReplicaNameIdPair() +
                                       " message op: " + message.getOp());
                }
                if (message instanceof StartStream) {
                    final StartStream startMessage = (StartStream) message;
                    startVLSN = startMessage.getVLSN();

                    /* set feeder filter */
                    final FeederFilter filter = startMessage.getFeederFilter();
                    feeder.setFeederFilter(filter);

                    /*
                     * skip security check if not needed, e.g., a replica in
                     * a secure store
                     */
                    if (!feeder.needSecurityChecks()) {
                        break;
                    }

                    final StreamAuthenticator auth = feeder.getAuthenticator();
                    /* if security check is needed, auth cannot be null */
                    assert (auth != null);
                    /* remember table id strings of subscribed tables */
                    if (filter != null) {
                        auth.setTableIds(filter.getTableIds());
                    } else {
                        /* if no filter, subscribe all tables */
                        auth.setTableIds(null);
                    }
                    /* security check */
                    if (!auth.checkAccess()) {
                        final String err = "Replica " +
                                           feeder.getReplicaNameIdPair()
                                                 .getName() +
                                           " fails security check.";
                        LoggerUtils.warning(logger, repImpl, err);

                        throw new ReplicationSecurityException(
                            err, feeder.getReplicaNameIdPair().getName(),
                            null);
                    }
                    break;
                } else if (message instanceof EntryRequest) {
                    response = makeResponseToEntryRequest
                        (range, (EntryRequest) message, false);
                    protocol.write(response, namedChannel);
                } else if (message instanceof RestoreRequest) {
                    throw answerRestore(range,
                                        ((RestoreRequest) message).getVLSN());
                } else {
                    throw EnvironmentFailureException.unexpectedState
                        (repImpl,
                         "Expected StartStream or EntryRequest but got " +
                         message);
                }
            }

            LoggerUtils.info(logger, repImpl,
                             "Feeder-replica " +
                             feeder.getReplicaNameIdPair().getName() +
                             " start stream at VLSN: " + startVLSN);

            feeder.initMasterFeederSource(startVLSN);

        } finally {
            repNode.syncupEnded(protectedFileSet);
            assert TestHookExecute.doHookIfSet(afterSyncupEndedHook, feeder);
            LoggerUtils.info
                (logger, repImpl,
                 String.format("Feeder-replica " +
                               feeder.getReplicaNameIdPair().getName() +
                               " syncup ended. Elapsed time: %,dms",
                               (System.currentTimeMillis() - startTime)));

        }
    }

    /** For testing. */
    public static void setAfterSyncupStartedHook(TestHook<Feeder> hook) {
        afterSyncupStartedHook = hook;
    }

    /** For testing. */
    public static void setAfterSyncupEndedHook(TestHook<Feeder> hook) {
        afterSyncupEndedHook = hook;
    }

    private FeederSyncupReader setupReader(VLSN startVLSN)
        throws DatabaseException, IOException {

        EnvironmentImpl envImpl = repNode.getRepImpl();
        int readBufferSize = envImpl.getConfigManager().
            getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        /*
         * A BackwardsReader for scanning the log file backwards.
         */
        long lastUsedLsn = envImpl.getFileManager().getLastUsedLsn();

        VLSN firstVLSN = vlsnIndex.getRange().getFirst();
        long firstFile = vlsnIndex.getLTEFileNumber(firstVLSN);
        long finishLsn = DbLsn.makeLsn(firstFile, 0);
        return new FeederSyncupReader(envImpl,
                                      vlsnIndex,
                                      lastUsedLsn,
                                      readBufferSize,
                                      startVLSN,
                                      finishLsn);
    }

    private Message makeResponseToEntryRequest(VLSNRange range,
                                               EntryRequest request,
                                               boolean isFirstResponse)
        throws IOException, ChecksumException {

        final VLSN requestMatchpoint = request.getVLSN();
        final EntryRequestType type = request.getType();

        /* if NOW mode, return high end regardless of requested vlsn */
        if (type.equals(EntryRequestType.NOW)) {
            /*
             * VLSN range is not empty even without user data, so we can
             * always get a valid entry.
             */
            return protocol.new Entry(getMatchPtRecord(range.getLast()));
        }

        /* stream modes other than NOW */

        /*
         * The matchpoint must be in the VLSN range, or more specifically, in
         * the VLSN index so we can map the VLSN to the lsn in order to fetch
         * the associated log record.
         */
        if (range.getFirst().compareTo(requestMatchpoint) > 0) {
            /* request point is smaller than lower bound of range */
            if (type.equals(BaseProtocol.EntryRequestType.AVAILABLE)) {
                return protocol.new Entry(getMatchPtRecord(range.getFirst()));
            }

            /* default mode */
            return protocol.new EntryNotFound();
        }

        if (range.getLast().compareTo(requestMatchpoint) < 0) {
            /* request point is higher than upper bound of range */
            if (type.equals(EntryRequestType.AVAILABLE)) {
                return protocol.new Entry(getMatchPtRecord(range.getLast()));
            }

            /*
             * default mode:
             *
             * The matchpoint is after the last one in the range. We have to
             * suggest the lastSync entry on this node as an alternative. This
             * should only happen on the feeder's first response. For example,
             * suppose the feeder's range is vlsns 1-100. It's possible that
             * the exchange is as follows:
             *  1 - replica has 1-110, asks feeder for 110
             *  2 - feeder doesn't have 110, counters with 100
             *  3 - from this point on, the replica should only ask for vlsns
             *      that are <= the feeder's counter offer of 100
             * Guard that this holds true, because the feeder's log reader is
             * only set to search backwards; it does not expect to toggle
             * between forward and backwards.
             */
            assert backwardsReader == null :
              "Replica request for vlsn > feeder range should only happen " +
              "on the first exchange.";

            if (range.getLastSync().equals(VLSN.NULL_VLSN)) {
                /*
                 * We have no syncable entry at all. The replica will have to
                 * do a network restore.
                 */
                return protocol.new EntryNotFound();
            }

            if (isFirstResponse) {
                final OutputWireRecord lastSync =
                    getMatchPtRecord(range.getLastSync());
                assert lastSync != null :
                "Look for alternative, range=" + range;
                return protocol.new AlternateMatchpoint(lastSync);
            }

            throw EnvironmentFailureException.unexpectedState
                (repNode.getRepImpl(), "RequestMatchpoint=" +
                 requestMatchpoint + " range=" + range +
                 "should only happen on first response");
        }

        /* The matchpoint is within the range. Find it. */
        final OutputWireRecord matchRecord =
            getMatchPtRecord(requestMatchpoint);
        if (matchRecord == null) {
            throw EnvironmentFailureException.unexpectedState
                (repNode.getRepImpl(),
                 "Couldn't find matchpoint " + requestMatchpoint +
                 " in log. VLSN range=" + range);
        }

        return protocol.new Entry(matchRecord);
    }

    /* scan log backwards to find match point record */
    private OutputWireRecord getMatchPtRecord(VLSN matchPointVLSN)
        throws IOException, ChecksumException {

        if (backwardsReader == null) {
            backwardsReader = setupReader(matchPointVLSN);
        }
        return backwardsReader.scanBackwards(matchPointVLSN);
    }

    private NetworkRestoreException answerRestore(VLSNRange range,
                                                  VLSN failedMatchpoint)
        throws IOException {

        /*
         * Note that getGlobalCBVLSN returns a null VLSN if the GlobalCBVLSN
         * is defunct. In that case the RestoreResponse.cbvlsn field is unused.
         */
        Message response = protocol.new
            RestoreResponse(repNode.getRestoreResponseVLSN(range),
                            repNode.getLogProviders());
        protocol.write(response, namedChannel);

        return new NetworkRestoreException(failedMatchpoint,
                                           range.getFirst(),
                                           range.getLast(),
                                           feeder.getReplicaNameIdPair());
    }

    @SuppressWarnings("serial")
    static public class NetworkRestoreException extends Exception {
        /* The out-of-range vlsn that provoked the exception */
        private final VLSN vlsn;
        private final VLSN firstVLSN;
        private final VLSN lastVLSN;

        /* The replica that made the request. */
        private final NameIdPair replicaNameIdPair;

        public NetworkRestoreException(VLSN vlsn,
                                       VLSN firstVLSN,
                                       VLSN lastVLSN,
                                       NameIdPair replicaNameIdPair) {
            this.vlsn = vlsn;
            this.firstVLSN = firstVLSN;
            this.lastVLSN = lastVLSN;
            this.replicaNameIdPair = replicaNameIdPair;
        }

        @Override
        public String getMessage() {
            return "Matchpoint vlsn " + vlsn + " requested by node: " +
                   replicaNameIdPair + " was outside the VLSN range: " +
                   "[" + firstVLSN + "-" + lastVLSN + "]";
        }

        public VLSN getVlsn() {
            return vlsn;
        }

        public NameIdPair getReplicaNameIdPair() {
            return replicaNameIdPair;
        }
    }
}
