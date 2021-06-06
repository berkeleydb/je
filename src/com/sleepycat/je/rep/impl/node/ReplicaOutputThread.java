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

package com.sleepycat.je.rep.impl.node;

import java.io.IOException;

import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.VLSN;

public class ReplicaOutputThread extends ReplicaOutputThreadBase {
    private final RepNode repNode;

    ReplicaOutputThread(RepImpl repImpl) {
        super(repImpl);
        repNode = repImpl.getRepNode();
    }

    @Override
    public void writeReauthentication() throws IOException {
    }

    @Override
    public void writeHeartbeat(Long txnId) throws IOException {

        if ((txnId == null) && (repNode.getReplica().getTestDelayMs() > 0)) {
            return;
        }

        final VLSN broadcastCBVLSN = repNode.getCBVLSNTracker()
                                            .getBroadcastCBVLSN();
        protocol.write(protocol.new HeartbeatResponse(broadcastCBVLSN,
                                                      repNode.getReplica()
                                                             .getTxnEndVLSN()),
                       replicaFeederChannel);
    }
}
