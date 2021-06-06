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

package com.sleepycat.je.rep.elections;

import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.elections.Protocol.Propose;
import com.sleepycat.je.rep.elections.Protocol.StringValue;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.util.test.TestBase;

/**
 * Tests the Acceptor Protocol for the correct responses to Propose and Accept
 * messages, based on the Paxos protocol.
 */
public class AcceptorTest extends TestBase {

    Protocol protocol;
    Acceptor acceptor;
    DataChannelFactory channelFactory;

    TimebasedProposalGenerator proposalGenerator =
        new TimebasedProposalGenerator();

    @Override
    @Before
    public void setUp() {
        channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig());

        Acceptor.SuggestionGenerator suggestionGenerator =
            new Acceptor.SuggestionGenerator() {

            @Override
            public Value get(Proposal proposal) {
                return new StringValue("VALUE");
            }

            @Override
            public Ranking getRanking(Proposal proposal) {
                return new Ranking(100, 0);
            }
        };
        protocol = new Protocol
        (TimebasedProposalGenerator.getParser(),
         MasterValue.getParser(),
         "TestGroup",
         new NameIdPair("n1", 1),
         null,
         channelFactory);
        protocol.updateNodeIds(new HashSet<Integer>
                                (Arrays.asList(Integer.valueOf(1))));
        RepNode rn =   new RepNode(new NameIdPair("n0", 0)) {
            @Override
            public int getElectionPriority() {
                return 1;
            }
        };
        ElectionsConfig ac = new RepElectionsConfig(rn);
        acceptor = new Acceptor
            (protocol,
             ac,
            suggestionGenerator);
    }

    @Override
    @After
    public void tearDown() {
        acceptor = null;
    }

    void checkPropose(Proposal pn, Protocol.MessageOp checkOp) {
        Propose prop = protocol.new Propose(pn);
        ResponseMessage prom1 = acceptor.process(prop);

        assertEquals(checkOp, prom1.getOp());
    }

    void checkAccept(Proposal pn, Value v, Protocol.MessageOp checkOp) {
        Accept a = protocol.new Accept(pn, v);
        ResponseMessage ad = acceptor.process(a);
        assertEquals(checkOp, ad.getOp());
    }

    @Test
    public void testAcceptor() {
        Proposal pn0 = proposalGenerator.nextProposal();
        Proposal pn1 = proposalGenerator.nextProposal();

        /* Proposal numbers should be in ascending order. */
        assertTrue(pn1.compareTo(pn0)> 0);

        checkPropose(pn1, protocol.PROMISE);

        /* Lower numbered proposal should be rejected. */
        checkPropose(pn0, protocol.REJECT);

        Value v = new StringValue("VALUE");
        checkAccept(pn1, v, protocol.ACCEPTED);

        /* .. and continue to be rejected after the acceptance. */
        checkPropose(pn0, protocol.REJECT);

        /* .. higher proposals should still be accepted. */
        Proposal pn2 = proposalGenerator.nextProposal();
        assertTrue(pn2.compareTo(pn1)> 0);
        checkPropose(pn2, protocol.PROMISE);
        checkAccept(pn2, v, protocol.ACCEPTED);

        /* .. and ones lower than the promised proposal are rejected. */
        checkAccept(pn0, v, protocol.REJECT);
        checkAccept(pn1, v, protocol.REJECT);
    }

    private class RepElectionsConfig implements ElectionsConfig {

        private final RepNode repNode;
        private String groupName;

        public RepElectionsConfig(RepNode repNode) {
            this.repNode = repNode;

            if (repNode.getRepImpl() == null) {
                /* when used for unit testing */
                return;
            }
            groupName =
                repNode.getRepImpl().getConfigManager().get(GROUP_NAME);
        }

        /**
         * used for testing only
         * @param groupName
         */
        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }
        public String getGroupName() {
            return groupName;
        }
        public NameIdPair getNameIdPair() {
            return repNode.getNameIdPair();
        }
        public ServiceDispatcher getServiceDispatcher() {
            return repNode.getServiceDispatcher();
        }
        public int getElectionPriority() {
            return repNode.getElectionPriority();
        }
        public int getLogVersion() {
            return repNode.getLogVersion();
        }
        public RepImpl getRepImpl() {
            return repNode.getRepImpl();
        }
        public RepNode getRepNode() {
            return repNode;
        }
    }
}
