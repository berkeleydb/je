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

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Promise;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.util.test.TestBase;

/** Test the RankingProposer class. */
public class RankingProposerTest extends TestBase {

    private static final String GROUP_NAME = "group1";
    private static final String NODE_NAME = "node1";
    private static final int NODE_ID = 42;
    private static final TimebasedProposalGenerator proposalGenerator =
        new TimebasedProposalGenerator();
    private static final InetSocketAddress socketAddress =
        new InetSocketAddress("localhost", 5000);

    private final Proposal proposal = proposalGenerator.nextProposal();
    private ServiceDispatcher serviceDispatcher;
    private RankingProposer proposer;
    private Protocol protocol;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        /* Set up facilities for creating proposals and promises */
        serviceDispatcher = new ServiceDispatcher(
            socketAddress, null, /* repImpl */ null /* channelFactory */);
        final NameIdPair nameIdPair = new NameIdPair(NODE_NAME, NODE_ID);
        final ElectionsConfig ec = new ElectionsConfig() {
            @Override
            public String getGroupName() { return GROUP_NAME; }
            @Override
            public NameIdPair getNameIdPair() { return nameIdPair; }
            @Override
            public ServiceDispatcher getServiceDispatcher() {
                return serviceDispatcher;
            }
            @Override
            public int getElectionPriority() { return 0; }
            @Override
            public int getLogVersion() { return -1; }
            @Override
            public RepImpl getRepImpl() { return null; }
            @Override
            public RepNode getRepNode() { return null; }
        };
        final Elections elections = new Elections(
            ec, null /* testListener */, null /* suggestionGenerator */);
        proposer = new RankingProposer(elections, nameIdPair);
        protocol = elections.getProtocol();
        protocol.updateNodeIds(new HashSet<Integer>(Arrays.asList(NODE_ID)));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.setUp();
        if (serviceDispatcher != null) {
            serviceDispatcher.shutdown();
        }
    }

    /* Tests */

    @Test
    public void testPhase2TwoNodes() {
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 200)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 200),
                                       promise(NODE_NAME, 100)));
    }

    @Test
    public void testPhase2ThreeNodes() {
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 200),
                                       promise(NODE_NAME, 300)));
    }

    @Test
    public void testPhase2ArbOneNode() {
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(null, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 100),
                                       promise(NODE_NAME, 100)));
        assertEquals(null,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(null, 200)));
        assertEquals(null,
                     choosePhase2Value(promise(null, 200),
                                       promise(NODE_NAME, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 200),
                                       promise(null, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 100),
                                       promise(NODE_NAME, 200)));
    }

    /**
     * Arbiter should be ignored if there are two nodes, even if the arbiter
     * has a higher DTVLSN.
     */
    @Test
    public void testPhase2ArbTwoNodes() {
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 100),
                                       promise(null, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(null, 100),
                                       promise(NODE_NAME, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 100),
                                       promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 100)));

        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 200),
                                       promise(null, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(null, 100),
                                       promise(NODE_NAME, 200)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 100),
                                       promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 200)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 200),
                                       promise(NODE_NAME, 100),
                                       promise(null, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 200),
                                       promise(null, 100),
                                       promise(NODE_NAME, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 100),
                                       promise(NODE_NAME, 200),
                                       promise(NODE_NAME, 100)));

        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 200),
                                       promise(null, 300)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(null, 300),
                                       promise(NODE_NAME, 200)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 300),
                                       promise(NODE_NAME, 100),
                                       promise(NODE_NAME, 200)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 200),
                                       promise(NODE_NAME, 100),
                                       promise(null, 300)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 200),
                                       promise(null, 300),
                                       promise(NODE_NAME, 100)));
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(null, 300),
                                       promise(NODE_NAME, 200),
                                       promise(NODE_NAME, 100)));
    }

    /** Both arbiters should be ignored. */
    @Test
    public void testPhase2TwoArbs() {
        assertEquals(NODE_NAME,
                     choosePhase2Value(promise(NODE_NAME, 100),
                                       promise(null, 300),
                                       promise(null, 400),
                                       promise(NODE_NAME, 200)));
    }

    /* Utilities */

    private String choosePhase2Value(Promise... promises) {
        final List<MessageExchange> exchangeList = new ArrayList<>();
        for (final Promise p : promises) {
            exchangeList.add(messageExchange(p));
        }
        final Set<MessageExchange> exchanges = sortedSet(exchangeList);
        final Value result = proposer.choosePhase2Value(exchanges);
        if (result instanceof MasterValue) {
            return ((MasterValue) result).getNodeName();
        }
        return null;
    }

    /**
     * Create a promise with the specified node as the suggested master and the
     * DTVLSN for ranking.  Specify a null nodeName for a promise from an
     * arbiter.
     */
    private Promise promise(String nodeName, long dtvlsn) {
        return protocol.new Promise(proposal,
                                    null /* value */,
                                    masterValue(nodeName), /* suggestion */
                                    new Ranking(dtvlsn, 0),
                                    1, /* priority*/
                                    0, /* logVersion */
                                    JEVersion.CURRENT_VERSION);
    }

    private MessageExchange messageExchange(Promise promise) {
        final MessageExchange msgExchange = protocol.new MessageExchange(
            socketAddress /* target */, "service1",
            protocol.new Propose(proposal));
        msgExchange.setResponseMessage(promise);
        return msgExchange;
    }

    /**
     * Create a master value for a node, or for an arbiter if nodeName is
     * null.
     */
    private static MasterValue masterValue(String nodeName) {
        return new MasterValue(nodeName, 5000,
                               (nodeName == null) ?
                               NameIdPair.NULL :
                               new NameIdPair(nodeName, NODE_ID));
    }

    /** Create a set with the elements and order specified by a list. */
    private static <E> SortedSet<E> sortedSet(final List<E> list) {
        final SortedSet<E> set = new TreeSet<>(
            new Comparator<E>() {
                @Override
                public int compare(E x, E y) {
                    return list.indexOf(x) - list.indexOf(y);
                }
            });
        for (E e : list) {
            set.add(e);
        }
        return set;
    }
}
