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

import java.util.Arrays;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Proposer.ProposalParser;
import com.sleepycat.je.rep.elections.Protocol.Promise;
import com.sleepycat.je.rep.elections.Protocol.StringValue;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.elections.Protocol.ValueParser;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.TextProtocol;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.Message;
import com.sleepycat.je.rep.impl.TextProtocolTestBase;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;

public class ProtocolTest extends TextProtocolTestBase {

    private TestProtocol protocol = null;
    private DataChannelFactory channelFactory;

    @Override
    @Before
    public void setUp()
        throws Exception {

        channelFactory =
            DataChannelFactoryBuilder.construct(
                RepTestUtils.readRepNetConfig(), GROUP_NAME);

        protocol = new TestProtocol(TimebasedProposalGenerator.getParser(),
                                new ValueParser() {
                                    @Override
                                    public Value parse(String wireFormat) {
                                        if ("".equals(wireFormat)) {
                                            return null;
                                        }
                                        return new StringValue(wireFormat);

                                    }
                                },
                                GROUP_NAME,
                                new NameIdPair(NODE_NAME, 1),
                                null,
                                channelFactory);
        protocol.updateNodeIds(new HashSet<Integer>
                               (Arrays.asList(new Integer(1))));
    }

    @Override
    @After
    public void tearDown() {
        protocol = null;
    }

    @Override
    protected Message[] createMessages() {
        TimebasedProposalGenerator proposalGenerator =
            new TimebasedProposalGenerator();
        Proposal proposal = proposalGenerator.nextProposal();
        Value value = new Protocol.StringValue("test1");
        Value svalue = new Protocol.StringValue("test2");
        Message[] messages = new Message[] {
                protocol.new Propose(proposal),
                protocol.new Accept(proposal, value),
                protocol.new Result(proposal, value),
                protocol.new Shutdown(),
                protocol.new MasterQuery(),

                protocol.new Reject(proposal),
                protocol.new Promise(proposal, value, svalue,
                                     new Ranking(100, 101), 1,
                                     LogEntryType.LOG_VERSION,
                                     JEVersion.CURRENT_VERSION),
                protocol.new Accepted(proposal, value),
                protocol.new MasterQueryResponse(proposal, value)
        };

        return messages;
    }

    @Override
    protected TextProtocol getProtocol() {
        return protocol;
    }

    @Test
    public void testPromiseCompatibility() throws InvalidMessageException {
        TimebasedProposalGenerator proposalGenerator =
            new TimebasedProposalGenerator();
        Proposal proposal = proposalGenerator.nextProposal();
        Value value = new Protocol.StringValue("test1");
        Value svalue = new Protocol.StringValue("test2");
        Promise prom =
            protocol.new Promise(proposal, value, svalue,new
                                 Ranking(100, 101), 1,
                                 LogEntryType.LOG_VERSION,
                                 JEVersion.CURRENT_VERSION);
        assertEquals(101, prom.getSuggestionRanking().minor);

        final String wireFormatNew = prom.wireFormat();

        int tieBreaker = wireFormatNew.lastIndexOf(TextProtocol.SEPARATOR);
        final String wireFormatOld = wireFormatNew.substring(0, tieBreaker);

        /* Simulate new node reading old Promise format. */
        Promise prom2 = (Promise)protocol.parse(wireFormatOld);

        assertEquals(Ranking.UNINITIALIZED.major,
                     prom2.getSuggestionRanking().minor);

        TestProtocol.OldPromise oldProm = protocol.new
            OldPromise(proposal, value, svalue, 100, 1,
                       LogEntryType.LOG_VERSION,
                       JEVersion.CURRENT_VERSION);

        /* Simulate old node reading old and new promise formats. */
        protocol.replacePromise();

        assertEquals(oldProm.wireFormat(), wireFormatOld);
        TestProtocol.OldPromise oldProm1 =
            (TestProtocol.OldPromise)protocol.parse(wireFormatOld);
        TestProtocol.OldPromise oldProm2 =
            (TestProtocol.OldPromise)protocol.parse(wireFormatNew);

        /* verify they check out equal. */
        assertEquals(oldProm1, oldProm2);
    }


    /**
     * Subclass of Protocol to facilitate compatibility testing with the
     * old definition of Promise.
     */
    class TestProtocol extends Protocol {

        /* An instance of ProposalParser used to de-serialize proposals */
        private final ProposalParser proposalParser;

        /* An instance of ValueParser used to de-serialize values */
        private final ValueParser valueParser;

        public TestProtocol(ProposalParser proposalParser,
            ValueParser valueParser,
            String groupName,
            NameIdPair nameIdPair,
            RepImpl repImpl,
            DataChannelFactory channelFactory) {
            super(proposalParser, valueParser, groupName, nameIdPair,
                  repImpl, channelFactory);
            this.proposalParser = proposalParser;
            this.valueParser = valueParser;
        }

        /**
         * Replace the message associated with "PR", so it's the old one for
         * compatibility testing.
         */
        protected void replacePromise() {
            replaceOp("PR", new MessageOp("PR", OldPromise.class));
        }

        /**
         * Old Promise response message. It's sent in response to a Propose
         * message.
         *
         * The code here has been copied as is from its previous version to
         * facilitate testing.
         */
        public class OldPromise extends ResponseMessage {
            private Proposal highestProposal = null;
            private Value acceptedValue = null;
            private Value suggestion = null;
            private long suggestionWeight = Long.MIN_VALUE;
            private final int priority;
            private int logVersion;
            private JEVersion jeVersion;

            public OldPromise(Proposal highestProposal,
                              Value value,
                              Value suggestion,
                              long suggestionWeight,
                              int priority,
                              int logVersion,
                              JEVersion jeVersion) {
                this.highestProposal = highestProposal;
                this.acceptedValue = value;
                this.suggestion = suggestion;
                this.suggestionWeight = suggestionWeight;
                this.priority = priority;
                this.logVersion = logVersion;
                this.jeVersion = jeVersion;
            }

            public OldPromise(String responseLine, String[] tokens)
                throws InvalidMessageException {

                super(responseLine, tokens);
                highestProposal = proposalParser.parse(nextPayloadToken());
                acceptedValue = valueParser.parse(nextPayloadToken());
                suggestion = valueParser.parse(nextPayloadToken());
                String weight = nextPayloadToken();
                suggestionWeight =
                    "".equals(weight) ?
                    Long.MIN_VALUE :
                    Long.parseLong(weight);
                priority = Integer.parseInt(nextPayloadToken());
                if (getMajorVersionNumber(sendVersion) > 1) {
                    logVersion = Integer.parseInt(nextPayloadToken());
                    jeVersion = new JEVersion(nextPayloadToken());
                }
            }

            @Override
            public MessageOp getOp() {
                return PROMISE;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = super.hashCode();
                result = prime * result + getOuterType().hashCode();
                result = prime * result
                        + ((acceptedValue == null) ? 0 : acceptedValue.hashCode());
                result = prime
                        * result
                        + ((highestProposal == null) ? 0
                                : highestProposal.hashCode());
                result = prime * result + priority;
                result = prime * result
                        + ((suggestion == null) ? 0 : suggestion.hashCode());
                result = prime * result
                        + (int) (suggestionWeight ^ (suggestionWeight >>> 32));

                if (getMajorVersionNumber(sendVersion) > 1) {
                    result += prime* result + logVersion + jeVersion.hashCode();
                }

                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }

                if (!super.equals(obj)) {
                    return false;
                }

                if (getClass() != obj.getClass()) {
                    return false;
                }

                OldPromise other = (OldPromise) obj;
                if (!getOuterType().equals(other.getOuterType())) {
                    return false;
                }

                if (acceptedValue == null) {
                    if (other.acceptedValue != null) {
                        return false;
                    }
                } else if (!acceptedValue.equals(other.acceptedValue)) {
                    return false;
                }

                if (highestProposal == null) {
                    if (other.highestProposal != null) {
                        return false;
                    }
                } else if (!highestProposal.equals(other.highestProposal)) {
                    return false;
                }

                if (priority != other.priority) {
                    return false;
                }

                if (getMajorVersionNumber(sendVersion) > 1) {
                    if (logVersion != other.logVersion) {
                        return false;
                    }

                    if (jeVersion.compareTo(other.jeVersion) != 0) {
                        return false;
                    }
                }

                if (suggestion == null) {
                    if (other.suggestion != null) {
                        return false;
                    }
                } else if (!suggestion.equals(other.suggestion)) {
                    return false;
                }

                if (suggestionWeight != other.suggestionWeight) {
                    return false;
                }

                return true;
            }

            @Override
            public String wireFormat() {
                String line =
                    wireFormatPrefix() +
                    SEPARATOR +
                    ((highestProposal != null) ?
                     highestProposal.wireFormat() :
                     "") +
                    SEPARATOR +
                    ((acceptedValue != null) ? acceptedValue.wireFormat() : "") +
                    SEPARATOR +
                    ((suggestion != null) ?  suggestion.wireFormat() : "") +
                    SEPARATOR +
                    ((suggestionWeight == Long.MIN_VALUE) ?
                     "" :
                     Long.toString(suggestionWeight)) +
                     SEPARATOR +
                     priority;

               if (getMajorVersionNumber(sendVersion) > 1) {
                  line += SEPARATOR + logVersion + SEPARATOR +
                          jeVersion.toString();
               }

               return line;
            }

            Proposal getHighestProposal() {
                return highestProposal;
            }

            Value getAcceptedValue() {
                return acceptedValue;
            }

            Value getSuggestion() {
                return suggestion;
            }

            long getSuggestionRanking() {
                return suggestionWeight;
            }

            int getPriority() {
                return priority;
            }

            int getLogVersion() {
                return logVersion;
            }

            JEVersion getJEVersion() {
                return jeVersion;
            }

            private Protocol getOuterType() {
                return TestProtocol.this;
            }
        }

    }
}
