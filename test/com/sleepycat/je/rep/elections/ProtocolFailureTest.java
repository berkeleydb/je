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

import static com.sleepycat.je.rep.impl.TextProtocol.SEPARATOR;
import static com.sleepycat.je.rep.impl.TextProtocol.SEPARATOR_REGEXP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepTestBase;
import com.sleepycat.je.rep.impl.TextProtocol;
import com.sleepycat.je.rep.impl.TextProtocol.Message;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.TextProtocol.TOKENS;
import com.sleepycat.je.utilint.TestHook;

/**
 * Check rep node resilience when there are various types of protocol message
 * format corruption or semantic inconsistencies. The intent here is to confine
 * the failure as much as possible and prevent the environment itself from
 * failing. The tests here are pretty basic for the most part. We can augment
 * them as we find more scenarios of interest.
 */
public class ProtocolFailureTest extends RepTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        groupSize = 3;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testBadVersionReq() throws InterruptedException {
        testInternal(TOKENS.VERSION_TOKEN, RequestMessage.class);
    }

    @Test
    public void testBadVersionResp() throws InterruptedException {
        testInternal(TOKENS.VERSION_TOKEN, ResponseMessage.class);
    }

    @Test
    public void testBadNameReq() throws InterruptedException {
        testInternal(TOKENS.NAME_TOKEN, RequestMessage.class);
    }

    @Test
    public void testBadNameResp() throws InterruptedException {
        testInternal(TOKENS.NAME_TOKEN, ResponseMessage.class);
    }

    @Test
    public void testBadId() throws InterruptedException {
        testInternal(TOKENS.ID_TOKEN, RequestMessage.class);
    }

    @Test
    public void testBadIdResp() throws InterruptedException {
        testInternal(TOKENS.ID_TOKEN, ResponseMessage.class);
    }

    @Test
    public void testBadOp() throws InterruptedException {
        testInternal(TOKENS.OP_TOKEN, RequestMessage.class);
    }

    @Test
    public void testBadOpResp() throws InterruptedException {
        testInternal(TOKENS.OP_TOKEN, ResponseMessage.class);
    }

    @Test
    public void testBadPayloadRequest() {
        /*
         * Future: Need custom tests and custom code for bad payload requests.
         */
        // testInternal(TOKENS.FIRST_PAYLOAD_TOKEN, RequestMessage.class);
    }

    @Test
    public void testBadPayloadResp() throws InterruptedException {
        testInternal(TOKENS.FIRST_PAYLOAD_TOKEN, ResponseMessage.class);
    }

    /**
     * Tests damage to a specific field as identified by the message token for
     * a specific type of message.
     *
     * @param testToken the token to be munged
     * @param testMessageType the type of messages (request/response) to be
     * munged.
     */
    private void testInternal(TOKENS testToken,
                              Class<? extends Message> testMessageType)
        throws InterruptedException {
        createGroup(3);
        closeNodes(repEnvInfo); /* Close all nodes to eliminate masters */

        repEnvInfo[0].getRepConfig().
        setConfigParam("je.rep.envUnknownStateTimeout", "1 ms");
        repEnvInfo[0].openEnv();

        final Protocol protocol =
            repEnvInfo[0].getRepNode().getElections().getProtocol();

        final Set<String> modOps = protocol.getOps(testMessageType);

        final SerDeHook  corruptOpHook = new SerDeHook(testToken, modOps);
        TextProtocol.setSerDeHook(corruptOpHook);
        repEnvInfo[2].getRepConfig().
            setConfigParam("je.rep.envUnknownStateTimeout", "5 s");
        repEnvInfo[2].openEnv();

        /* Verify that hook was called by the test. */
        assertTrue(corruptOpHook.count > 0);

        /*
         * Verify that nodes are up and standing, that is, not DETACHED and
         * there's no master or replica since the election messages were munged.
         */
        assertEquals(State.UNKNOWN, repEnvInfo[2].getEnv().getState());
        assertEquals(State.UNKNOWN, repEnvInfo[0].getEnv().getState());

        /* Start sending valid messages. */
        TextProtocol.setSerDeHook(null);

        assertTrue(findMasterWait(60000, repEnvInfo[0], repEnvInfo[2])
                   != null);
    }

    /**
     * The Hook that mungs messages. Note that since the hook is invoked from
     * a method that's re-entrant, its methods must be re-entrant as well,
     */
    private class SerDeHook implements TestHook<String> {
        final TOKENS testToken;

        /* Number of times messages were actually modified. */
        volatile int count;

        /*
         * Messages with this op will be modified at the token identified by
         * testToken.
         */
        final Set<String> modOps;

        /*
         * Use ThreadLocal to make the doHook and getValue methods re-entrant
         */
        final ThreadLocal<String> messageLine = new ThreadLocal<>();


        /**
         * Hook constructor
         * @param testToken the token that must be munged within the message
         * @param modOps the ops associated with messages that should be
         * munged. All other messages are left intact.
         */
        public SerDeHook(TOKENS testToken,
                         Set<String> modOps) {
            super();
            this.testToken = testToken;
            this.modOps = Collections.unmodifiableSet(modOps);
        }

        @Override
        public void hookSetup() {
            throw new UnsupportedOperationException("Method not implemented: " +
                                                    "hookSetup");
        }

        @Override
        public void doIOHook() throws IOException {
            throw new UnsupportedOperationException("Method not implemented: " +
                                                    "doIOHook");
        }

        @Override
        public void doHook() {
            throw new UnsupportedOperationException("Method not implemented: " +
                                                    "doHook");
        }

        @Override
        public void doHook(String origMessage) {

            if (origMessage == null) {
                messageLine.set(null);
                return;
            }

            final String[] tokens = origMessage.split(SEPARATOR_REGEXP);
            if (testToken.ordinal() >= tokens.length) {
                /* The messages does not have any payload. */
                messageLine.set(origMessage);
                return;
            }

            final String opToken = tokens[TOKENS.OP_TOKEN.ordinal()];
            if (!modOps.contains(opToken)) {
                /* Not in the set of modifiable messages */
                messageLine.set(origMessage);
                return;
            }

            /*
             * Modify the token. The message format is:
             * <version>|<name>|<id>|<op>|<op-specific payload>
             */
            count++;

            final String token = tokens[testToken.ordinal()];

            final String leadSeparator =
                (testToken.ordinal() > 0) ? SEPARATOR : "";
            final String trailingSeparator =
                (testToken.ordinal() == (tokens.length - 1)) ?
                "" : SEPARATOR ;
            final String badToken =
                leadSeparator + "bad" + token + trailingSeparator;

            String newMessage = origMessage.
                replace(leadSeparator + token + trailingSeparator, badToken);
            assertTrue(!messageLine.equals(origMessage));

            if (testToken.equals(TOKENS.FIRST_PAYLOAD_TOKEN)) {
                /* Mung the rest of the payload. */
                int payloadStart = newMessage.indexOf(badToken);
                newMessage =
                    newMessage.substring(0, payloadStart) + leadSeparator +
                    "badPayload";
            }
            messageLine.set(newMessage);
        }

        @Override
        public String getHookValue() {
            try {
                return messageLine.get();
            } finally {
                messageLine.remove();
            }
        }
    }
}
