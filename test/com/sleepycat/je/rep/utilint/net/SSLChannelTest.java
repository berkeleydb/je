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

package com.sleepycat.je.rep.utilint.net;

import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_CLASS;
import static
    com.sleepycat.je.rep.ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_PARAMS;
import static com.sleepycat.je.rep.ReplicationSSLConfig.SSL_PROTOCOLS;
import static com.sleepycat.je.rep.ReplicationSSLConfig.SSL_CIPHER_SUITES;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;
import com.sleepycat.utilint.StringUtils;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.net.PasswordSource;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.net.InstanceContext;
import com.sleepycat.je.rep.net.InstanceParams;
import com.sleepycat.je.rep.utilint.FreePortLocator;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.net.SSLChannelFactory;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder.ChannelFormatter;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder.ChannelLoggerFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test is intended to check the behavior of the SSLDataChannel class.
 * It can serve as both a unit test and as a stress test.  When run normally,
 * it runs as a unit test, completing in about 1 minute.  When run with the
 * ant argument -Dlongtest=true, the length of the streams and number of streams
 * are both increased, for a significantly longer run time.
 */
public class SSLChannelTest extends TestBase {

    /* The socket on which the dispatcher is listening */
    private static InetSocketAddress socketAddress = null;

    /*
     * The selector that watches for accept events on the server socket and
     * on subsequent read events.
     */
    private static Selector selector;

    /* The server socket channel */
    private static ServerSocketChannel serverChannel;

    /* Packet direction */
    final static int TO_SERVER = 0;
    final static int TO_CLIENT = 1;

    /* Number of iterations at each test case */
    private static int N_ITERS = 5;

    /* Multiplier for number of packets per stream */
    private static int STREAM_MULTIPLIER = 1;

    private final static String SHOW_THRUPUT = "test.showThruput";
    private final static String SHOW_ERRORS = "test.showErrors";
    private boolean showThruput = false;
    private boolean showErrors = false;
    private static long randomSeed = System.nanoTime();

    /* The IBM JVM names cipher suites differently. */
    private static final String SUPPORTED_CIPHER_SUITE =
        System.getProperty("java.vendor", "unknown").startsWith("IBM") ?
        "SSL_RSA_WITH_AES_128_CBC_SHA256" :
        "TLS_RSA_WITH_AES_128_CBC_SHA256";

    /**
     * A tuple of a DataChannel and an associated ByteBuffer
     */
    static class ChannelBuffer {

        private final DataChannel dataChannel;
        private ByteBuffer byteBuffer;

        public ChannelBuffer(DataChannel dataChannel, ByteBuffer byteBuffer) {
            this.dataChannel = dataChannel;
            this.byteBuffer = byteBuffer;
        }

        DataChannel getChannel() {
            return dataChannel;
        }

        ByteBuffer getBuffer() {
            return byteBuffer;
        }
        void setBuffer(ByteBuffer newByteBuffer) {
            this.byteBuffer = newByteBuffer;
        }
    }

    public SSLChannelTest() {
        showThruput = Boolean.getBoolean(SHOW_THRUPUT);
        showErrors = Boolean.getBoolean(SHOW_ERRORS);
    }

    @BeforeClass
    public static void setUpOnce()
        throws Exception {

        if (SharedTestUtils.runLongTests()) {
            N_ITERS = 100;
            STREAM_MULTIPLIER = 10;
        }

        initServerSocket();
        System.out.println("Random seed  = " + randomSeed);
    }

    @AfterClass
    public static void shutdownOnce()
        throws Exception {

        closeServerSocket();
    }

    static void initServerSocket() {
        try {
            FreePortLocator locator =
                new FreePortLocator("localhost", 5000, 6000);
            int freePort = locator.next();

            socketAddress = new InetSocketAddress("localhost", freePort);
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            ServerSocket acceptSocket = serverChannel.socket();

            /* No timeout */
            acceptSocket.setSoTimeout(0);
            acceptSocket.bind(socketAddress);
        } catch (IOException ioe) {
            System.out.println("Error initializing server socket");
            ioe.printStackTrace();
        }
    }

    static void closeServerSocket() {
        try {
            serverChannel.socket().close();
            selector.close();
        } catch (IOException ioe) {
            System.out.println("Error closing server socket");
            ioe.printStackTrace();
        }
    }

    class Packet {
        private int direction;
        private int packetSize;
        private int startIdx;

        /* length of the byte buffer array to hold the data */
        private int bufArrLen;
        private int bytesPerBuf;
        private int bytesLastBuf;

        public Packet(int dir, int packetSize, int startIdx, int bufArrLen) {
            assert bufArrLen <= packetSize;
            this.direction = dir;
            this.packetSize = packetSize;
            this.startIdx = startIdx;
            this.bufArrLen = bufArrLen;
            this.bytesPerBuf = packetSize / bufArrLen;
            this.bytesLastBuf = packetSize - (bufArrLen - 1) * bytesPerBuf;
        }

        public boolean toServer() {
            return (direction == TO_SERVER);
        }

        public int getPacketSize() {
            return this.packetSize;
        }

        public int getStartIdx() {
            return this.startIdx;
        }

        public int getBufArrLen() {
            return this.bufArrLen;
        }

        /* Get the byte buffer array. */
        public ByteBuffer[] getBufArr() {
            ByteBuffer[] bufarr = new ByteBuffer[bufArrLen];
            for (int i = 0; i < bufArrLen; ++i) {
                int n = (i == bufArrLen - 1) ? bytesLastBuf : bytesPerBuf;
                bufarr[i] = ByteBuffer.allocate(n);
            }
            return bufarr;
        }

        /* Get the half way index of the array. */
        public int getArrHalfwayIdx() {
            return bufArrLen / 2;
        }

        /**
         * Get the num of bytes in the array from the index 0 (inclusive) to
         * the result of getArrHalfwayIdx(exclusive).
         */
        public int nbytesArrHalf1() {
            return (bufArrLen / 2) * bytesPerBuf;
        }

        /**
         * Get the num of bytes in the array from the index of the result of
         * getArrHalfwayIdx(inclusive) to the end.
         */
        public int nbytesArrHalf2() {
            return (bufArrLen - 1 - bufArrLen / 2) * bytesPerBuf + bytesLastBuf;
        }

        /**
         * Fill the packet data.
         *
         * Buffers are flip to be ready to get after filled.
         */
        public void fill(ByteBuffer[] bufarr) {
            int val = startIdx;
            for (int idx = 0; idx < bufArrLen; ++idx) {
                ByteBuffer buf = bufarr[idx];
                buf.clear();
                for (int i = 0; i < buf.capacity(); ++i) {
                    buf.put((byte)(val++));
                }
                buf.clear();
            }
        }

        /* Check the packet data. */
        public void check(ByteBuffer[] bufarr) {
            int val = startIdx;
            for (int idx = 0; idx < bufArrLen; ++idx) {
                ByteBuffer buf = bufarr[idx];
                buf.clear();
                for (int i = 0; i < buf.capacity(); ++i) {
                    if (((byte)(val++)) != buf.get()) {
                        System.out.println("Data mismatch");
                        return;
                    }
                }
            }
        }
    }

    interface Checker {
        void check(ClientTask client, ServerTask server);
    }

    class BasicChecker implements Checker {
        public void check(ClientTask client, ServerTask server) {
            assertEquals(null, client.getError());
            assertFalse(client.getOtherError());
            assertFalse(server.getOtherError());
            assertTrue(server.channelSecure);
            assertFalse(server.channelTrusted);
        }
    }

    class PeerAuthenticatedChecker implements Checker {
        private boolean expectTrusted;

        PeerAuthenticatedChecker(boolean trusted) {
            expectTrusted = trusted;
        }

        public void check(ClientTask client, ServerTask server) {
            assertEquals(null, client.getError());
            assertFalse(client.getOtherError());
            assertFalse(server.getOtherError());
            assertTrue(server.channelSecure);
            assertEquals(server.channelTrusted, expectTrusted);
        }
    }

    class PeerNotVerifiedChecker implements Checker {
        PeerNotVerifiedChecker() {
        }

        public void check(ClientTask client, ServerTask server) {
            assertTrue(client.getError() != null);
            assertTrue(client.getError().getMessage().contains(
                           "Server identity could not be verified"));
        }
    }

    /**
     * Tests initiation of connections with packet delivery using a
     * sequence of small packets
     */
    @Test
    public void testSmallPackets() throws InterruptedException {
        Random random = new Random(randomSeed);

        final int N_PACKETS = 50;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;
        DataChannelFactory channelFactory =
            createSSLFactory(createSSLFactoryProps());

        runScenario(random, false, channelFactory,
                    N_ITERS, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());
    }

    /**
     * Force thoughput testing to be sure it doesn't bit rot.
     */
    @Test
    public void testBasicThroughput() throws InterruptedException {
        boolean origShowThruput = showThruput;
        try {
            showThruput = true;
            Random random = new Random(randomSeed);

            final int ONE_ITER = 1;
            final int N_PACKETS = 1000;
            final int MIN_PACKET_SIZE = 100;
            final int MAX_PACKET_SIZE = 200;
            DataChannelFactory channelFactory =
                createSSLFactory(createSSLFactoryProps());

            runScenario(random, true, channelFactory,
                        ONE_ITER, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                        new BasicChecker());
        } finally {
            showThruput = origShowThruput;
        }
    }

    /**
     * Tests throughput by sending packets in only one direction with "smallish"
     * packets.
     */
    @Test
    public void testSmallThroughput() throws InterruptedException {
        if (showThruput) {
            Random random = new Random(randomSeed);

            final int N_PACKETS = 100000;
            final int MIN_PACKET_SIZE = 100;
            final int MAX_PACKET_SIZE = 200;
            DataChannelFactory channelFactory =
                createSSLFactory(createSSLFactoryProps());

            runScenario(random, true, channelFactory,
                        N_ITERS, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                        new BasicChecker());
        }
    }

    /**
     * Tests initiation of connections with packet delivery using a
     * sequence of medium sized packets
     */
    @Test
    public void testMediumPackets() throws InterruptedException {
        Random random = new Random(randomSeed);

        final int N_PACKETS = 50;
        final int MIN_PACKET_SIZE = 1000;
        final int MAX_PACKET_SIZE = 5000;
        DataChannelFactory channelFactory =
            createSSLFactory(createSSLFactoryProps());

        runScenario(random, false, channelFactory,
                    N_ITERS, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());
    }

    /**
     * Tests initiation of connections with packet delivery using a
     * sequence of largepackets
     */
    @Test
    public void testLargePackets() throws InterruptedException {
        Random random = new Random(randomSeed);

        final int N_PACKETS = 20;
        final int MIN_PACKET_SIZE = 20000;
        final int MAX_PACKET_SIZE = 50000;
        DataChannelFactory channelFactory =
            createSSLFactory(createSSLFactoryProps());

        runScenario(random, false, channelFactory,
                    N_ITERS, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());
    }

    /**
     * Tests initiation of connections with packet delivery using a
     * sequence of huge packets.
     */
    @Test
    public void testHugePackets() throws InterruptedException {
        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 100000;
        final int MAX_PACKET_SIZE = 500000;
        DataChannelFactory channelFactory =
            createSSLFactory(createSSLFactoryProps());

        runScenario(random, false, channelFactory,
                    N_ITERS, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());
    }

    /**
     * Tests througput by sending packets in only one direction with "huge"
     * packets.
     */
    @Test
    public void testHugeThroughput() throws InterruptedException {
        if (showThruput) {
            Random random = new Random(randomSeed);

            final int N_PACKETS = 100;
            final int MIN_PACKET_SIZE = 100000;
            final int MAX_PACKET_SIZE = 500000;
            DataChannelFactory channelFactory =
                createSSLFactory(createSSLFactoryProps());

            runScenario(random, true, channelFactory,
                        N_ITERS, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                        new BasicChecker());
        }
    }

    /**
     * Tests peer authentication with pattern authentication
     */
    @Test
    public void testPeerPatternAuthentication() throws InterruptedException {

        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;

        /* This version should be trusted */
        DataChannelFactory channelFactory =
            createSSLFactory(addPatternAuthentication(createSSLFactoryProps(),
                                                      "CN=Unit Test",
                                                      "CN=Unit Test"));

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new PeerAuthenticatedChecker(true));

        /* This version should not be trusted */
        channelFactory =
            createSSLFactory(addPatternAuthentication(createSSLFactoryProps(),
                                                      "CN=Not A Unit Test",
                                                      "CN=Unit Test"));

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new PeerAuthenticatedChecker(false));

        /* This case should fail verification */
        channelFactory =
            createSSLFactory(addPatternAuthentication(createSSLFactoryProps(),
                                                      "CN=Unit Test",
                                                      "CN=Not A Unit Test"));

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new PeerNotVerifiedChecker());

        /* Test assymetric keys */
        channelFactory =
            createSSLFactory(
                setClientKeyAlias(
                    addPatternAuthentication(createSSLFactoryProps(),
                                             "CN=Other Test 1",
                                             "CN=Unit Test"),
                    "otherkey1"));

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new PeerAuthenticatedChecker(true));
    }


    /**
     * Tests peer authentication with basic mirror authentication
     */
    @Test
    public void testPeerMirrorAuthentication() throws InterruptedException {

        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;

        /* This version should be trusted */
        DataChannelFactory channelFactory =
            createSSLFactory(addMirrorAuthentication(createSSLFactoryProps()));

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new PeerAuthenticatedChecker(true));

        /* No easy way to force a failure */
    }

    /**
     * Tests server verification with standard verification.
     * This test only tests the negative path currently.
     */
    @Test
    public void testStdHostVerification() throws InterruptedException {

        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;

        /* This case should fail verification */
        DataChannelFactory channelFactory =
            createSSLFactory(addStdVerification(createSSLFactoryProps()));
        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new PeerNotVerifiedChecker());
    }

    /**
     * Tests that we can set cipher suites and protocols.
     * We can't really check whether it used what we want, but we can check
     * including invalid entries doesn't kill things, provided that there
     * is at least one valid entry.
     */
    @Test
    public void testProtosAndCiphers()
        throws InterruptedException {

        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;

        /* First, a successful attempt */

        Properties props = createSSLFactoryProps();
        props.setProperty(SSL_CIPHER_SUITES,
                          "Foo," + SUPPORTED_CIPHER_SUITE + ",Bar");
        props.setProperty(SSL_PROTOCOLS,
                          "Foo,TLSv1.2,,Bar");
        ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.create(props);
        InstanceParams params = makeParams(repNetConfig, null);
        DataChannelFactory channelFactory = new SSLChannelFactory(params);

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());

        /* Now, verify that no valid cipher entries causes an error */

        props = createSSLFactoryProps();
        props.setProperty(SSL_CIPHER_SUITES, "Foo,Bar");
        props.setProperty(SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
        repNetConfig = ReplicationNetworkConfig.create(props);
        params = makeParams(repNetConfig, null);
        try {
            channelFactory = new SSLChannelFactory(params);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }

        /* Verify that no valid protocol entries causes an error */

        props = createSSLFactoryProps();
        props.setProperty(SSL_CIPHER_SUITES, SUPPORTED_CIPHER_SUITE);
        props.setProperty(SSL_PROTOCOLS, "Foo,Bar");
        repNetConfig = ReplicationNetworkConfig.create(props);
        params = makeParams(repNetConfig, null);
        try {
            channelFactory = new SSLChannelFactory(params);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Tests the keystore password source mechanism for keystore access
     */
    @Test
    public void testKeyStorePasswordSource()
        throws InterruptedException {

        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;

        Properties props = createSSLFactoryProps();

        String pwPropName = SSL_KEYSTORE_PASSWORD;
        final String ksPw = props.getProperty(pwPropName);
        props.remove(pwPropName);

        PasswordSource pwSource = new PasswordSource () {
                public char[] getPassword() { return ksPw.toCharArray(); }
            };

        ReplicationSSLConfig repNetConfig = new ReplicationSSLConfig(props);
        repNetConfig.setSSLKeyStorePasswordSource(pwSource);

        InstanceParams params = makeParams(repNetConfig, null);
        DataChannelFactory channelFactory = new SSLChannelFactory(params);

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());

        /* No easy way to force a failure */
    }

    /**
     * Tests the keystore password source loading mechanism for keystore access
     */
    @Test
    public void testLoadedKeyStorePasswordSource()
        throws InterruptedException {

        Random random = new Random(randomSeed);

        final int N_PACKETS = 10;
        final int MIN_PACKET_SIZE = 1;
        final int MAX_PACKET_SIZE = 10;

        Properties props = createSSLFactoryProps();

        String pwPropName = SSL_KEYSTORE_PASSWORD;
        final String ksPw = props.getProperty(pwPropName);
        props.remove(pwPropName);

        props.setProperty(SSL_KEYSTORE_PASSWORD_CLASS,
                          TestPasswordSource.class.getName());
        props.setProperty(SSL_KEYSTORE_PASSWORD_PARAMS,
                          ksPw);

        ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.create(props);

        InstanceParams params = makeParams(repNetConfig, null);
        DataChannelFactory channelFactory = new SSLChannelFactory(params);

        runScenario(random, false, channelFactory,
                    1, N_PACKETS, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
                    new BasicChecker());

        /* No easy way to force a failure */
    }

    public void runScenario(Random random,
                            boolean unidirectional,
                            DataChannelFactory channelFactory,
                            int nIters,
                            int numPackets,
                            int minPacketSize,
                            int maxPacketSize,
                            Checker checker) throws InterruptedException {

        numPackets *= STREAM_MULTIPLIER;
        for (int iter = 0; iter < nIters; iter++) {

            Packet[] packets = new Packet[numPackets];

            int packetIdx = 0;
            for (int pkt = 0; pkt < numPackets; pkt++) {
                int packetSize = minPacketSize +
                    random.nextInt(maxPacketSize-minPacketSize);
                int dir = (pkt == 0 || unidirectional) ?
                    TO_SERVER : random.nextInt(2);
                int bufArrLen = Math.min(packetSize, random.nextInt(8) + 1);
                packets[pkt] = new Packet(dir, packetSize,
                        packetIdx, bufArrLen);
                packetIdx += packetSize;
            }

            long startTime = System.nanoTime();
            runSequence(packets, channelFactory, checker);

            /*
             * To enable, add <sysproperty key="test.showThruput" value="true"/>
             * to the do-junit task
             */
            if (showThruput) {
                long endTime = System.nanoTime();
                long throughput = packetIdx * 1000000000L;
                throughput /= (endTime-startTime);
                System.out.println("stream " + iter + " has " + numPackets +
                                   " packets with " + packetIdx + " bytes: " +
                                   throughput + " bytes/sec");
            }
        }
    }

    void runSequence(Packet[] packets,
                     DataChannelFactory channelFactory,
                     Checker checker) {
        ServerTask server = spawnServer(packets, channelFactory);
        ClientTask client = spawnClient(packets, channelFactory);
        try {
            client.getThread().join();
            server.exitLoop();
            server.getThread().join();
        } catch (InterruptedException ie) {
            System.out.println("Unexpected interruption");
        }
        if (client.getError() != null && showErrors) {
            System.out.println("client error");
            client.getError().printStackTrace();
        }
        checker.check(client, server);
    }

    private Properties createSSLFactoryProps() {
        /* Set up properties to get the pre-built keystore and truststore */
        Properties props = new Properties();
        RepTestUtils.setUnitTestSSLProperties(props);
        return props;
    }

    private Properties addPatternAuthentication(Properties props,
                                                String authPattern,
                                                String hvPattern) {
        props.put(ReplicationSSLConfig.SSL_AUTHENTICATOR_CLASS,
                  SSLDNAuthenticator.class.getName());
        props.put(ReplicationSSLConfig.SSL_AUTHENTICATOR_PARAMS,
                  authPattern);
        props.put(ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS,
                  SSLDNHostVerifier.class.getName());
        props.put(ReplicationSSLConfig.SSL_HOST_VERIFIER_PARAMS,
                  hvPattern);
        return props;
    }

    private Properties setClientKeyAlias(Properties props, String alias) {
        props.put(ReplicationSSLConfig.SSL_CLIENT_KEY_ALIAS, alias);
        return props;
    }

    private Properties addMirrorAuthentication(Properties props) {
        props.put(ReplicationSSLConfig.SSL_AUTHENTICATOR_CLASS,
                  SSLMirrorAuthenticator.class.getName());
        props.put(ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS,
                  SSLMirrorHostVerifier.class.getName());
        return props;
    }

    private Properties addStdVerification(Properties props) {
        props.put(ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS,
                  SSLStdHostVerifier.class.getName());
        return props;
    }

    private DataChannelFactory createSSLFactory(Properties props) {
        final InstanceParams params = makeParams(
            ReplicationNetworkConfig.create(props), null);
        return new SSLChannelFactory(params);
    }

    private InstanceParams makeParams(ReplicationNetworkConfig config,
                                      String paramVal) {
        return new InstanceParams(
            new InstanceContext(config,
                                new ChannelLoggerFactory(
                                    null, /* envImpl */
                                    new ChannelFormatter("SSLChannelTest"))),
            null);
    }

    public final static String INITIAL_MESSAGE_STR = "Hello";
    public final static byte[] INITIAL_MESSAGE =
        StringUtils.toASCII(INITIAL_MESSAGE_STR);
    public final static int INITIAL_BUFFER_SIZE = INITIAL_MESSAGE.length;

    ServerTask spawnServer(
        Packet[] packets, DataChannelFactory channelFactory) {

        ServerTask serverTask = new ServerTask(packets, channelFactory);
        Thread serverThread = new Thread(serverTask);
        serverTask.setThread(serverThread);
        serverThread.start();
        return serverTask;
    }

    ClientTask spawnClient(
        Packet[] packets, DataChannelFactory channelFactory) {

        ClientTask clientTask = new ClientTask(packets, channelFactory);
        Thread clientThread = new Thread(clientTask);
        clientTask.setThread(clientThread);
        clientThread.start();
        return clientTask;
    }

    class BasicTask {

        private Thread executingThread = null;
        private Exception error = null;
        private boolean otherError = false;

        void processPackets(
            DataChannel dataChannel, boolean asServer, Packet[] packets) {

            int midPoint = packets.length/2;
            int pkt = -1;

            try {
                for (pkt = 0; pkt < packets.length; pkt++) {
                    Packet packet = packets[pkt];

                    ByteBuffer[] bufarr = packet.getBufArr();

                    /*
                     * For the server, switch from non-blocking to blocking
                     * half-way through the sequence of packets.
                     */
                    if (asServer && pkt == midPoint) {
                        dataChannel.getSocketChannel().configureBlocking(true);
                    }

                    if (packet.toServer() != asServer) {
                        /* Send a packet */
                        packet.fill(bufarr);

                        /* Write first half */
                        int remaining = packet.nbytesArrHalf1();
                        int halfOffset = packet.getArrHalfwayIdx();
                        while (remaining > 0) {
                            long n = dataChannel.write(
                                    bufarr, 0, halfOffset);
                            remaining -= n;
                        }
                        /* Write the rest */
                        remaining = packet.nbytesArrHalf2();
                        while (remaining > 0) {
                            long n = dataChannel.write(
                                    bufarr, halfOffset,
                                    packet.getBufArrLen() - halfOffset);
                            remaining -= n;
                        }
                        while (dataChannel.flush() !=
                               DataChannel.FlushStatus.DONE) {
                            /* repeat */
                        }
                    } else {
                        /* Receive a packet */
                        /* read first half */
                        int remaining = packet.nbytesArrHalf1();
                        int halfOffset = packet.getArrHalfwayIdx();
                        while (remaining > 0) {
                            long n = dataChannel.read(
                                    bufarr, 0, halfOffset);
                            remaining -= n;
                        }
                        /* read the rest */
                        remaining = packet.nbytesArrHalf2();
                        while (remaining > 0) {
                            long n = dataChannel.read(
                                    bufarr, halfOffset,
                                    packet.getBufArrLen() - halfOffset);
                            remaining -= n;
                        }
                        packet.check(bufarr);
                    }
                }
            } catch (IOException ioe) {
                System.out.println("Basic task as " +
                                   (asServer ? "server" : "client") +
                                   " got IOException: " + ioe +
                                   " on packet " + pkt + " of " +
                                   packets.length);
            }

            try {
                dataChannel.close();
            } catch (IOException ioe) {
                /* ignore */
            }
        }

        Thread getThread() {
            return executingThread;
        }
        void setThread(Thread t) {
            executingThread = t;
        }

        Exception getError() {
            return error;
        }

        void setError(Exception e) {
            error = e;
        }

        void noteOtherError() {
            otherError = true;
        }

        boolean getOtherError() {
            return otherError;
        }

    }

    class ClientTask extends BasicTask implements Runnable {

        Packet[] packets;
        DataChannelFactory channelFactory;

        ClientTask(Packet[] packets, DataChannelFactory channelFactory) {
            this.packets = packets;
            this.channelFactory = channelFactory;
        }

        public void run() {
            DataChannel dataChannel = null;
            try {
                dataChannel = channelFactory.connect(socketAddress,
                                                     new ConnectOptions());
                ByteBuffer buffer =
                    ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
                buffer.put(INITIAL_MESSAGE);
                buffer.flip();
                dataChannel.write(buffer);

                processPackets(dataChannel,
                               false, /* asServer */
                               packets);

            } catch (IOException ioe) {

                if (showErrors) {
                    System.out.println("Client task got IOException: " + ioe);
                }
                setError(ioe);
            }

            try {
                dataChannel.close();
            } catch (IOException ioe) {
                if (getError() == null) {
                    setError(ioe);
                }
            }
        }
    }

    class ServerTask extends BasicTask implements Runnable {

        Packet[] packets;
        DataChannelFactory channelFactory;
        volatile boolean exit = false;
        boolean channelSecure = false;
        boolean channelTrusted = false;

        ServerTask(Packet[] packets, DataChannelFactory channelFactory) {

            this.packets = packets;
            this.channelFactory = channelFactory;
        }

        void exitLoop() {
            exit = true;
        }

        public void run() {
            while (true) {
                try {
                    int result = selector.select(10L);

                    if (exit) {
                        return;
                    }
                    if (result == 0) {
                        continue;
                    }
                } catch (IOException e) {
                    if (showErrors) {
                        System.out.println(
                            "Server socket exception " + e.getMessage());
                    }
                    if (getError() == null) {
                        setError(e);
                    }
                }

                Set<SelectionKey> skeys = selector.selectedKeys();
                for (SelectionKey key : skeys) {
                    switch (key.readyOps()) {

                        case SelectionKey.OP_ACCEPT:
                            processAccept();
                            break;

                        case SelectionKey.OP_READ:
                            boolean proceed = processRead(key);
                            if (!proceed) {
                                break;
                            }
                            key.cancel();
                            ChannelBuffer cb = (ChannelBuffer)key.attachment();

                            this.channelSecure = cb.getChannel().isSecure();
                            this.channelTrusted = cb.getChannel().isTrusted();

                            processPackets(cb.getChannel(),
                                           true, /*asServer*/
                                           packets);
                            try {
                                cb.getChannel().close();
                            } catch (IOException ioe) {
                                if (getError() == null) {
                                    setError(ioe);
                                }
                            }
                            break;

                        default:
                            System.out.println(
                                "Unexpected ops bit set: " + key.readyOps());
                    }
                }
                /* All keys have been processed clear them. */
                skeys.clear();
            }
        }

        void processAccept() {
            SocketChannel socketChannel = null;
            try {
                socketChannel = serverChannel.accept();
                socketChannel.configureBlocking(false);
                DataChannel dataChannel =
                    channelFactory.acceptChannel(socketChannel);

                ChannelBuffer channelBuffer =
                    new ChannelBuffer(dataChannel,
                                      ByteBuffer.allocate(INITIAL_BUFFER_SIZE));

                /* Register the selector with the base SocketChannel */
                socketChannel.register
                    (selector,
                     SelectionKey.OP_READ,
                     channelBuffer);
            } catch (IOException e) {
                if (showErrors) {
                    System.out.println(
                        "Server accept exception: " + e.getMessage());
                }
                try {
                    socketChannel.close();
                } catch (IOException ioe) {
                }
            }
        }

        private boolean processRead(SelectionKey readKey) {
            DataChannel dataChannel = null;
            try {
                ChannelBuffer readChannelBuffer =
                    (ChannelBuffer) readKey.attachment();
                dataChannel = readChannelBuffer.getChannel();
                ByteBuffer readBuffer = readChannelBuffer.getBuffer();
                int readBytes = dataChannel.read(readBuffer);
                if (readBytes < 0 ) {
                    /* Premature EOF */
                    if (showErrors) {
                        System.out.println(
                            "Premature EOF on channel: " +
                            dataChannel + " read() returned: " +
                            readBytes);
                    }
                    dataChannel.close();
                    return false;
                }
                if (readBuffer.remaining() == 0) {
                    readBuffer.flip();
                    String message = StringUtils.fromASCII
                        (readBuffer.array(), 0, INITIAL_MESSAGE.length);
                    if (!message.equals(INITIAL_MESSAGE_STR)) {
                        dataChannel.close();
                        return false;
                    }

                    return true;
                }

                /* Buffer not full as yet, keep reading */
                return false;
            } catch (IOException e) {
                if (showErrors) {
                    System.out.println(
                        "Exception during read: " + e.getMessage());
                }
                try {
                    dataChannel.close();
                } catch (IOException ioe) {
                }

                return false;
            }
        }
    }

    public static class TestPasswordSource implements PasswordSource {
        private final String configuredPassword;

        public TestPasswordSource(InstanceParams params) {
            configuredPassword = params.getClassParams();
        }

        public char[] getPassword() {
            if (configuredPassword == null) {
                return null;
            }
            return configuredPassword.toCharArray();
        }
    }
}
