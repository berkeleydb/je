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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import com.sleepycat.je.rep.ReplicationNetworkConfig;
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

import org.junit.Test;

/**
 * This test is intended to check the behavior of the SSLDataChannel class when
 * multiple threads access the channel concurrently.
 *
 * TODO: refactor some of this to add reusability.
 */
public class SSLMultiThreadTest {

    private final String hostname = "localhost";
    private final PrintStream log = System.out;
    private int port = 0;
    private InetSocketAddress socketAddress;
    private DataChannelFactory channelFactory;
    
    /**
     * This test exercises a case that can occur with JE HA feeders, where there
     * are separate threads for read and write.  When operating in blocking
     * mode, a case was found that the a blocked writer would prevent a
     * reader from reading, leading to a cross-process deadlock.
     */
    @Test
    public void TestWritesBeforeReads()
        throws IOException {

        channelFactory = createSSLFactory(createSSLFactoryProps());
        findFreePort();
        socketAddress = new InetSocketAddress(hostname, port);

        /*
         * A place to put newly accepted incoming channels.
         */
        final List<DataChannel> channelList =
            Collections.synchronizedList(new ArrayList<DataChannel>());

        ConnectionHandler handler = new ConnectionHandler() {
                @Override
                public void newChannel(DataChannel channel) {
                    channelList.add(channel);
                }};

        ListenerTask listener = spawnListener(handler);

        /*
         * Allow time for the Listener thread to get started and to bind
         * its listen socket.
         */
        delay(500);

        final DataChannel clientChannel =
            channelFactory.connect(socketAddress,
                                   new ConnectOptions().
                                   setBlocking(true));

        /*
         * Wait for the handler to push the channel on our list. Provide a
         * limit on the number of iterations to avoid a full test timeout
         * in the event something goes wrong.
         */
        for (int i = 0; i < 100 && channelList.isEmpty(); i++) {
            delay(100);
        }

        final DataChannel serverChannel = channelList.get(0);

        /* switch to blocking mode */
        serverChannel.getSocketChannel().configureBlocking(true);
        
        /*
         * This is how much data will be written from each end.  This needs
         * to be large enough to fill the TCP window buffers plus SSL internal
         * buffers.  More is better still, but the key point is to ensure that
         * writers are blocked on writing.
         */
        final int writeMB = 10;
        final int writeAmount = writeMB * 1024 * 1024;

        WriteTask clientWriter =
            spawnWriter("client", clientChannel, writeAmount);
        WriteTask serverWriter =
            spawnWriter("server", serverChannel, writeAmount);

        /* Pause to allow the writers to fill their output buffers */
        delay(2000);

        /*
         * check that we've supplied enough to let the writers get
         * stuck
         */
        final Stats clStats = clientWriter.getStats();
        assertTrue("client didn't write any", clStats.getBytes() > 0);
        assertTrue("client wrote all", clStats.getBytes() < writeAmount);

        final Stats srStats = serverWriter.getStats();
        assertTrue("server didn't write any", srStats.getBytes() > 0);
        assertTrue("server wrote all", srStats.getBytes() < writeAmount);

        /* Now fire up the readers */
        final ReadTask clientReader = spawnReader("client", clientChannel);
        final ReadTask serverReader = spawnReader("server", serverChannel);

        boolean completed = false;

        final long startTime = System.currentTimeMillis();
        while (true) {

            if (clientReader.getStats().getBytes() >= writeAmount &&
                serverReader.getStats().getBytes() >= writeAmount) {
                completed = true;
                break;
            }

            /*
             * Allow up to 1 second per MB to let the transmission complete
             */
            if ((System.currentTimeMillis() - startTime) > writeMB * 1000L) {
                break;
            }

            delay(100);
        }

        assertTrue("didn't complete successfully", completed);

        /*
         * Tell the readers and writers that their work is done so that they
         * don't complain when they see an exception when the sockets get
         * shut down.
         */
        clientReader.setDone();
        clientWriter.setDone();
        serverReader.setDone();
        serverWriter.setDone();

        /* Close the client end of the socket */
        clientChannel.close();

        /* Tell the listener to quit listening */
        listener.exitLoop();

        /* wait for the readers and writers to stop */
        joinThread(clientReader.getThread());
        joinThread(clientWriter.getThread());
        joinThread(serverReader.getThread());
        joinThread(serverWriter.getThread());

        /* wait for the listner to stop */
        joinThread(listener.getThread());
    }

    private void joinThread(Thread t) {
        while (true) {
            try {
                t.join();
                return;
            } catch (InterruptedException ie) {
            }
        }
    }

    private void delay(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
        }
    }

    private Properties createSSLFactoryProps() {
        /* Set up properties to get the pre-built keystore and truststore */
        Properties props = new Properties();
        RepTestUtils.setUnitTestSSLProperties(props);
        return props;
    }

    private static DataChannelFactory createSSLFactory(Properties props) {
        final InstanceParams params = makeParams(
            ReplicationNetworkConfig.create(props), null);
        return new SSLChannelFactory(params);
    }

    private static InstanceParams makeParams(ReplicationNetworkConfig config,
                                      String paramVal) {
        return new InstanceParams(
            new InstanceContext(config,
                                new ChannelLoggerFactory(
                                    null, /* envImpl */
                                    new ChannelFormatter("SSLChannelTest"))),
            null);
    }

    ListenerTask spawnListener(ConnectionHandler handler) {

        ListenerTask listenerTask = new ListenerTask(handler);
        Thread listenerThread = new Thread(listenerTask);
        listenerThread.setName("Server Listener");
        listenerTask.setThread(listenerThread);
        listenerThread.start();
        return listenerTask;
    }

    ReadTask spawnReader(String owner, DataChannel channel) {

        ReadTask readTask = new ReadTask(owner, channel);
        Thread readThread = new Thread(readTask);
        readThread.setName(owner + "Reader");
        readTask.setThread(readThread);
        readThread.start();
        return readTask;
    }

    WriteTask spawnWriter(String owner, DataChannel channel, int writeAmt) {

        WriteTask writeTask = new WriteTask(owner, channel, writeAmt);
        Thread writeThread = new Thread(writeTask);
        writeThread.setName(owner + "Writer");
        writeTask.setThread(writeThread);
        writeThread.start();
        return writeTask;
    }

    class Stats {
        private long bytes;
        private long packets;
        private int problems;

        Stats() {
            bytes = 0;
            packets = 0;
        }

        Stats(long bytes, long packets) {
            this.bytes = bytes;
            this.packets = packets;
        }

        synchronized Stats copy() {
            return new Stats(bytes, packets);
        }

        synchronized void update(int packetSize) {
            packets++;
            bytes += packetSize;
        }

        synchronized void hadProblem() {
            problems += 1;
        }

        synchronized int getProblems() {
            return problems;
        }

        synchronized long getPackets() {
            return packets;
        }

        synchronized long getBytes() {
            return bytes;
        }

        Stats subtract(Stats other) {
            long newBytes = bytes - other.bytes;
            long newPackets = packets - other.packets;
            return new Stats(newBytes, newPackets);
        }
    }

    class BasicTask {

        private volatile Thread executingThread = null;

        BasicTask() {
        }

        Thread getThread() {
            return executingThread;
        }
        void setThread(Thread t) {
            executingThread = t;
        }
    }

    class IOTask extends BasicTask {

        protected final Random random = new Random();
        protected final DataChannel channel;
        protected final Stats stats = new Stats();
        protected final String owner;
        protected volatile boolean taskDone;

        IOTask(String owner, DataChannel channel) {
            this.owner = owner;
            this.channel = channel;
            this.taskDone = false;
        }

        void setDone() {
            taskDone = true;
        }

        Stats getStats() {
            return stats.copy();
        }
    }

    class ReadTask extends IOTask implements Runnable{

        private int currPktNum = 0;
        private int currByteNum = 0;

        ReadTask(String owner, DataChannel channel) {
            super(owner, channel);
        }

        public void run() {

            /*
             * packet# + bytecount
             */
            final int HDR_SIZE = 8;
            ByteBuffer hdrBuf = ByteBuffer.allocate(HDR_SIZE);

            final int MAX_DATA_SIZE = 0x10000;
            byte[] msgData = new byte[MAX_DATA_SIZE];
            boolean done = false;

            try {
                while (!done) {
                    /* Receive a packet */
                    while (hdrBuf.remaining() > 0) {
                        if (channel.read(hdrBuf) < 0) {
                            done = true;
                            break;
                        }
                    }
                    if (done) {
                        break;
                    }
                    hdrBuf.flip();
                    int pktNum = hdrBuf.getInt();
                    int pktCount = hdrBuf.getInt();
                    hdrBuf.compact();

                    if (pktNum != currPktNum) {
                        fatalCorruption(owner + ": packetNumber mismatch");
                    }

                    if (pktCount > MAX_DATA_SIZE) {
                        fatalCorruption(owner + ": illegal packet size");
                    }

                    ByteBuffer msgBuf = ByteBuffer.allocate(pktCount);
                    while (msgBuf.remaining() > 0) {
                        channel.read(msgBuf);
                    }

                    msgBuf.flip();
                    msgBuf.get(msgData, 0, pktCount);

                    for (int i = 0; i < pktCount; i++) {
                        if (msgData[i] != (byte)(currByteNum + i)) {
                            fatalCorruption(owner + ": data content mismatch");
                        }
                    }
                    currByteNum += pktCount;
                    currPktNum++;

                    stats.update(pktCount);
                }
            } catch (IOException ioe) {
                if (!taskDone) {
                    log.println("Read task got IOException: " + ioe);
                    ioe.printStackTrace(log);
                }
            }
        }
    }

    class WriteTask extends IOTask implements Runnable {

        private int currPktNum = 0;
        private int currByteNum = 0;
        private final int writeAmount;

        WriteTask(String owner, DataChannel channel, int writeAmount) {
            super(owner, channel);
            this.writeAmount = writeAmount;
        }

        public void run() {

            /*
             * packet# + bytecount
             */
            final int HDR_SIZE = 8;
            final int MAX_DATA_SIZE = 0x10000;
            byte[] msgData = new byte[MAX_DATA_SIZE + HDR_SIZE];
            final Random random = new Random();
            int bytesWritten = 0;

            try {
                while (bytesWritten < writeAmount) {
                    int pktSize = (random.nextInt() & 0xffff) + 1;

                    if (pktSize > (writeAmount - bytesWritten)) {
                        pktSize = (writeAmount - bytesWritten);
                    }

                    /* pktBrk is the chunk size for splitting the message */
                    int pktBrk = random.nextInt() & 0x1ffff;

                    if (pktBrk < HDR_SIZE) {
                        pktBrk = HDR_SIZE;
                    } else if (pktBrk > pktSize + HDR_SIZE) {
                        pktBrk = pktSize + HDR_SIZE;
                    }

                    for (int i = 0; i < pktSize; i++) {
                        msgData[i] = (byte) (currByteNum + i);
                    }
                    currByteNum += pktSize;

                    ByteBuffer buffer = ByteBuffer.allocate(pktBrk);
                    buffer.putInt(currPktNum++);
                    buffer.putInt(pktSize);

                    int sent = 0;
                    while (sent < pktSize) {
                        int toSend = pktSize - sent;
                        int rem = buffer.remaining();
                        if (rem > 0) {
                            if (rem > toSend) {
                                /* can't happen in the first pass */
                                buffer = ByteBuffer.allocate(toSend);
                            } else if (toSend > rem) {
                                toSend = rem;
                            }
                            buffer.put(msgData, sent, toSend);
                            sent += toSend;
                        }
                        buffer.flip();
                        channel.write(buffer);
                        buffer.compact();

                    }
                    stats.update(pktSize);
                    bytesWritten += pktSize;
                }
            } catch (IOException ioe) {
                if (!taskDone) {
                    log.println("Write task got IOException: " + ioe);
                }
            }
        }
    }

    interface ConnectionHandler {
        /**
         * Called by the listener task when an new incoming connection is
         * accepted.  The underlying socket channel is in non-blocking mode.
         */
        void newChannel(DataChannel channel);
    }

    class ListenerTask extends BasicTask implements Runnable {

        /*
         * The selector that watches for accept events on the server socket and
         * on subsequent read events.
         */
        private Selector selector;

        /* The server socket channel */
        private ServerSocketChannel serverChannel;

        private volatile boolean exit;

        private ConnectionHandler handler;

        ListenerTask(ConnectionHandler handler) {
            this.handler = handler;
        }

        void exitLoop() {
            exit = true;
        }

        public void run() {
            initServerSocket();
            processIncoming();
            closeServerSocket();
        }

        void initServerSocket() {
            try {
                serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(false);
                selector = Selector.open();
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);

                ServerSocket acceptSocket = serverChannel.socket();
                
                /* No timeout */
                acceptSocket.setSoTimeout(0);
                acceptSocket.bind(socketAddress);

            } catch (IOException ioe) {
                log.println("Error initializing server socket");
                ioe.printStackTrace();
            }
        }

        void closeServerSocket() {
            try {
                serverChannel.socket().close();
                selector.close();
            } catch (IOException ioe) {
                log.println("Error closing server socket");
                ioe.printStackTrace();
            }
        }

        void processIncoming() {

            while (!exit) {
                try {
                    int result = selector.select(10L);

                    if (result == 0) {
                        continue;
                    }
                } catch (IOException e) {
                    log.println("Server socket exception " + e.getMessage());
                }

                Set<SelectionKey> skeys = selector.selectedKeys();
                for (SelectionKey key : skeys) {
                    switch (key.readyOps()) {

                        case SelectionKey.OP_ACCEPT:
                            processAccept();
                            break;

                        default:
                            log.println(
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
                handler.newChannel(dataChannel);
            } catch (IOException e) {
                log.println("Server accept exception: " + e.getMessage());
                try {
                    socketChannel.close();
                } catch (IOException ioe) {
                }
            }
        }
    }

    private void fatalCorruption(String msg) {
        log.println("fatal corruption encountered: " + msg);
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
    }

    private void findFreePort() {
        final FreePortLocator locator =
            new FreePortLocator(hostname, 5000, 6000);
        port = locator.next();
    }
}
