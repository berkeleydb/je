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

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus;
import static javax.net.ssl.SSLEngineResult.Status;

import java.io.IOException;
import java.net.SocketException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.rep.net.SSLAuthenticator;

/**
 * SSLDataChannel provides SSL-based communications on top of a SocketChannel.
 * We attempt to maintain a degree of compatibility with SocketChannel
 * in terms of request completion semantics.  In particular,
 *    If in blocking mode:
 *       read() will return at least one byte if the buffer has room
 *       write() will write the entire buffer
 *    If in non-blocking mode:
 *       read() and write are not guaranteed to consume or produce anything.
 */
public class SSLDataChannel extends AbstractDataChannel {
    /**
     * The SSLEngine that will manage the secure operations.
     */
    private final SSLEngine sslEngine;

    /**
     * raw bytes received from the SocketChannel - not yet unwrapped.
     */
    private final ByteBuffer netRecvBuffer;

    /**
     * raw bytes to be sent to the wire - already wrapped
     */
    private final ByteBuffer netXmitBuffer;

    /**
     * Bytes unwrapped and ready for application consumption.
     */
    private final ByteBuffer appRecvBuffer;

    /**
     * A dummy buffer used during handshake operations.
     */
    private final ByteBuffer emptyXmitBuffer;

    /**
     * Lock object for protection of appRecvBuffer, netRecvBuffer and SSLEngine
     * unwrap() operations
     */
    private final ReentrantLock readLock = new ReentrantLock();

    /**
     * Lock object for protection of netXmitBuffer and SSLEngine wrap()
     * operations
     */
    private final ReentrantLock writeLock = new ReentrantLock();

    /* Set to true if we have closed the underlying socketChannel */
    private boolean channelClosed = false;

    /*
     * Remember whether we did a closeInbound already.
     */
    private volatile boolean sslInboundClosed = false;

    /**
     * The String identifying the target host that we are connecting to, if
     * this channel was created in client context.
     */
    private final String targetHost;

    /**
     * Possibly null authenticator object used for checking whether the
     * peer for the negotiated session should be trusted.
     */
    private final SSLAuthenticator authenticator;

    /**
     * Possibly null host verifier object used for checking whether the
     * peer for the negotiated session is correct based on the connection
     * target.
     */
    private final HostnameVerifier hostVerifier;

    /**
     * Set to true when a handshake completes and a non-null authenticator
     * acknowledges the session as trusted.
     */
    private volatile boolean peerTrusted = false;

    private final InstanceLogger logger;

    /**
     * Construct an SSLDataChannel given a SocketChannel and an SSLEngine
     *
     * @param socketChannel a SocketChannel over which SSL communcation will
     *     occur.  This should generally be connected, but that is not
     *     absolutely required until the first read/write operation.
     * @param sslEngine an SSLEngine instance that will control the SSL
     *     interaction with the peer.
     */
    public SSLDataChannel(SocketChannel socketChannel,
                          SSLEngine sslEngine,
                          String targetHost,
                          HostnameVerifier hostVerifier,
                          SSLAuthenticator authenticator,
                          InstanceLogger logger) {

        super(socketChannel);
        this.sslEngine = sslEngine;
        this.targetHost = targetHost;
        this.authenticator = authenticator;
        this.hostVerifier = hostVerifier;
        this.logger = logger;
        SSLSession sslSession = sslEngine.getSession();

        /* Determine the required buffer sizes */
        int netBufferSize = sslSession.getPacketBufferSize();
        int appBufferSize = sslSession.getApplicationBufferSize();

        /* allocate the buffers */
        this.emptyXmitBuffer = ByteBuffer.allocate(1);
        this.netXmitBuffer = ByteBuffer.allocate(3*netBufferSize);
        this.appRecvBuffer = ByteBuffer.allocate(2*appBufferSize);
        this.netRecvBuffer = ByteBuffer.allocate(2*netBufferSize);
    }

    /**
     * Is the channel encrypted?
     * @return true if the channel is encrypted
     */
    @Override
    public boolean isSecure() {
        return true;
    }

    /**
     * Is the channel capable of determining peer trust?
     * In this case, we are capable only if the application has configured an
     * SSL authenticator
     *
     * @return true if this data channel is capable of determining trust
     */
    @Override
    public boolean isTrustCapable() {
        return authenticator != null;
    }

    /**
     * Is the channel peer trusted?
     * A channel is trusted if the peer should be treated as authenticated.
     * The meaning of this is context dependent.  The channel will only be
     * trusted if the configured peer authenticator says it should be trusted,
     * so the creator of this SSLDataChannel knows what "trusted" means.
     *
     * @return true if the SSL peer should be trusted
     */
    @Override
    public boolean isTrusted() {
        return peerTrusted;
    }

    /**
     * Read data into the toFill data buffer.
     *
     * @param toFill the data buffer into which data will be read.  This buffer
     *        is expected to be ready for a put.  It need not be empty.
     * @return the count of bytes read into toFill.
     */
    @Override
    public int read(ByteBuffer toFill) throws IOException, SSLException {
        return (int) read(new ByteBuffer[] { toFill }, 0, 1);
    }

    @Override
    public long read(ByteBuffer[] toFill) throws IOException, SSLException {
        return read(toFill, 0, toFill.length);
    }

    @Override
    public long read(ByteBuffer toFill[], int offset, int length)
        throws IOException, SSLException {

        if ((offset < 0) ||
            (length < 0) ||
            (offset > toFill.length - length)) {
            throw new IndexOutOfBoundsException();
        }

        /*
         * Short-circuit if there's no work to be done at this time.  This
         * avoids an unnecessary read() operation from blocking.
         */
        int toFillRemaining = 0;
        for (int i = offset; i < offset + length; ++i) {
            toFillRemaining += toFill[i].remaining();
        }
        if (toFillRemaining <= 0) {
            return 0;
        }

        /*
         * In non-blocking mode, a preceding write operation might not have
         * completed.
         */
        if (!socketChannel.isBlocking()) {
            flush_internal();
        }

        /*
         * If we have data that is already unwrapped and ready to transfer, do
         * it now
         */
        readLock.lock();
        try {
            if (appRecvBuffer.position() > 0) {
                appRecvBuffer.flip();
                final int count = transfer(appRecvBuffer,
                        toFill, offset, length);
                appRecvBuffer.compact();
                return count;
            }
        } finally {
            readLock.unlock();
        }

        int readCount = 0;
        while (readCount == 0) {
            if (sslEngine.isInboundDone()) {
                return -1;
            }

            processAnyHandshakes();

            /* See if we have unwrapped data available */
            readLock.lock();
            try {
                if (appRecvBuffer.position() > 0) {
                    appRecvBuffer.flip();
                    readCount = transfer(appRecvBuffer,
                            toFill, offset, length);
                    appRecvBuffer.compact();
                    break;
                }
            } finally {
                readLock.unlock();
            }

            if (sslEngine.getHandshakeStatus() ==
                HandshakeStatus.NOT_HANDSHAKING) {

                boolean progress = false;
                readLock.lock();
                try {
                    if (netRecvBuffer.position() > 0) {
                        /* There is some data in the network buffer that may be
                         * able to be unwrapped.  If so, we'll try to unwrap it.
                         * If that fails, then we may need more network data.
                         */
                        final int initialPos = netRecvBuffer.position();
                        netRecvBuffer.flip();
                        final SSLEngineResult engineResult =
                            sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                        netRecvBuffer.compact();

                        final int updatedPos = netRecvBuffer.position();
                        if (updatedPos != initialPos) {
                            /* We did something */
                            progress = true;
                        }

                        switch (engineResult.getStatus()) {
                        case BUFFER_UNDERFLOW:
                            /* Not enough data to do anything useful. */
                            break;

                        case BUFFER_OVERFLOW:
                            /* Shouldn't happen, but apparently there's not
                             * enough space in the application receive buffer */
                            throw new BufferOverflowException();

                        case CLOSED:
                            /* We apparently got a CLOSE_NOTIFY */
                            socketChannel.socket().shutdownInput();
                            break;

                        case OK:
                            break;
                        }
                    }

                    if (!progress) {
                        final int count = socketChannel.read(netRecvBuffer);

                        if (count < 0) {
                            readCount = count;
                        } else if (count == 0) {
                            /* Presumably we are in non-blocking mode */
                            break;
                        }
                    }
                } finally {
                    readLock.unlock();
                }
            }
        }

        if (readCount < 0) {
            /*
             * This will throw an SSLException if we haven't yet received a
             * close_notify.
             */
            sslEngine.closeInbound();
            sslInboundClosed = true;
        }

        if (sslEngine.isInboundDone()) {
            return -1;
        }

        return readCount;
    }

    @Override
    public int write(ByteBuffer toSend) throws IOException, SSLException {
        return (int) write(new ByteBuffer[] { toSend }, 0, 1);
    }

    @Override
    public long write(ByteBuffer[] toSend) throws IOException, SSLException {
        return write(toSend, 0, toSend.length);
    }

    @Override
    public long write(ByteBuffer[] toSend, int offset, int length)
        throws IOException, SSLException {

        if ((offset < 0) ||
            (length < 0) ||
            (offset > toSend.length - length)) {
            throw new IndexOutOfBoundsException();
        }

        int toSendRemaining = 0;
        for (int i = offset; i < offset + length; ++i) {
            toSendRemaining += toSend[i].remaining();
        }
        if (toSendRemaining == 0) {
            return 0;
        }
        final int toSendTotal = toSendRemaining;

        /*
         * Probably not needed, but just in case there's a backlog, start with
         * a flush to clear out the network transmit buffer.
         */
        flush_internal();

        while (true) {
            writeLock.lock();
            try {
                final SSLEngineResult engineResult =
                    sslEngine.wrap(toSend, offset, length, netXmitBuffer);

                toSendRemaining -= engineResult.bytesConsumed();

                switch (engineResult.getStatus()) {
                case BUFFER_OVERFLOW:
                    /*
                     * Although we are flushing as part of the loop, we can
                     * still receive this because flush_internal isn't
                     * guaranteed to flush everything.
                     */
                    break;

                case BUFFER_UNDERFLOW:
                    /* Should not be possible here */
                    throw new BufferUnderflowException();

                case CLOSED:
                    throw new SSLException(
                        "Attempt to write to a closed SSL Channel");

                case OK:
                    break;
                }
            } finally {
                writeLock.unlock();
            }

            processAnyHandshakes();
            flush_internal();

            if (toSendRemaining == 0 || !socketChannel.isBlocking()) {
                break;
            }
        }

        return toSendTotal - toSendRemaining;
    }

    /**
     * Attempt to flush any pending writes to the underlying socket buffer.
     * The caller should ensure that it is the only thread accessing the
     * DataChannel in order that the return value be meaningful.
     *
     * @return flush status
     */
    @Override
    public FlushStatus flush()
        throws IOException {

        int n = flush_internal();
        if (writeLock.tryLock()) {
            try {
                SSLEngineResult.HandshakeStatus hstatus =
                    sslEngine.getHandshakeStatus();
                switch (hstatus) {
                case NEED_TASK:
                    return FlushStatus.NEED_TASK;
                case NEED_UNWRAP:
                    return FlushStatus.NEED_READ;
                case NEED_WRAP:
                    /*
                     * We should not be here if we are the only thread doing
                     * handshake, so there must be another thread, they will
                     * flush after they wrap, our job is done here.
                     */
                    return FlushStatus.DONE;
                case FINISHED:
                case NOT_HANDSHAKING:
                    break;
                default:
                    assert false : "Unexpected handshake status.";
                }

                if (n == 0) {
                    /*
                     * It is possible that there was nothing to flush last time
                     * we flushed but someone wrote something before we
                     * acquired the lock, so we flush again here
                     */
                    n = flush_internal();
                }

                final int pos = netXmitBuffer.position();

                if (pos == 0) {
                    return FlushStatus.DONE;
                }

                if (n != 0) {
                    return FlushStatus.AGAIN;
                }

                /* Here n == 0 and pos != 0, i.e., socket write busy. */
                return FlushStatus.WRITE_BUSY;
            } finally {
                writeLock.unlock();
            }
        }

        /*
         * If we weren't able to acquire the write lock, we can't be sure that
         * everything has been flushed, and there's a good chance that someone
         * else is writing (which the caller should have protected against in
         * order to get a reliable answer). Just ask the caller to flush again.
         */
        return FlushStatus.AGAIN;
    }

    /**
     * If any data is queued up to be sent in the network transmit buffer, try
     * to push it out.
     */
    private int flush_internal() throws IOException {

        int count = 0;

        /*
         * Don't insist on getting a lock.  If someone else has it, they will
         * probably flush it for us.
         */
        if (writeLock.tryLock()) {
            try {
                if (netXmitBuffer.position() == 0) {
                    return 0;
                }
                netXmitBuffer.flip();

                /*
                 * try/finally to keep things clean, in case the socket channel
                 * gets closed
                 */
                try {
                    count = socketChannel.write(netXmitBuffer);
                } finally {
                    netXmitBuffer.compact();
                }
            } finally {
                writeLock.unlock();
            }
        }
        return count;
    }

    @Override
    public void close() throws IOException, SSLException {

        try {
            flush_internal();

            if (!sslEngine.isOutboundDone()) {
                sslEngine.closeOutbound();
                processAnyHandshakes();
            } else if (!sslEngine.isInboundDone()) {
                if (sslInboundClosed) {
                    /*
                     * We only expect one handshake operation (the close) to
                     * happen at this point
                     */
                    processOneHandshake();
                }
            }
        } finally {
            synchronized(this) {
                if (!channelClosed) {
                    channelClosed = true;
                    socketChannel.close();
                }
            }
        }
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    /**
     * Transfer as much data as possible from the src buffer to the dst
     * buffers.
     *
     * @param src the source ByteBuffer - it is expected to be ready for a get.
     * @param dsts the destination array of ByteBuffers, each of which is
     * expected to be ready for a put.
     * @param offset the offset within the buffer array of the first buffer
     * into which bytes are to be transferred.
     * @param length the maximum number of buffers to be accessed
     * @return The number of bytes transfered from src to dst
     */
    private int transfer(ByteBuffer src,
                         ByteBuffer[] dsts,
                         int offset,
                         int length) {

        int transferred = 0;
        for (int i = offset; i < offset + length; ++i) {
            final ByteBuffer dst = dsts[i];
            final int space = dst.remaining();

            if (src.remaining() > space) {
                /* not enough room for it all */
                final ByteBuffer slice = src.slice();
                slice.limit(space);
                dst.put(slice);
                src.position(src.position() + space);
                transferred += space;
            } else {
                transferred += src.remaining();
                dst.put(src);
                break;
            }
        }
        return transferred;
    }

    /**
     * Repeatedly perform handshake operations while there is still
     * more work to do.
     */
    private void processAnyHandshakes() throws IOException {

        while (processOneHandshake()) {
            /* do nothing */
        }
    }

    /*
     * Attempt a handshake step.
     *
     * @return true if it is appropriate to call this again immediately.
     */
    private boolean processOneHandshake() throws IOException {

        int readCount = 0;
        int flushCount = 0;
        SSLEngineResult engineResult = null;

        switch (sslEngine.getHandshakeStatus()) {
        case FINISHED:
            /*
             * Just finished handshaking. We shouldn't actually see this here
             * as it is only supposed to be produced by a wrap or unwrap.
             */
            return false;

        case NEED_TASK:
            /*
             * Need results from delegated tasks before handshaking can
             * continue, so do them now.  We assume that the tasks are done
             * inline, and so we can return true here.
             */
            runDelegatedTasks();
            return true;

        case NEED_UNWRAP:
            {
                boolean unwrapped = false;

                /* Attempt to flush anything that is pending */
                try {
                    flush_internal();
                } catch (SocketException se) {
                }

                /*
                 * Attempt to process anything that is pending in the
                 * netRecvBuffer.
                 */
                readLock.lock();
                try {
                    if (netRecvBuffer.position() > 0) {
                        netRecvBuffer.flip();
                        engineResult =
                            sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                        netRecvBuffer.compact();
                        if (engineResult.getStatus() == Status.OK) {
                            unwrapped = true;
                        }
                    }

                    if (!unwrapped && !sslEngine.isInboundDone()) {
                        /*
                         * Either we had nothing in the netRecvBuffer or there
                         * was not enough data to unwrap, so let's try getting
                         * some more.
                         *
                         * If a re-negotiation is happening and the
                         * appRecvBuffer was full, we could have received a
                         * BUFFER_OVERFLOW engineResult, in which case a read()
                         * is not really helpful here, but it's harmless and is
                         * a rare occurrence, so we won't worry about it.
                         */
                        readCount = socketChannel.read(netRecvBuffer);
                        if (readCount < 0) {
                            try {
                                sslEngine.closeInbound();
                                sslInboundClosed = true;
                            } catch (SSLException ssle) {
                                // ignore
                            }
                        }

                        netRecvBuffer.flip();
                        engineResult =
                            sslEngine.unwrap(netRecvBuffer, appRecvBuffer);
                        netRecvBuffer.compact();
                    }
                } finally {
                    readLock.unlock();
                }
            }

            break;

        case NEED_WRAP:
            /*
             * Must send data to the remote side before handshaking can
             * continue, so wrap() must be called.
             */
            writeLock.lock();
            try {
                engineResult = sslEngine.wrap(emptyXmitBuffer, netXmitBuffer);
            } finally {
                writeLock.unlock();
            }

            if (engineResult.getStatus() == SSLEngineResult.Status.CLOSED) {
                /*
                 * If the engine is already closed, flush may fail, and that's
                 * ok, so squash any exceptions that happen
                 */
                try {
                    /* ignore the flush count */
                    flush_internal();
                } catch (SocketException se) {
                }
            } else {
                flushCount = flush_internal();
            }
            break;

        case NOT_HANDSHAKING:
            /* Not currently handshaking */
            return false;
        }

        /*
         * We may have done a wrap or unwrap above.  Check the engineResult
         */

        if (engineResult != null) {
            if (engineResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
                /*
                 * Handshaking just completed.   Here is our chance to do any
                 * session validation that might be required.
                 */
                if (sslEngine.getUseClientMode()) {
                    if (hostVerifier != null) {
                        peerTrusted =
                            hostVerifier.verify(targetHost,
                                                sslEngine.getSession());
                        if (peerTrusted) {
                            logger.log(FINE,
                                          "SSL host verifier reports that " +
                                          "connection target is valid");
                        } else {
                            logger.log(INFO,
                                       "SSL host verifier reports that " +
                                       "connection target is NOT valid");
                            throw new IOException(
                                "Server identity could not be verified");
                        }
                    }
                } else {
                    if (authenticator != null) {
                        peerTrusted =
                            authenticator.isTrusted(sslEngine.getSession());
                        if (peerTrusted) {
                            logger.log(FINE,
                                       "SSL authenticator reports that " +
                                       "channel is trusted");
                        } else {
                            logger.log(INFO,
                                       "SSL authenticator reports that " +
                                       "channel is NOT trusted");
                        }
                    }
                }
            }

            switch (engineResult.getStatus()) {
            case BUFFER_UNDERFLOW:
                /*
                 * This must have resulted from an unwrap, meaning we need to
                 * do another read.  If the last read did something useful,
                 * tell the caller to call us again.
                 */
                return readCount > 0;

            case BUFFER_OVERFLOW:
                /*
                 * Either we were processing an unwrap and the appRecvBuffer is
                 * full or we were processing a wrap and the netXmitBuffer is
                 * full.  For the unwrap case, the only way we can make progress
                 * is for the application to receive control.  For the wrap
                 * case, we may be able to make progress if the flush
                 * did something useful.
                 */
                if ((sslEngine.getHandshakeStatus() ==
                     HandshakeStatus.NEED_WRAP) &&
                    flushCount > 0) {
                    return true;
                }
                return false;

            case CLOSED:
                if (sslEngine.isOutboundDone()) {
                    try {
                        socketChannel.socket().shutdownOutput();
                    } catch (Exception e) {
                    }
                }
                return false;

            case OK:
                break;
            }
        }

        /*
         * Tell the caller to try again.  Cases where no handshake progress
         * can be made should return false above.
         */
        return true;
    }

    private void runDelegatedTasks() {
        Runnable task;
        /*
         * In theory, we could run these as a background job, but no need for
         * that level of complication.  Our server doesn't serve a large number
         * of clients.
         */
        while ((task = sslEngine.getDelegatedTask()) != null) {
            task.run();
        }
    }
}


