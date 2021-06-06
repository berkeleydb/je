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

package com.sleepycat.je.dbi;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.DiskOrderedCursorProducerException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.cleaner.FileProtector;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.tree.LN;

/**
 * This class implements the DiskOrderedCursor. When an instance is
 * constructed, a Producer Thread is created which runs a DiskOrderedScanner
 * against the DiskOrderedCursor's Database.  The callback for the
 * DiskOrderedScanner takes key/data byte arrays that are passed to it, and
 * then place those entries on a BlockingQueue which is shared between the
 * Producer Thread and the application thread.  When the application calls
 * getNext(), it simply takes an entry off the queue and hands it to the
 * caller.  The entries on the queue are simple KeyAndData structs which hold
 * byte[]'s for the key (and optional) data.  A special instance of KeyAndData
 * is used to indicate that the cursor scan has finished.
 *
 * The consistency guarantees are documented in the public javadoc for
 * DiskOrderedCursor, and are based on the use of DiskOrderedScanner (see its
 * javadoc for details).
 *
 * If the cleaner is operating concurrently with the DiskOrderedScanner, then
 * it is possible for a file to be deleted and a not-yet-processed LSN (i.e.
 * one which has not yet been returned to the user) might be pointing to that
 * deleted file.  Therefore, we must disable file deletion (but not cleaner
 * operation) during the DOS.
 */
public class DiskOrderedCursorImpl {

    /*
     * Simple struct to hold key and data byte arrays being passed through the
     * queue.
     */
    private static class KeyAndData {

        final int dbIdx;
        final byte[] key;
        final byte[] data;

        /* Negative value means "in hours", to save queue space. */
        final int expiration;

        /**
         * Creates a marker instance, for END_OF_QUEUE.
         */
        private KeyAndData() {
            this.dbIdx = -1;
            this.key = null;
            this.data = null;
            this.expiration = 0;
        }

        private KeyAndData(
            int dbIdx,
            byte[] key,
            byte[] data,
            int expiration,
            boolean expirationInHours) {

            this.dbIdx = dbIdx;
            this.key = key;
            this.data = data;
            this.expiration = expirationInHours ? (- expiration) : expiration;
        }

        private int getDbIdx() {
            return dbIdx;
        }

        private byte[] getKey() {
            return key;
        }

        private byte[] getData() {
            return data;
        }

        private long getExpirationTime() {
            if (expiration == 0) {
                return 0;
            }
            if (expiration < 0) {
                return TTL.expirationToSystemTime(- expiration, true);
            }
            return TTL.expirationToSystemTime(expiration, false);
        }
    }

    /*
     * The maximum number of entries that the BlockingQueue will store before
     * blocking the producer thread.
     */
    private int queueSize = 1000;

    /* Queue.offer() timeout in msec. */
    private int offerTimeout;

    private final boolean keysOnly;

    private final EnvironmentImpl env;

    private final Processor processor;

    private final DiskOrderedScanner scanner;

    private final Thread producer;

    private final BlockingQueue<KeyAndData> queue;

    /* The special KeyAndData which marks the end of the operation. */
    private final KeyAndData END_OF_QUEUE = new KeyAndData();

    private final RuntimeException SHUTDOWN_REQUESTED_EXCEPTION =
        new RuntimeException("Producer Thread shutdown requested");

    /* DiskOrderedCursors are initialized as soon as they are created. */
    private boolean closed = false;

    private KeyAndData currentNode = null;

    public DiskOrderedCursorImpl(
        final DatabaseImpl[] dbImpls,
        final DiskOrderedCursorConfig config) {

        this.env = dbImpls[0].getEnv();

        DbConfigManager configMgr = env.getConfigManager();

        this.offerTimeout = configMgr.getDuration(
            EnvironmentParams.DOS_PRODUCER_QUEUE_TIMEOUT);

        this.keysOnly = config.getKeysOnly();
        this.queueSize = config.getQueueSize();

        if (keysOnly) {
            for (int i = 0; i < dbImpls.length; ++i) {
                if (queueSize < dbImpls[i].getNodeMaxTreeEntries()) {
                    queueSize = dbImpls[i].getNodeMaxTreeEntries();
                }
            }
        }

        this.processor = new Processor();

        this.scanner = new DiskOrderedScanner(
            dbImpls, processor,
            config.getSerialDBScan(),
            config.getBINsOnly(), keysOnly, config.getCountOnly(),
            config.getLSNBatchSize(), config.getInternalMemoryLimit(),
            config.getDebug());

        this.queue = new ArrayBlockingQueue<KeyAndData>(queueSize);

        this.producer = new Thread() {

                public void run() {
                    try {
                        scanner.scan(
                            FileProtector.DISK_ORDERED_CURSOR_NAME,
                            env.getNodeSequence().
                                getNextDiskOrderedCursorId());

                        processor.close();

                    } catch (Throwable T) {
                        if (T == SHUTDOWN_REQUESTED_EXCEPTION) {
                            /* Shutdown was requested.  Don't rethrow. */
                            processor.isClosed = true;
                            return;
                        }

                        /* The exception is check by the getNext() method of
                           the consumer code.
                         */
                        processor.setException(T);

                        queue.offer(END_OF_QUEUE);
                    }
                }
            };

        this.producer.setName("DiskOrderedCursor Producer Thread for " +
                              Thread.currentThread());
        this.producer.start();
    }

    private class Processor implements DiskOrderedScanner.RecordProcessor {

        /*
         * A place to stash any exception caught by the producer thread so that
         * it can be returned to the application.
         */
        private Throwable exception;

        private volatile boolean shutdownNow;

        public boolean isClosed = false; // used for unit testing only

        @Override
        public void process(
            int dbIdx,
            byte[] key,
            byte[] data,
            int expiration,
            boolean expirationInHours) {

            checkShutdown();

            try {
                KeyAndData e = new KeyAndData(
                    dbIdx, key, data, expiration, expirationInHours);

                while (!queue.offer(e, offerTimeout, TimeUnit.MILLISECONDS)) {
                    checkShutdown();
                }

            } catch (InterruptedException IE) {
                setException(
                    new ThreadInterruptedException(env, IE));
                setShutdown();
            }
        }

        @Override
        public boolean canProcessWithoutBlocking(int nRecords) {
            return queue.remainingCapacity() >= nRecords;
        }

        @Override
        public int getCapacity() {
            return queueSize;
        }

        /*
         * Called from the producer thread's run() method after there are
         * no more records to scan.
         */
        void close() {

            try {
                if (!queue.offer(END_OF_QUEUE, offerTimeout,
                                 TimeUnit.MILLISECONDS)) {
                    /* Cursor.close() called, but queue was not drained. */
                    setException(SHUTDOWN_REQUESTED_EXCEPTION.
                                 fillInStackTrace());
                    setShutdown();
                }

                isClosed = true;

            } catch (InterruptedException IE) {
                setException(
                    new ThreadInterruptedException(env, IE));
                setShutdown();
            }
        }

        /*
         * Called by producer code only.
         */
        void setException(Throwable t) {
            exception = t;
        }

        /*
         * Called by consumer thread's getNext() method.
         */
        private Throwable getException() {
            return exception;
        }

        /*
         * Called by by both producer and consumer code.
         */
        private void setShutdown() {
            shutdownNow = true;
        }

        /*
         * Called by producer code only.
         */
        @Override
        public void checkShutdown() {
            if (shutdownNow) {
                throw SHUTDOWN_REQUESTED_EXCEPTION;
            }
        }
    }

    /*
     * For unit testing only
     */
    public boolean isProcessorClosed() {
        return processor.isClosed;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    public synchronized void close() {
        if (closed) {
            return;
        }

        /* Tell Producer Thread to die if it hasn't already. */
        processor.setShutdown();

        closed = true;
    }

    public void checkEnv() {
        env.checkIfInvalid();
    }

    private OperationResult setData(
        final DatabaseEntry foundKey,
        final DatabaseEntry foundData) {

        if (foundKey != null) {
            LN.setEntry(foundKey, currentNode.getKey());
        }
        if (foundData != null) {
            LN.setEntry(foundData, currentNode.getData());
        }
        return DbInternal.makeResult(currentNode.getExpirationTime());
    }

    public synchronized OperationResult getCurrent(
        final DatabaseEntry foundKey,
        final DatabaseEntry foundData) {

        if (closed) {
            throw new IllegalStateException("Not initialized");
        }

        if (currentNode == END_OF_QUEUE) {
            return null;
        }

        return setData(foundKey, foundData);
    }

    public int getCurrDb() {

        if (closed) {
            throw new IllegalStateException("Not initialized");
        }

        if (currentNode == END_OF_QUEUE) {
            return -1;
        }

        return currentNode.getDbIdx();
    }

    public synchronized OperationResult getNext(
        final DatabaseEntry foundKey,
        final DatabaseEntry foundData) {

        if (closed) {
            throw new IllegalStateException("Not initialized");
        }

        /*
         * If null was returned earlier, do not enter loop below to avoid a
         * hang. [#21282]
         */
        if (currentNode == END_OF_QUEUE) {
            return null;
        }

        try {

            /*
             * Poll in a loop in case the producer thread throws an exception
             * and can't put END_OF_QUEUE on the queue because of an
             * InterruptedException.  The presence of an exception is the last
             * resort to make sure that getNext actually returns to the user.
             */
            do {
                currentNode = queue.poll(1, TimeUnit.SECONDS);
                if (processor.getException() != null) {
                    break;
                }
            } while (currentNode == null);

        } catch (InterruptedException IE) {
            currentNode = END_OF_QUEUE;
            throw new ThreadInterruptedException(env, IE);
        }

        if (processor.getException() != null) {
            throw new DiskOrderedCursorProducerException(
                "Producer Thread Failure", processor.getException());
        }

        if (currentNode == END_OF_QUEUE) {
            return null;
        }

        return setData(foundKey, foundData);
    }

    /**
     * For unit testing only
     */
    int freeQueueSlots() {
        return queue.remainingCapacity();
    }

    /*
     * For unit testing only.
     */
    long getNumLsns() {
        return scanner.getNumLsns();
    }

    /*
     * For unit testing only.
     */
    DiskOrderedScanner getScanner() {
        return scanner;
    }

    /**
     * For testing and other internal use.
     */
    public int getNScannerIterations() {
        return scanner.getNIterations();
    }
}
