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
package com.sleepycat.je.rep.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;

public class ServiceDispatcherTest extends ServiceDispatcherTestBase {

    /* The number of simulated services. */
    private static int numServices = 10;

    /* The simulated service map. */
    private static final Map<String, BlockingQueue<DataChannel>> serviceMap =
        new HashMap<>();

    private DataChannelFactory channelFactory =
        DataChannelFactoryBuilder.construct(RepTestUtils.readRepNetConfig());

    /* Initialize the simulated service map. */
    static {
        for (int i=0; i < numServices; i++) {
            serviceMap.put("service"+i,
                           new LinkedBlockingQueue<DataChannel>());
        }
    }

    class EService implements Runnable {
        final DataChannel dataChannel;
        final int serviceNumber;

        EService(DataChannel dataChannel, int serviceNumber) {
            this.dataChannel = dataChannel;
            this.serviceNumber = serviceNumber;
        }

        @Override
        public void run() {
            try {
                dataChannel.getSocketChannel().configureBlocking(true);
                Channels.newOutputStream(dataChannel).write(
                    (byte)serviceNumber);
                dataChannel.close();
            } catch (IOException e) {
                throw new RuntimeException("Unexpected exception", e);
            }
        }
    }

    class LService extends Thread {

        final int serviceNumber;
        final BlockingQueue<DataChannel> queue;
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        volatile boolean started;

        LService(int serviceNumber, BlockingQueue<DataChannel> queue) {
            this.queue = queue;
            this.serviceNumber = serviceNumber;
        }

        @Override
        public void run() {
            started = true;
            try {
                final DataChannel dataChannel = queue.take();
                final SocketChannel channel = dataChannel.getSocketChannel();
                channel.configureBlocking(true);
                Channels.newOutputStream(dataChannel).write(
                    (byte)serviceNumber);
                dataChannel.close();
            } catch (Throwable e) {
                ex.compareAndSet(null, e);
            }
        }

        void finishTest() throws Throwable {
            assertTrue(started);
            join();
            final Throwable e = ex.get();
            if (e != null) {
                throw e;
            }
        }
    }

    @Test
    public void testExecuteBasic()
        throws IOException, ServiceConnectFailedException {

        for (int i=0; i < numServices; i++) {
            final int serviceNumber = i;
            dispatcher.register(new ServiceDispatcher.ExecutingService(
                "service"+i, dispatcher) {

                @Override
                public Runnable getRunnable(DataChannel dataChannel) {
                    return new EService(dataChannel, serviceNumber);
                }
            });
        }

        verifyServices();
    }

    @Test
    public void testLazyQueueBasic() throws Throwable {

        List<LService> lServiceList = new ArrayList<>();

        for (int i=0; i < numServices; i++) {
            LinkedBlockingQueue<DataChannel> queue =
                new LinkedBlockingQueue<>();

            LService lService = new LService(i, queue);

            dispatcher.register(dispatcher.new LazyQueuingService(
                "service"+i, queue, lService) {
            });
        }

        verifyServices();

        for (LService lService : lServiceList) {
            lService.finishTest();
        }
    }

    /*
     * Verifies the services that were set up.
     */
    private void verifyServices()
        throws IOException, ServiceConnectFailedException  {

        DataChannel dataChannels[] = new DataChannel[numServices];
        for (int i=0; i < numServices; i++) {
            DataChannel dataChannel = channelFactory.connect(
                dispatcherAddress, new ConnectOptions());
            dataChannels[i] = dataChannel;

            ServiceDispatcher.doServiceHandshake(dataChannel, "service"+i);
        }

        for (int i=0; i < numServices; i++) {
            DataChannel dataChannel = dataChannels[i];
            int result = Channels.newInputStream(dataChannel).read();
            assertEquals(i,result);
            dataChannel.close();
        }
    }

    @Test
    public void testBusyExecuteBasic()
        throws IOException, ServiceConnectFailedException  {

        dispatcher.register(new ServiceDispatcher.ExecutingService(
            "service1", dispatcher) {
            int bcount=0;
            @Override
            public Runnable getRunnable(DataChannel dataChannel) {
                bcount++;
                return new EService(dataChannel, 1);
            }
            @Override
            public boolean isBusy() {
                return bcount > 0;
            }
        });

        DataChannel dataChannel =
            channelFactory.connect(dispatcherAddress, new ConnectOptions());

        ServiceDispatcher.doServiceHandshake(dataChannel, "service1");

        /* Service should now be busy. */
        try {
            ServiceDispatcher.doServiceHandshake(dataChannel, "service1");
            fail("expected exception");
        } catch (ServiceConnectFailedException e1) {
            assertEquals(Response.BUSY, e1.getResponse());
        }
    }


    @Test
    public void testQueueBasic()
        throws IOException, InterruptedException,
               ServiceConnectFailedException  {

        for (Entry<String, BlockingQueue<DataChannel>> e :
            serviceMap.entrySet()) {
            dispatcher.register(e.getKey(), e.getValue());
        }

        for (Entry<String, BlockingQueue<DataChannel>> e :
            serviceMap.entrySet()) {
            DataChannel dataChannel = channelFactory.connect(
                dispatcherAddress, new ConnectOptions());

            ServiceDispatcher.doServiceHandshake(dataChannel, e.getKey());
            dataChannel.close();
        }

        for (Entry<String, BlockingQueue<DataChannel>> e :
            serviceMap.entrySet()) {
            DataChannel channel =
                dispatcher.takeChannel(e.getKey(), true, 100);
            assertTrue(channel != null);
            assertTrue(e.getValue().isEmpty());
        }
    }

    @Test
    public void testRegister() {
        try {
            dispatcher.register(null, new LinkedBlockingQueue<DataChannel>());
            fail("Expected EnvironmentFailureException");
        } catch(EnvironmentFailureException e) {
        }

        try {
            dispatcher.register("s1", (BlockingQueue<DataChannel>)null);
            fail("Expected EnvironmentFailureException");
        } catch(EnvironmentFailureException e) {
        }

        dispatcher.register("s1", new LinkedBlockingQueue<DataChannel>());
        try {
            dispatcher.register("s1", new LinkedBlockingQueue<DataChannel>());
            fail("Expected EnvironmentFailureException");
        } catch(EnvironmentFailureException e) {
        }
        dispatcher.cancel("s1");
    }

    @Test
    public void testCancel() {
        dispatcher.register("s1", new LinkedBlockingQueue<DataChannel>());
        dispatcher.cancel("s1");

        try {
            dispatcher.cancel("s1");
            fail("Expected EnvironmentFailureException");
        } catch(EnvironmentFailureException e) {
        }

        try {
            dispatcher.cancel(null);
            fail("Expected EnvironmentFailureException");
        } catch(EnvironmentFailureException e) {
        }
    }

    @Test
    public void testExceptions()
        throws IOException {

        /* Close connection due to unregistered service name. */
        DataChannel dataChannel =
            channelFactory.connect(dispatcherAddress, new ConnectOptions());
        try {
            ServiceDispatcher.doServiceHandshake(dataChannel, "s1");
            fail("Expected exception");
        } catch (ServiceConnectFailedException e) {
        }
    }
}
