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

package com.sleepycat.je.rep.utilint;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * An iterator to iterate over the free ports on an interface.
 */
public class FreePortLocator {

    /**
     * Whether to print debugging messages -- use this to find tests that are
     * not closing ports.
     */
    private static final boolean debug =
        Boolean.getBoolean("test.debugFreePortLocator");

    private final String hostname;
    private final int portStart;
    private final int portEnd;

    private int currPort;

    /**
     * Constructor identifying the interface and the port range within which
     * to look for free ports. The port range specified by the arguments
     * must be < 32768, that is, it should be outside the dynamic port range
     * that is typically configured on most machines.
     *
     * @see <a href="https://sleepycat.oracle.com/trac/wiki/JEKV/UnitTest#Avoidingproblemswithanonymousports.html">Anonymous ports</a>
     * for details regarding port configuration for tests.
     */
    public FreePortLocator(String hostname, int portStart, int portEnd) {
        super();
        assert portStart < portEnd;

        if ((portStart > 0x7fff) || (portEnd > 0x7fff)) {
            throw new IllegalArgumentException
                ("Invalid port range:" + portStart + " - " + portEnd + ". " +
                 "The port range must not extend past:" + 0x7fff +
                 " since the allocated ports could then overlap with " +
                 "dynamically assigned ports used by other services. ");
        }

        this.hostname = hostname;
        this.portStart = portStart;
        this.portEnd = portEnd;
        currPort = portStart;
    }

    public int getPortStart() {
        return portStart;
    }

    public int getPortEnd() {
        return portEnd;
    }

    /**
     * Returns the next free port. Note that it's possible that on a busy
     * machine another process may grab the "free" port before it's actually
     * used.
     *
     * There is somewhat AIsh aspect to the code below. In general it tries to
     * be very conservative, using different techniques so that it works
     * reasonably well on Linux, Mac OS and Windows.
     *
     * Note: The use of setReuseAddress after a bind operation may look
     * dubious, since it runs counter to the API doc, but it helps based on
     * actual tests. It's also the idiom used by Apache Camel to find a
     * free port. It, at least, can't hurt.
     */
    public int next() {
        while (++currPort < portEnd) {

            /* Try connecting to the port to see if somebody is listening. */
            Socket s = null;
            try {
                s = new Socket(hostname, currPort);
                /* Somebody is listening on the port. */
                if (debug) {
                    System.err.println(
                        "FreePortLocator: " + currPort + " busy - socket");
                    Thread.dumpStack();
                }
                continue;
            } catch (IOException e) {
                /* Nobody is listening, continue with other tests. */
            } finally {
                if (s != null){
                    try {
                        s.close();
                    } catch (IOException e) {
                        /* Unexpected, something's wrong, ignore the port. */
                        if (debug) {
                            System.err.println(
                                "FreePortLocator: " + currPort +
                                " busy - socket close: " + e);
                            e.printStackTrace();
                        }
                       continue;
                    }
                }
            }

            /* Try without a hostname */
            ServerSocket ss = null;
            DatagramSocket ds = null;
            try {
                ss = new ServerSocket(currPort);
                ss.setReuseAddress(true);
                ds = new DatagramSocket(currPort);
                ds.setReuseAddress(true);
            } catch (IOException e) {
                if (debug) {
                    System.err.println(
                        "FreePortLocator: " + currPort +
                        " busy - server, datagram: " + e);
                    e.printStackTrace();
                }
                continue;
            } finally {
                if (ds != null) {
                    ds.close();
                }

                if (ss != null) {
                    try {
                        ss.close();
                    } catch (IOException e) {
                        if (debug) {
                            System.err.println(
                                "FreePortLocator: " + currPort +
                                " busy - server close: " + e);
                            e.printStackTrace();
                        }
                        continue;
                    }
                }
            }

            ss = null;
            ds = null;

            /* try with a hostname */
           final InetSocketAddress sa =
               new InetSocketAddress(hostname, currPort);
            try {
                ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(sa);

                ds = new DatagramSocket(sa);
                ds.setReuseAddress(true);
            } catch (IOException e) {
                if (debug) {
                    System.err.println(
                        "FreePortLocator: " + currPort +
                        " busy - server, datagram hostname: " + e);
                    e.printStackTrace();
                }
                continue;
            } finally {
                if (ds != null) {
                    ds.close();
                }

                if (ss != null) {
                    try {
                        ss.close();
                    } catch (IOException e) {
                        if (debug) {
                            System.err.println(
                                "FreePortLocator: " + currPort +
                                " busy - server hostname close: " + e);
                            e.printStackTrace();
                        }
                        continue;
                    }
                }
            }

            /* Survived port test gauntlet, return it. */
            if (debug) {
                System.err.println(
                    "FreePortLocator: " + currPort + " free");
            }
            return currPort;
        }

        throw new IllegalStateException
            ("No more ports available in the range: " +
             portStart + " - " + portEnd);
    }

    /**
     * Skip a number of ports.
     */
    public void skip(int num) {
        currPort += num;
    }
}
