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

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import com.sleepycat.je.rep.impl.RepParams;

/**
 * Encapsulates the functionality around dealing with HostPort string pairs
 * having the format:
 *
 *  host[:port]
 */

public class HostPortPair {

    static public final String SEPARATOR = ":";

    /**
     * Parses a hostPort pair into the socket it represents.
     * @param hostPortPair
     * @return socket address for this host pair
     *
     * @throws IllegalArgumentException via ReplicatedEnvironment and Monitor
     * ctors.
     */
    public static InetSocketAddress getSocket(String hostPortPair) {
        if ("".equals(hostPortPair)) {
            throw new IllegalArgumentException
                ("Host and port pair was missing");
        }
        int portStartIndex = hostPortPair.indexOf(SEPARATOR);
        String hostName = hostPortPair;
        int port = -1;
        if (portStartIndex < 0) {
            port = Integer.parseInt(RepParams.DEFAULT_PORT.getDefault());
        } else {
            hostName = hostPortPair.substring(0, portStartIndex);
            port =
                Integer.parseInt(hostPortPair.substring(portStartIndex+1));
        }
        return new InetSocketAddress(hostName, port);
    }

    /**
     * Parses hostPort pairs into sockets it represents.
     *
     * @param hostPortPairs
     *
     * @return a set of socket addresses for these host pairs
     */
    public static Set<InetSocketAddress> getSockets(String hostPortPairs) {
        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        if (hostPortPairs != null) {
            for (String hostPortPair : hostPortPairs.split(",")) {
                final String hpp = hostPortPair.trim();
                if (hpp.length() > 0) {
                    helpers.add(getSocket(hpp));
                }
            }
        }

        return helpers;
    }

    public static String getString(String host, int port) {
        return host + SEPARATOR + port;
    }

    /**
     * Parses and returns the hostname string of a hostport pair
     */
    public static String getHostname(String hostPortPair) {
        int portStartIndex = hostPortPair.indexOf(SEPARATOR);
        return (portStartIndex < 0) ?
                hostPortPair :
                hostPortPair.substring(0, portStartIndex);
    }

    /**
     * Parses and returns the port of a hostport pair
     */
    public static int getPort(String hostPortPair) {
        int portStartIndex = hostPortPair.indexOf(SEPARATOR);
        return Integer.parseInt((portStartIndex < 0) ?
                                RepParams.DEFAULT_PORT.getDefault() :
                                hostPortPair.substring(portStartIndex+1));
    }
}
