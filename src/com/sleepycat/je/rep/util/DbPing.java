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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.StringTokenizer;

import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol.BinaryNodeStateResponse;
import com.sleepycat.je.rep.impl.BinaryNodeStateService;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.CmdUtil;

/**
 * This class provides the utility to request the current state of a replica in
 * a JE replication group, see more details in
 * {@link com.sleepycat.je.rep.NodeState}.
 */
public class DbPing {
    /* The name of the state requested node. */
    private String nodeName;
    /* The name of group which the requested node joins. */
    private String groupName;
    /* The SocketAddress of the requested node. */
    private InetSocketAddress socketAddress;
    /* The timeout value for building the connection. */
    private int socketTimeout = 10000;
    /* The factory for channel creation */
    private DataChannelFactory channelFactory;

    private static final String undocumentedUsageString =
        "  -netProps <optional>    # name of a property file containing\n" +
        "                             # properties needed for replication\n" +
        "                             # service access\n";

    private static final String usageString =
        "Usage: " + CmdUtil.getJavaCommand(DbPing.class) + "\n" +
        "  -nodeName <node name>      # name of the node whose state is\n" +
        "                             # requested\n" +
        "  -groupName <group name>    # name of the group which the node\n" +
        "                             # joins\n" +
        "  -nodeHost <host:port>      # the host name and port pair the\n" +
        "                             # node used to join the group\n" +
        "  -socketTimeout <optional>  # the timeout value for creating a\n" +
        "                             # socket connection with the node,\n" +
        "                             # default is 10 seconds if not set";

    /* Undocumented usage - SSL deferred
     *   -netProps &lt;optional&gt; # name of a property file containing
     *                               # properties needed for replication
     *                               # service access
     */

    /**
     * Usage:
     * <pre>
     * java {com.sleepycat.je.rep.util.DbPing |
     *       -jar je-&lt;version&gt;.jar DbPing}
     *   -nodeName &lt;node name&gt; # name of the node whose state is
     *                               # requested
     *   -groupName &lt;group name&gt; # name of the group which the node joins
     *   -nodeHost &lt;host:port&gt; # the host name and port pair the node
     *                               # used to join the group
     *   -socketTimeout              # the timeout value for creating a
     *                               # socket connection with the node,
     *                               # default is 10 seconds if not set
     * </pre>
     */
    public static void main(String args[])
        throws Exception {

        DbPing ping = new DbPing();
        ping.parseArgs(args);
        System.out.println(ping.getNodeState());
    }

    /**
     * Print usage information for this utility.
     *
     * @param message the errors description.
     */
    private void printUsage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }

        System.err.println(usageString);
        System.exit(-1);
    }

    /**
     * Parse the command line parameters.
     *
     * @param argv Input command line parameters.
     */
    private void parseArgs(String argv[]) {
        int argc = 0;
        int nArgs = argv.length;
        String netPropsName = null;

        if (nArgs == 0) {
            printUsage(null);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-nodeName")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                } else {
                    printUsage("-nodeName requires an argument");
                }
            } else if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = argv[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-nodeHost")) {
                if (argc < nArgs) {
                    StringTokenizer st =
                        new StringTokenizer(argv[argc++], ":");
                    if (st.countTokens() != 2) {
                        printUsage("Argument for -nodeHost is not valid.");
                    }
                    try {
                        socketAddress = new InetSocketAddress
                            (st.nextToken(), Integer.parseInt(st.nextToken()));
                    } catch (NumberFormatException e) {
                        printUsage("the port of -nodeHost is not valid");
                    }
                } else {
                    printUsage("-nodeHost requires an argument");
                }
            } else if (thisArg.equals("-socketTimeout")) {
                if (argc < nArgs) {
                    try {
                        socketTimeout = Integer.parseInt(argv[argc++]);
                    } catch (NumberFormatException e) {
                        printUsage("Argument for -socketTimeout is not valid");
                    }
                } else {
                    printUsage("-socketTimeout requires an argument");
                }
            } else if (thisArg.equals("-netProps")) {
                if (argc < nArgs) {
                    netPropsName = argv[argc++];
                } else {
                    printUsage("-netProps requires an argument");
                }
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
        }

        if (socketTimeout <= 0) {
            printUsage("-socketTimeout requires a positive integer number");
        }

        ReplicationNetworkConfig repNetConfig =
            ReplicationNetworkConfig.createDefault();
        if (netPropsName != null) {
            try {
                repNetConfig =
                    ReplicationNetworkConfig.create(new File(netPropsName));
            } catch (FileNotFoundException fnfe) {
                printUsage("The net properties file " + netPropsName +
                           " does not exist: " + fnfe.getMessage());
            } catch (IllegalArgumentException iae) {
                printUsage("The net properties file " + netPropsName +
                           " is not valid: " + iae.getMessage());
            }
        }

        if (nodeName == null || groupName == null || socketAddress == null) {
            printUsage("Node name, group name and the node host port are " +
                       "mandatory arguments, please configure.");
        }

        this.channelFactory = initializeFactory(repNetConfig, nodeName);
    }

    private DbPing() {
    }

    /**
     * Create a DbPing instance for programmatic use.
     *
     * @param repNode a class that implements
     * {@link com.sleepycat.je.rep.ReplicationNode}
     * @param groupName name of the group which the node joins
     * @param socketTimeout timeout value for creating a socket connection
     * with the node
     */
    /*
     * SSL deferred
     * This constructor form does not support setting of non-default service
     * access properties.
     */
    public DbPing(ReplicationNode repNode,
                  String groupName,
                  int socketTimeout) {
        this(repNode, groupName, socketTimeout, (ReplicationNetworkConfig)null);
    }

    /**
     * @hidden SSL deferred
     * Create a DbPing instance for programmatic use.
     *
     * @param repNode a class that implements
     * {@link com.sleepycat.je.rep.ReplicationNode}
     * @param groupName name of the group which the node joins
     * @param socketTimeout timeout value for creating a socket connection
     * with the node
     * @param netPropsFile a File containing replication net property
     * settings.  Null is allowed.
     * @throws FileNotFoundException if the netPropsFile does not exist
     * @throws IllegalArgumentException if the netProps file contains
     * invalid settings.
     */
    public DbPing(ReplicationNode repNode,
                  String groupName,
                  int socketTimeout,
                  File netPropsFile)
        throws FileNotFoundException, IllegalArgumentException {

        this(repNode, groupName, socketTimeout, makeRepNetConfig(netPropsFile));
    }

    /**
     * @hidden SSL deferred
     * Create a DbPing instance for programmatic use.
     *
     * @param repNode a class that implements
     * {@link com.sleepycat.je.rep.ReplicationNode}
     * @param groupName name of the group which the node joins
     * @param socketTimeout timeout value for creating a socket connection
     * with the node
     * @param netConfig a replication-net configuration object
     * property settings.  Null is allowed.
     * @throws IllegalArgumentException if the netProps contains invalid
     * settings.
     */
    public DbPing(ReplicationNode repNode,
                  String groupName,
                  int socketTimeout,
                  ReplicationNetworkConfig netConfig) {
        this(repNode, groupName, socketTimeout,
             initializeFactory(netConfig, repNode.getName()));
    }

    /**
     * @hidden SSL deferred
     * Create a DbPing instance for programmatic use.
     *
     * @param repNode a class that implements
     * {@link com.sleepycat.je.rep.ReplicationNode}
     * @param groupName name of the group which the node joins
     * @param socketTimeout timeout value for creating a socket connection
     * with the node
     * @param channelFactory the factory for channel creation
     * @throws IllegalArgumentException if the netProps contains invalid
     * settings.
     */
    public DbPing(ReplicationNode repNode,
                  String groupName,
                  int socketTimeout,
                  DataChannelFactory channelFactory) {
        this.nodeName = repNode.getName();
        this.groupName = groupName;
        this.socketAddress = repNode.getSocketAddress();
        this.socketTimeout = socketTimeout;
        this.channelFactory = channelFactory;
    }

    /* Get the state of the specified node. */
    public NodeState getNodeState()
        throws IOException, ServiceConnectFailedException {

        BinaryNodeStateProtocol protocol =
            new BinaryNodeStateProtocol(NameIdPair.NOCHECK, null);
        DataChannel channel = null;

        try {
            /* Build the connection. */
            channel = channelFactory.connect(
                socketAddress,
                new ConnectOptions().
                setTcpNoDelay(true).
                setOpenTimeout(socketTimeout).
                setReadTimeout(socketTimeout));
            ServiceDispatcher.doServiceHandshake
                (channel, BinaryNodeStateService.SERVICE_NAME);

            /* Send a NodeState request to the node. */
            protocol.write
                (protocol.new BinaryNodeStateRequest(nodeName, groupName),
                 channel);

            /* Get the response and return the NodeState. */
            BinaryNodeStateResponse response =
                protocol.read(channel, BinaryNodeStateResponse.class);

            return response.convertToNodeState();
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
    }

    private static ReplicationNetworkConfig makeRepNetConfig(File propFile)
        throws FileNotFoundException {
    	
        if (propFile == null) {
            return ReplicationNetworkConfig.createDefault();
        }

        return ReplicationNetworkConfig.create((propFile));
    }

    private static DataChannelFactory initializeFactory(
        ReplicationNetworkConfig repNetConfig,
        String logContext) {

        if (repNetConfig == null) {
            repNetConfig = ReplicationNetworkConfig.createDefault();
        }

        return DataChannelFactoryBuilder.construct(repNetConfig, logContext);
    }
}
