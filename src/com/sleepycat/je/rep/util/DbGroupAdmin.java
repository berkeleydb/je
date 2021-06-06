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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.PropUtil;

/**
 * DbGroupAdmin supplies the functionality of the administrative class {@link
 * ReplicationGroupAdmin} in a convenient command line utility. For example, it
 * can be used to display replication group information, or to remove a node
 * from the replication group.
 * <p>
 * Note: This utility does not handle security and authorization. It is left
 * to the user to ensure that the utility is invoked with proper authorization.
 * <p>
 * See {@link DbGroupAdmin#main} for a full description of the command line
 * arguments.
 */
/*
 * SSL deferred
 * See {@link ReplicationConfig} for descriptions of the parameters that
 * control replication service access.
 */
public class DbGroupAdmin {

    enum Command { DUMP, REMOVE, TRANSFER_MASTER, UPDATE_ADDRESS, DELETE };

    private String groupName;
    private Set<InetSocketAddress> helperSockets;
    private String nodeName;
    private String newHostName;
    private int newPort;
    private String timeout;
    private boolean forceFlag;
    private DataChannelFactory channelFactory;
    private ReplicationGroupAdmin groupAdmin;
    private final ArrayList<Command> actions = new ArrayList<Command>();

    private static final String undocumentedUsageString =
        "  -netProps <optional>   # name of a property file containing\n" +
        "                            # properties needed for replication\n" +
        "                            # service access\n";

    private static final String usageString =
        "Usage: " + CmdUtil.getJavaCommand(DbGroupAdmin.class) + "\n" +
        "  -groupName <group name>   # name of replication group\n" +
        "  -helperHosts <host:port>  # identifier for one or more members\n" +
        "                            # of the replication group which can\n"+
        "                            # be contacted for group information,\n"+
        "                            # in this format:\n" +
        "                            # hostname[:port][,hostname[:port]]\n" +
        "  -dumpGroup                # dump group information\n" +
        "  -removeMember <node name> # node to be removed\n" +
        "  -updateAddress <node name> <new host:port>\n" +
        "                            # update the network address for a\n" +
        "                            # specified node.  The node should not\n" +
        "                            # be alive when updating the address\n" +
        "  -transferMaster [-force] <node1,node2,...> <timeout>\n" +
        "                            # transfer master role to one of the\n" +
        "                            # specified nodes.";

    /* Undocumented options for main()
     *   -netProps &lt;propFile&gt;  # (optional)
     *                               # name of a property file containing
     *                               # properties needed for replication
     *                               # service access
     *   -deleteMember <node name>   # Deletes the node from the group, doesn't
     *                               # just mark it removed
     */

    /**
     * Usage:
     * <pre>
     * java {com.sleepycat.je.rep.util.DbGroupAdmin |
     *       -jar je-&lt;version&gt;.jar DbGroupAdmin}
     *   -groupName &lt;group name&gt;  # name of replication group
     *   -helperHosts &lt;host:port&gt; # identifier for one or more members
     *                            # of the replication group which can be
     *                            # contacted for group information, in
     *                            # this format:
     *                            # hostname[:port][,hostname[:port]]*
     *   -dumpGroup               # dump group information
     *   -removeMember &lt;node name&gt;# node to be removed
     *   -updateAddress &lt;node name&gt; &lt;new host:port&gt;
     *                            # update the network address for a specified
     *                            # node. The node should not be alive when
     *                            # updating address
     *   -transferMaster [-force] &lt;node1,node2,...&gt; &lt;timeout&gt;
     *                            # transfer master role to one of the
     *                            # specified nodes.
     * </pre>
     */
    public static void main(String... args)
        throws Exception {

        DbGroupAdmin admin = new DbGroupAdmin();
        admin.parseArgs(args);
        admin.run();
    }

    /**
     * Print usage information for this utility.
     *
     * @param msg
     */
    private void printUsage(String msg) {
        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println(usageString);
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
            System.exit(0);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = argv[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-helperHosts")) {
                if (argc < nArgs) {
                    helperSockets = HostPortPair.getSockets(argv[argc++]);
                } else {
                    printUsage("-helperHosts requires an argument");
                }
            } else if (thisArg.equals("-dumpGroup")) {
                actions.add(Command.DUMP);
            } else if (thisArg.equals("-removeMember")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.REMOVE);
                } else {
                    printUsage("-removeMember requires an argument");
                }
            } else if (thisArg.equals("-updateAddress")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];

                    if (argc < nArgs) {
                        String hostPort = argv[argc++];
                        int index = hostPort.indexOf(":");
                        if (index < 0) {
                            printUsage("Host port pair format must be " +
                                       "<host name>:<port number>");
                        }

                        newHostName = hostPort.substring(0, index);
                        newPort = Integer.parseInt
                            (hostPort.substring(index + 1, hostPort.length()));
                    } else {
                        printUsage("-updateAddress requires a " +
                                   "<host name>:<port number> argument");
                    }

                    actions.add(Command.UPDATE_ADDRESS);
                } else {
                    printUsage
                        ("-updateAddress requires the node name argument");
                }
            } else if (thisArg.equals("-transferMaster")) {

                // TODO: it wouldn't be too hard to allow "-force" as a
                // node name.
                //
                if (argc < nArgs && "-force".equals(argv[argc])) {
                    forceFlag = true;
                    argc++;
                }
                if (argc + 1 < nArgs) {
                    nodeName = argv[argc++];

                    /*
                     * Allow either
                     *     -transferMaster mercury,venus 900 ms
                     * or
                     *     -transferMaster mercury,venus "900 ms"
                     */
                    if (argc + 1 < nArgs && argv[argc + 1].charAt(0) != '-') {
                        timeout = argv[argc] + " " + argv[argc + 1];
                        argc += 2;
                    } else {
                        timeout = argv[argc++];
                    }

                    actions.add(Command.TRANSFER_MASTER);
                } else {
                    printUsage
                        ("-transferMaster requires at least two arguments");
                }
            } else if (thisArg.equals("-netProps")) {
                if (argc < nArgs) {
                    netPropsName = argv[argc++];
                } else {
                    printUsage("-netProps requires an argument");
                }
            } else if (thisArg.equals("-deleteMember")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.DELETE);
                } else {
                    printUsage("-deleteMember requires an argument");
                }
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
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

        this.channelFactory = initializeFactory(repNetConfig, groupName);
    }

    /* Execute commands */
    private void run()
        throws Exception {

        createGroupAdmin();

        if (actions.size() == 0) {
            return;
        }

        for (Command action : actions) {
            switch (action) {

                /* Dump the group information. */
            case DUMP:
                dumpGroup();
                break;

                /* Remove a member. */
            case REMOVE:
                removeMember(nodeName);
                break;

                /* Transfer the current mastership to a specified node. */
            case TRANSFER_MASTER:
                transferMaster(nodeName, timeout);
                break;

                /* Update the network address of a specified node. */
            case UPDATE_ADDRESS:
                updateAddress(nodeName, newHostName, newPort);
                break;

                /* Delete a member */
            case DELETE:
                deleteMember(nodeName);
                break;

            default:
                throw new AssertionError();
            }
        }
    }

    private DbGroupAdmin() {
    }

    /**
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
    */
    /*
     * SSL deferred
     * This constructor does not support non-default service net properties.
     * See the other constructor forms which allow setting of net properties.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets) {
        this(groupName, helperSockets, (ReplicationNetworkConfig)null);
    }

    /**
     * @hidden SSL deferred
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
     * @param netPropsFile a File containing replication net property
     * settings.  This parameter is ignored if null.
     * @throws FileNotFoundException if the netPropsFile does not exist
     * @throws IllegalArgumentException if the netPropsFile contains
     * invalid settings.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets,
                        File netPropsFile)
        throws FileNotFoundException {

        this(groupName, helperSockets, makeRepNetConfig(netPropsFile));
    }

    /**
     * @hidden SSL deferred
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
     * @param netConfig replication net configuration - null allowable
     * This parameter is ignored if null.
     * @throws IllegalArgumentException if the netProps contains
     * invalid settings.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets,
                        ReplicationNetworkConfig netConfig) {
        this.groupName = groupName;
        this.helperSockets = helperSockets;
        this.channelFactory = initializeFactory(netConfig, groupName);

        createGroupAdmin();
    }

    /* Create the ReplicationGroupAdmin object. */
    private void createGroupAdmin() {
        if (groupName == null) {
            printUsage("Group name must be specified");
        }

        if ((helperSockets == null) || (helperSockets.size() == 0)) {
            printUsage("Host and ports of helper nodes must be specified");
        }

        groupAdmin = new ReplicationGroupAdmin(
            groupName, helperSockets, channelFactory);
    }

    /**
     * Display group information. Lists all members and the group master.  Can
     * be used when reviewing the <a
     * href="http://www.oracle.com/technetwork/database/berkeleydb/je-faq-096044.html#HAChecklist">group configuration. </a>
     */
    public void dumpGroup() {
        System.out.println(getFormattedOutput());
    }

    /**
     * Remove a node from the replication group. Once removed, a
     * node cannot be added again to the group under the same node name.
     *
     * <p>{@link NodeType#SECONDARY Secondary} nodes cannot be removed; they
     * automatically leave the group when they are shut down or become
     * disconnected from the master.
     *
     * @param name name of the node to be removed
     *
     * @see ReplicationGroupAdmin#removeMember
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public void removeMember(String name) {
        if (name == null) {
            printUsage("Node name must be specified");
        }

        groupAdmin.removeMember(name);
    }

    /**
     * @hidden internal, for use in disaster recovery [#23447]
     *
     * Deletes a node from the replication group, which allows the node to be
     * added to the group again under the same name.
     *
     * <p>{@link NodeType#SECONDARY Secondary} and {@link NodeType#EXTERNAL
     * External} nodes cannot be deleted; they automatically leave the group
     * when they are shut down or become disconnected from the master.
     *
     * @param name name of the node to be deleted
     *
     * @see ReplicationGroupAdmin#deleteMember
     */
    public void deleteMember(String name) {
        if (name == null) {
            printUsage("Node name must be specified");
        }

        groupAdmin.deleteMember(name);
    }

    /**
     * Update the network address for a specified node. When updating the
     * address of a node, the node cannot be alive. See {@link
     * ReplicationGroupAdmin#updateAddress} for more information.
     *
     * <p>The address of a {@link NodeType#SECONDARY} node cannot be updated
     * with this method, since nodes must be members but not alive to be
     * updated, and secondary nodes are not members when they are not alive.
     * To change the address of a secondary node, restart the node with the
     * updated address.
     *
     * @param nodeName the name of the node whose address will be updated
     * @param newHostName the new host name of the node
     * @param newPort the new port number of the node
     */
    @SuppressWarnings("hiding")
    public void updateAddress(String nodeName,
                              String newHostName,
                              int newPort) {
        if (nodeName == null || newHostName == null) {
            printUsage("Node name and new host name must be specified");
        }

        if (newPort <= 0) {
            printUsage("Port of the new network address must be specified");
        }

        groupAdmin.updateAddress(nodeName, newHostName, newPort);
    }

    /**
     * Transfers the master role from the current master to one of the
     * electable replicas specified in the argument list.
     *
     * @param nodeList comma-separated list of nodes
     * @param timeout in <a href="../../EnvironmentConfig.html#timeDuration">
     *        same form</a> as accepted by duration config params
     *
     * @see ReplicatedEnvironment#transferMaster
     */
    @SuppressWarnings("hiding")
    public void transferMaster(String nodeList, String timeout) {
        String result =
            groupAdmin.transferMaster(parseNodes(nodeList),
                                      PropUtil.parseDuration(timeout),
                                      TimeUnit.MILLISECONDS,
                                      forceFlag);
        System.out.println("The new master is: " + result);
    }

    private Set<String> parseNodes(String nodes) {
        if (nodes == null) {
            throw new IllegalArgumentException("node list may not be null");
        }
        StringTokenizer st = new StringTokenizer(nodes, ",");
        Set<String> set = new HashSet<String>();
        while (st.hasMoreElements()) {
            set.add(st.nextToken());
        }
        return set;
    }

    /*
     * This method presents group information in a user friendly way. Internal
     * fields are hidden.
     */
    private String getFormattedOutput() {
        StringBuilder sb = new StringBuilder();
        RepGroupImpl repGroupImpl = groupAdmin.getGroup().getRepGroupImpl();

        /* Get the master node name. */
        String masterName = groupAdmin.getMasterNodeName();

        /* Get the electable nodes information. */
        sb.append("\nGroup: " + repGroupImpl.getName() + "\n");
        sb.append("Electable Members:\n");
        Set<RepNodeImpl> nodes = repGroupImpl.getElectableMembers();
        if (nodes.size() == 0) {
            sb.append("    No electable members\n");
        } else {
            for (RepNodeImpl node : nodes) {
                String type =
                    masterName.equals(node.getName()) ? "master, " : "";
                sb.append("    " + node.getName() + " (" + type +
                          node.getHostName() + ":" + node.getPort() + ", " +
                          node.getBarrierState() + ")\n");
            }
        }

        /* Get the monitors information. */
        sb.append("\nMonitor Members:\n");
        nodes = repGroupImpl.getMonitorMembers();
        if (nodes.size() == 0) {
            sb.append("    No monitors\n");
        } else {
            for (RepNodeImpl node : nodes) {
                sb.append("    " + node.getName() + " (" + node.getHostName() +
                          ":" + node.getPort() + ")\n");
            }
        }

        /* Get information about secondary nodes */
        sb.append("\nSecondary Members:\n");
        nodes = repGroupImpl.getSecondaryMembers();
        if (nodes.isEmpty()) {
            sb.append("    No secondary members\n");
        } else {
            for (final RepNodeImpl node : nodes) {
                sb.append("    " + node.getName() + " (" + node.getHostName() +
                          ":" + node.getPort() + ", " +
                          node.getBarrierState() + ")\n");
            }
        }

        /* Get information about external nodes */
        sb.append("\nExternal Members:\n");
        nodes = repGroupImpl.getExternalMembers();
        if (nodes.isEmpty()) {
            sb.append("    No external members\n");
        } else {
            for (final RepNodeImpl node : nodes) {
                sb.append("    " + node.getName() + " (" + node.getHostName() +
                          ":" + node.getPort() + ", " +
                          node.getBarrierState() + ")\n");
            }
        }

        return sb.toString();
    }

    private static ReplicationNetworkConfig makeRepNetConfig(File propFile)
        throws FileNotFoundException {
    	
        if (propFile == null) {
            return ReplicationNetworkConfig.createDefault();
        }

        return ReplicationNetworkConfig.create(propFile);
    }

    private static DataChannelFactory initializeFactory(
        ReplicationNetworkConfig repNetConfig,
        String logContext) {

        if (repNetConfig == null) {
            repNetConfig =
                ReplicationNetworkConfig.createDefault();
        }

        return DataChannelFactoryBuilder.construct(repNetConfig, logContext);
    }
}
