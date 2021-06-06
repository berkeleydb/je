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

import static com.sleepycat.je.rep.impl.RepParams.NODE_HOST_PORT;

import java.io.File;

import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepParams;

/**
 * A utility to reset the members of a replication group, replacing the group
 * with a new group consisting of a single new member as described by the
 * arguments supplied to the utility.
 * <p>
 * This utility is useful when a copy of an existing replicated environment
 * needs to be used at a different site, with the same data, but with a
 * different initial node that can be used to grow the replication group as
 * usual. The utility can also be used to change the group name associated with
 * the environment.
 * <p>
 * The reset environment has a different identity from the environment before
 * the reset operation although it contains the same application data. To avoid
 * confusion, the reset environment is assigned a new internal unique id. The
 * unique id is checked whenever nodes attempt to communicate with each other
 * and ensure that all nodes in a group are dealing with the same data.
 * <p>
 * The reset process is typically accomplished using the steps outlined below.
 * It's good practice to back up your environment before running any utilities
 * that modify an environment.
 * <ol>
 * <li>Use {@code DbResetRepGroup} to reset an existing environment.
 * {@code DbResetRepGroup} can be used as a command line utility, and must be
 * executed locally on the host specified in the -nodeHostPort argument. The
 * host must also contain the environment directory.
 * Alternatively, {@code DbResetRepGroup} may be used programmatically through
 * the provided APIs.</li>
 * <li>Once reset, the environment can be opened with a
 * {@code ReplicatedEnvironment}, using the same node configuration as the one
 * that was passed in to the utility. No helper host configuration is needed.
 * Since the group consists of a single node, it will assume the role of a
 * Master, so long as it is created as an electable node.
 * <li>Additional nodes may now be created and can join the group as newly
 * created replicas, as described in {@code ReplicatedEnvironment}. Since these
 * new nodes are empty, they should be configured to use the new master as
 * their helper node, and will go through the
 * {@link <a href="{@docRoot}/../ReplicationGuide/lifecycle.html#lifecycle-nodestartup"> replication node lifecycle</a>}
 * to populate their environment directories. In this case, there will be data
 * in the converted master that can only be transferred to the replica through
 * a file copy executed with the help of a
 * {@link com.sleepycat.je.rep.NetworkRestore}</li>
 * </ol>
 * <p>
 * For example:
 *
 * <pre class="code">
 * // Run the utility on a copy of an existing replicated environment. Usually
 * // this environment will have originated on a different node and its
 * // replication group information will contain meta data referring to its
 * // previous host. The utility will reset this metadata so that it has a
 * // rep group (UniversalRepGroup) with a single node named nodeMars. The node
 * // is associated with the machine mars and will communicate on port 5001.
 *
 * DbResetRepGroup resetUtility =
 *     new DbResetRepGroup(envDirMars,          // env home dir
 *                         "UniversalRepGroup", // group name
 *                         "nodeMars",          // node name
 *                         "mars:5001");        // node host,port
 * resetUtility.reset();
 *
 * // Open the reset environment; it will take on the role of master.
 * ReplicatedEnvironment nodeMars = new ReplicatedEnvironment(envDirMars, ...);
 * ...
 * // Bring up additional nodes, which will be initialized from
 * // nodeMars. For example, from the machine venus you can now add a new
 * // member to the group(UniversalRepGroup) as below.
 *
 * ReplicationConfig repConfig = null;
 * try {
 *     repConfig = new ReplicationConfig("UniversalRepGroup", // groupName
 *                                       "nodeVenus",         // nodeName
 *                                       "venus:5008");       // nodeHostPort
 *     repConfig.setHelperHosts("mars:5001");
 *
 *     nodeVenus = new ReplicatedEnvironment(envDirB, repConfig, envConfig);
 * } catch (InsufficientLogException insufficientLogEx) {
 *
 *     // log files will be copied from another node in the group
 *     NetworkRestore restore = new NetworkRestore();
 *     restore.execute(insufficientLogEx, new NetworkRestoreConfig());
 *
 *     // try opening the node now that the environment files have been
 *     // restored on this machine.
 *     nodeVenus = new ReplicatedEnvironment(envDirVenus,
 *                                           repConfig,
 *                                           envConfig);
 * }
 * ...
 * </pre>
 */
public class DbResetRepGroup {

    private File envHome;
    private String groupName;
    private String nodeName;
    private String nodeHostPort;

    private static final String usageString =
        "usage: java -cp je.jar " +
        "com.sleepycat.je.rep.util.DbResetRepGroup\n" +
        " -h <dir>                              # environment home directory\n" +
        " -groupName <group name>               # replication group name\n" +
        " -nodeName <node name>                 # replicated node name\n" +
        " -nodeHostPort <host name:port number> # host name or IP address\n" +
        "                                          and port number to use\n" +
        "                                          for this node\n";

    /**
     * Usage:
     * <pre>
     * java -cp je.jar com.sleepycat.je.rep.util.DbResetRepGroup
     *   -h &lt;dir&gt;                          # environment home directory
     *   -groupName &lt;group name&gt;           # replication group name
     *   -nodeName &lt;node name&gt;             # replicated node name
     *   -nodeHostPort &lt;host name:port number&gt; # host name or IP address
     *                                             and port number to use
     *                                             for this node
     * </pre>
     */
    public static void main(String[] args) {
        DbResetRepGroup converter = new DbResetRepGroup();
        converter.parseArgs(args);
        converter.reset();
    }

    private void printUsage(String msg) {
        System.err.println(msg);
        System.err.println(usageString);
        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        int argc = 0;
        int nArgs = args.length;

        while (argc < nArgs) {
            String thisArg = args[argc++].trim();
            if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(args[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = args[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-nodeName")) {
                if (argc < nArgs) {
                    nodeName = args[argc++];
                } else {
                    printUsage("-nodeName requires an argument");
                }
            } else if (thisArg.equals("-nodeHostPort")) {
                if (argc < nArgs) {
                    nodeHostPort = args[argc++];
                    try {
                        NODE_HOST_PORT.validateValue(nodeHostPort);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                        printUsage("-nodeHostPort is illegal!");
                    }
                } else {
                    printUsage("-nodeHostPort requires an argument");
                }
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument.");
        }

        if (groupName == null) {
            printUsage("-groupName is a required argument.");
        }

        if (nodeName == null) {
            printUsage("-nodeName is a required argument.");
        }

        if (nodeHostPort == null) {
            printUsage("-nodeHostPort is a required argument.");
        }
    }

    private DbResetRepGroup() {
    }

    /**
     * Create a DbResetRepGroup object for this node.
     *
     * @param envHome The node's replicated environment directory. The
     * directory must be accessible on this host.
     * @param groupName The name of the new replication group
     * @param nodeName The node's name
     * @param nodeHostPort The host and port for this node. The utility
     * must be executed on this host.
     */
    public DbResetRepGroup(File envHome,
                           String groupName,
                           String nodeName,
                           String nodeHostPort) {
        this.envHome = envHome;
        this.groupName = groupName;
        this.nodeName = nodeName;
        this.nodeHostPort = nodeHostPort;
    }

    /**
     * Replaces the existing group with the new group having a single new node
     * as described by the constructor arguments.
     *
     * @see DbResetRepGroup
     */
     public void reset() {

        Durability durability =
            new Durability(Durability.SyncPolicy.SYNC,
                           Durability.SyncPolicy.SYNC,
                           Durability.ReplicaAckPolicy.NONE);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(durability);

        ReplicationConfig repConfig =
            new ReplicationConfig(groupName, nodeName, nodeHostPort);
        repConfig.setHelperHosts(repConfig.getNodeHostPort());

        /* Force the re-initialization upon open. */
        repConfig.setConfigParam(RepParams.RESET_REP_GROUP.getName(), "true");

        /* Open the environment, thus replacing the group. */
        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envHome, repConfig, envConfig);

        repEnv.close();
    }
}
