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

package com.sleepycat.je.rep;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Properties;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.VLSN;

/**
 * This exception indicates that the log files constituting the Environment are
 * insufficient and cannot be used as the basis for continuing with the
 * replication stream provided by the current master.
 * <p>
 * This exception is typically thrown by the ReplicatedEnvironment constructor
 * when a node has been down for a long period of time and is being started up
 * again. It may also be thrown when a brand new node attempts to become a
 * member of the group and it does not have a sufficiently current set of log
 * files. If the group experiences sustained network connectivity problems,
 * this exception may also be thrown by an active Replica that has been unable
 * to stay in touch with the members of its group for an extended period of
 * time.
 * <p>
 * In the typical case, application handles the exception by invoking
 * {@link NetworkRestore#execute} to obtain the log files it needs from one of
 * the members of the replication group. After the log files are obtained, the
 * node recreates its environment handle and resumes participation as an active
 * member of the group.
 *
 * @see NetworkRestore
 */
public class InsufficientLogException extends RestartRequiredException {
    private static final long serialVersionUID = 1;

    /*
     * These properties store information to serialize the ILE and save it in a
     * RestoreRequired log entry.
     *
     * The properties named P_* describe a log provider. An ILE may have one or
     * more log providers, and a more structured format like Json would make it
     * easier to describe an array of items. Since we're currently constrained
     * to using a property list, we follow the convention of suffixing each
     * provider property with a number. For example, if there are two
     * providers, P_NUMPROVIDERS=2, and there would be P_NODENAME0,
     * P_NODENAME1, etc.
     */
    private static final String P_NUMPROVIDERS = "P_NUMPROVIDERS";
    private static final String P_NODENAME = "P_NODENAME";
    private static final String P_NODETYPE = "P_NODETYPE";
    private static final String P_HOSTNAME = "P_HOSTNAME";
    private static final String P_PORT = "P_PORT";

    /*
     * Properties needed to create an environment handle for the node which
     * needs new logs, and is the target for a backup.
     */
    private static final String GROUP_NAME = "GROUP_NAME";
    private static final String NODE_NAME = "NODE_NAME";
    private static final String HOSTNAME = "HOSTNAME";
    private static final String PORT = "PORT";
    private static final String ENV_DIR = "ENV_DIR";
    private static final String REFRESH_VLSN = "REFRESH_VLSN";

    /*
     * A handle to the replication environment which is the target for the
     * network restore. May be null. If repImpl was created by the ILE, the
     * caller has the responsibility for closing the environment.  Note that a
     * RepImpl rather than RepNode is used as the environment handle. RepNodes
     * are only available for nodes which are a member of a group. In some
     * cases, an ILE is used by a node which is detached, and is not currently
     * connected to its group.
     */
    private transient RepImpl repImpl;
    private boolean openedByILE;

    /* Attributes used by the network restore, in serialized format */
    private final transient Properties props;

    /*
     * No longer used as of JE 7.5, but the field is retained for serializable
     * compatibility.
     */
    private final VLSN refreshVLSN;

    /*
     * Candidate nodes for a log file refresh. Note that this field is only
     * used by a thread that is synchronously processing the caught exception,
     * which is safely after the instance has been initialized.
     */
    private final Set<ReplicationNode> logProviders;

    /**
     * For KVS test use only.
     * @hidden
     */
    @SuppressWarnings("unused") // KVS-only
    public InsufficientLogException(String message) {
        super(null, EnvironmentFailureReason.UNEXPECTED_STATE, message);
        this.repImpl = null;
        this.refreshVLSN = null;
        this.logProviders = null;
        this.props = null;
    }

    /**
     * @hidden
     *
     * Creates an instance of the exception and packages up the information
     * needed by NetworkRestore.
     */
    public InsufficientLogException(RepNode repNode,
                                    Set<ReplicationNode> logProviders) {
        super(repNode.getRepImpl(), EnvironmentFailureReason.INSUFFICIENT_LOG);
        this.repImpl = repNode.getRepImpl();
        this.refreshVLSN = VLSN.NULL_VLSN;
        this.logProviders = logProviders;
        props = initProperties(repNode.getGroup().getName());
    }

    private Properties initProperties(String groupName) {
        Properties p = new Properties();

        /*
         * These apply to the destination node which is experiencing the
         * insufficient logs.
         */
        p.setProperty(GROUP_NAME, groupName);
        p.setProperty(NODE_NAME, repImpl.getNameIdPair().getName());
        p.setProperty(HOSTNAME, repImpl.getHostName());
        p.setProperty(PORT, Integer.toString(repImpl.getPort()));
        p.setProperty(ENV_DIR, repImpl.getEnvironmentHome().getPath());
        p.setProperty(REFRESH_VLSN, Long.toString(refreshVLSN.getSequence()));

        /*
         * There is a set of nodes which might act as the source for the
         * network restore. Since we can't store arrays, append an index
         * to the property name for each log provider.
         */
        p.setProperty(P_NUMPROVIDERS, Integer.toString(logProviders.size()));
        int i = 0;
        for (ReplicationNode rn: logProviders) {
            p.setProperty(P_NODENAME + i, rn.getName());
            p.setProperty(P_HOSTNAME + i, rn.getHostName());
            p.setProperty(P_PORT + i, Integer.toString(rn.getPort()));
            p.setProperty(P_NODETYPE + i, rn.getType().name());
            i++;
        }
        return p;
    }

    /**
     * @hidden
     *
     * Creates an instance of the exception and packages up the information
     * needed by Subscription API. The target is not a replication node, so the
     * repImpl field is a shell which represents the subscription target.
     */
    public InsufficientLogException(RepImpl repImpl, VLSN refreshVLSN) {
        super(repImpl, EnvironmentFailureReason.INSUFFICIENT_LOG);
        this.repImpl = repImpl;
        this.refreshVLSN = refreshVLSN;
        /*
         * No log providers in this use case, but initialize the set for
         * initProperties, and for robustness.
         */
        this.logProviders = new HashSet<ReplicationNode>();
        props = initProperties("NO_GROUP");
    }

    /**
     * @hidden
     *
     * Creates an instance of the exception when a LOG_RESTORE_REQUIRED was
     * found at recovery, and network restore must be initiated before the
     * recovery can succeed. The flow is:
     * 0. A network restore is underway. It writes a marker file, essentially
     *    serializing this ILE as a property list, and writing it into the log.
     *    Something then interrupts this network restore. The process dies,
     *    so all knowledge of the interrupted network restore is lost in
     *    memory but the the marker file acts as a persistent breadcrumb.
     * 1. Since knowledge of the network restore was lost, the application tries
     *    to open and recover the target node. The LOG_RESTORE_REQUIRED entry
     *    is seen, which means that recovery can't continue.
     * 2. Within recovery, a new ILE is created using information from the
     *    LOG_RESTORE_REQUIRED. It's thrown, which ends recovery.
     * 3. The caller realizes that a network restore has to be carried out
     *    before this environment can be recovered. It uses the ILE instance
     *    created in step 2 to start a new invocation of network restore.
     * 4. Network restore starts. Since network restore needs a RepImpl, one
     *    is instantiated using info from the ILE
     * 5. When Network restore succeeds, it removes the marker file.
     *
     * @param helperHosts extra helper hosts are derived from those used
     * to open the environment.
     */
    public InsufficientLogException(Properties properties,
                                    String helperHosts) {
        super(null, EnvironmentFailureReason.INSUFFICIENT_LOG);

        /*
         * Don't initialize the repImpl until it's needed. If we try to do
         * so in step2, we'll be in a loop, trying to recover an environment
         * from  within recovery.
         */
        String vlsnVal =  properties.getProperty(REFRESH_VLSN);
        this.refreshVLSN = new VLSN(Long.parseLong(vlsnVal));
        this.logProviders = new HashSet<>();

        if (helperHosts != null) {
            for (String hostPortPair : helperHosts.split(",")) {
                final String hpp = hostPortPair.trim();
                if (hpp.length() > 0) {
                    LogFileSource source = 
                        new LogFileSource("NoName", // not important
                                          NodeType.ELECTABLE.name(),
                                          HostPortPair.getHostname(hpp),
                                          HostPortPair.getPort(hpp));
                    logProviders.add(source);
                }
            }
        }
        this.props = properties;
    }

    private void initRepImpl() {
        /*
         * Setup log providers. Since can't use something that supports array
         * types like JSON, and must use a property list, the provider flags
         * are named NODE_NAME0, HOSTNAME0, etc.
         */
        int numLogProviders =
            Integer.parseInt(props.getProperty(P_NUMPROVIDERS));
        for (int i = 0; i < numLogProviders; i++) {
            String name = props.getProperty(P_NODENAME + i);
            String nodeType = props.getProperty(P_NODETYPE + i);
            String hostname = props.getProperty(P_HOSTNAME + i);
            int port = Integer.parseInt(props.getProperty(P_PORT + i));
            logProviders.add(new LogFileSource(name, nodeType, hostname, port));
        }

        /*
         * Create new, read only, internal handle type environment for use
         * by the network backup. 
         */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        envConfig.setConfigParam(EnvironmentParams.ENV_RECOVERY.getName(),
                                 "false");
        envConfig.setTransactional(true);

        String hostname = props.getProperty(HOSTNAME);
        int portVal = Integer.parseInt(props.getProperty(PORT));
        ReplicationConfig repConfig =
            new ReplicationConfig(props.getProperty(GROUP_NAME),
                                  props.getProperty(NODE_NAME),
                                  HostPortPair.getString(hostname, portVal));

        repConfig.setConfigParam(RepParams.NETWORKBACKUP_USE.getName(), "true");
        ReplicationNetworkConfig defaultNetConfig =
            ReplicationNetworkConfig.createDefault();
        repConfig.setRepNetConfig(defaultNetConfig);

        File envDir = new File(props.getProperty(ENV_DIR));
        ReplicatedEnvironment restoreEnv =
            RepInternal.createInternalEnvHandle(envDir,
                                                repConfig,
                                                envConfig);
        this.repImpl = RepInternal.getRepImpl(restoreEnv);
        openedByILE = true;
    }

    /**
     * For internal use only.
     * @hidden
     */
    private InsufficientLogException(String message,
                                     InsufficientLogException cause) {
        super(message, cause);
        this.repImpl = cause.repImpl;
        this.openedByILE = cause.openedByILE;
        this.refreshVLSN = cause.refreshVLSN;
        this.logProviders = cause.logProviders;
        this.props = cause.props;
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public EnvironmentFailureException wrapSelf(String msg) {
        return new InsufficientLogException(msg, this);
    }

    /**
     * Returns the members of the replication group that can serve as candidate
     * log providers to supply the logs needed by this node.
     *
     * @return a list of members that can provide logs
     */
    public Set<ReplicationNode> getLogProviders() {
        return logProviders;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Returns the replication node whose log files need to be refreshed.
     */
    public RepImpl getRepImpl() {
        if (repImpl == null) {
            initRepImpl();
        }

        return repImpl;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("refreshVLSN=").append(refreshVLSN);
        sb.append(" logProviders=").append(logProviders);
        sb.append(" repImpl=").append(repImpl);
        sb.append(" props=").append(props);
        return sb.toString();
    }

    /**
     * For internal use only.
     * @hidden
     */
    public Properties getProperties() {
        return props;
    }

    /*
     * A standin for a ReplicationNode, so NetworkRestore can establish a
     * protocol connection with the source of the log files.
     */
    private class LogFileSource implements ReplicationNode {

        private final String name;
        private final NodeType type;
        private final String hostname;
        private final int port;

        LogFileSource(String name,
                      String nodeTypeName,
                      String hostname,
                      int port) {
            this.name = name;
            this.type = NodeType.valueOf(nodeTypeName);
            this.hostname = hostname;
            this.port = port;
        }


        @Override
        public String getName() {
            return name;
        }

        @Override
        public NodeType getType() {
            return type;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
            return new InetSocketAddress(hostname, port);
        }

        @Override
        public String getHostName() {
            return hostname;
        }

        @Override
        public int getPort() {
            return port;
        }
    }

    /**
     * Called when network restore is complete to close the env if it was
     * opened via this exception.
     *
     * Also sets the repImpl field to null, to avoid OOME when opening the
     * restored environment. This is important whether or not the env was
     * opened via this exception. [#26305]
     */
    void releaseRepImpl() {
        if (repImpl == null) {
            return;
        }

        try {
            if (openedByILE) {
                repImpl.close();
            }
        } finally {
            repImpl = null;
        }
    }
}
