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

package com.sleepycat.je.rep.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.StringTokenizer;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.BooleanConfigParam;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.DurationConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.config.IntConfigParam;
import com.sleepycat.je.config.LongConfigParam;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.util.DbResetRepGroup;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.net.SSLChannelFactory;

public class RepParams {

    /*
     * Note: all replicated parameters should start with
     * EnvironmentParams.REP_PARAM_PREFIX, which is "je.rep.",
     * see SR [#19080].
     */

    /**
     * @hidden
     * Name of a java System property (boolean) which can be turned on in order
     * to avoid input validation checks on node names.  This is undocumented.
     * <p>
     * Generally users should not skip validation, because there are a few
     * kinds of punctuation characters that would cause problems if they were
     * allowed in node names.  But in the past users might have inadvertantly
     * created node names that do not conform to the new, stricter rules.  In
     * that case they would not be able to upgrade to the newer version of JE
     * that now includes this checking.
     * <p>
     * This flag actually applies to the group name too.  But for group names
     * the new rules are actually less strict than they used to be, so there
     * should be no problem.
     */
    public static final String SKIP_NODENAME_VALIDATION =
        "je.rep.skipNodenameValidation";

    /**
     * @hidden
     * Name of a java System property (boolean) which can be turned on in order
     * to avoid hostname resolution checks on helper host values.
     * This is undocumented.
     * <p>
     * Generally users should not skip validation, because having valid helper
     * hosts is an important path to let this node find the master in a
     * replication group when its group membership db is not
     * sufficient. Disabling the check should only be done in unusual cases,
     * such as in the face of intermittent DNS failures. In that case, a
     * hostname which seems to be invalid may actually be a transient problem,
     * rather than a permanent configuration issue. Skipping the resolution
     * check at config setting time may supply some degree of resilience in
     * this unusual case.
     */
    public static final String SKIP_HELPER_HOST_RESOLUTION =
        "je.rep.skipHelperHostResolution";

    /**
     * A JE/HA configuration parameter describing an Identifier name.
     */
    static public class IdentifierConfigParam extends ConfigParam {
        private static final String DEBUG_NAME =
            IdentifierConfigParam.class.getName();

        public IdentifierConfigParam(String configName,
                                     String defaultValue,
                                     boolean mutable,
                                     boolean forReplication) {
            super(configName, defaultValue, mutable, forReplication);
        }

        @Override
        public void validateValue(String value) {
            if (Boolean.getBoolean(SKIP_NODENAME_VALIDATION)) {
                return;
            }
            if ((value == null) || (value.length() == 0)) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ": a value is required");
            }
            for (char c : value.toCharArray()) {
                if (!isValid(c)) {
                    throw new IllegalArgumentException
                        (DEBUG_NAME + ": " + name + ", must consist of " +
                         "letters, digits, hyphen, underscore, period.");
                }
            }
        }

        private boolean isValid(char c) {
            if (Character.isLetterOrDigit(c) ||
                c == '-' ||
                c == '_' ||
                c == '.') {
                return true;
            }
            return false;
        }
    }

    /*
     * Replication group-wide properties. These properties are candidates for
     * consistency checking whenever there is a handshake between a master and
     * replica.
     */

    /** Names the Replication group. */
    public static final ConfigParam GROUP_NAME =
        new IdentifierConfigParam(ReplicationConfig.GROUP_NAME,
                                  "DefaultGroup",      // default
                                  false,               // mutable
                                  true);               // forReplication

    /**
     * @deprecated see {@link ReplicationConfig#REP_STREAM_TIMEOUT}
     */
    public static final DurationConfigParam REP_STREAM_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REP_STREAM_TIMEOUT,
                                null,                         // min
                                null,                         // max
                                "30 min",                     // default
                                false,                        // mutable
                                true);

    /**
     * MIN_RETAINED_VLSNS was never exposed in the public API, although we
     * did ask several users to configure it in the past, so we shouldn't
     * delete the param definition.
     *
     * @deprecated and no longer used as of JE 7.5. Reserved files are now
     * retained based on available disk space -- see
     * {@link EnvironmentConfig#MAX_DISK} and
     * {@link EnvironmentConfig#FREE_DISK} should be used instead.
     * However, this param is still used when some, but not all, nodes in a
     * group have been upgraded to 7.5 or later.
     */
    public static final IntConfigParam MIN_RETAINED_VLSNS =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                           "minRetainedVLSNs",
                           0,               // min
                           null,            // max
                           0,               // default
                           false,           // mutable
                           true);           // forReplication

    /**
     * Unpublished (for now at least) parameter describing the minimum size
     * of the VLSNIndex as a number of VLSNs. Once the index grows to this
     * size, it will not get smaller due to head truncation by the cleaner.
     *
     * <p>When a disk limit is violated we will have already deleted as many
     * reserved files as possible. If space is made available and write
     * operations can resume, we need to ensure that each node has a large
     * enough VLSNIndex to perform syncup, etc.</p>
     *
     * <p>This limit is enforced on both master and replicas. It is
     * particularly important on replicas because feeders will not prevent
     * VLSN index head truncation.</p>
     */
    public static final IntConfigParam MIN_VLSN_INDEX_SIZE =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
            "minVLSNIndexSize",
            0,         // min
            null,      // max
            1000,      // default
            false,     // mutable
            true);     // forReplication

    /**
     * Can be used by tests to prevent the GlobalCBVLSN from being defunct,
     * even when all nodes are DEFUNCT_JE_VERSION or higher.
     */
    public static final BooleanConfigParam TEST_CBVLSN =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "testCBVLSN",
            false,          // default
            false,          // mutable
            true);

    /**
     * @see ReplicationConfig#REPLICA_RECEIVE_BUFFER_SIZE
     */
    public static final IntConfigParam REPLICA_RECEIVE_BUFFER_SIZE =
            new IntConfigParam(ReplicationConfig.REPLICA_RECEIVE_BUFFER_SIZE,
                               0,               // min
                               null,            // max
                               1048576,         // default
                               false,           // mutable
                               true);           // forReplication

    /**
     * The size of the message queue used for communication between the thread
     * reading the replication stream and the thread doing the replay. The
     * default buffer size has been chosen to hold 500 single operation
     * transactions (the ln + commit record) assuming 1K sized LN record.
     * <p>
     * Larger values of buffer size may result in higher peak memory
     * utilization, due to a larger number of LNs sitting in the queue. The
     * size of the queue itself is unlikely to be an issue, since it's tiny
     * relative to cache sizes. At 1000, 1kbyte LNs it raises the peak
     * utilization by 1MB which for most apps is an insignificant rise in the
     * peak.
     *
     * Note that the parameter is lazily mutable, that is, the change will take
     * effect the next time the node transitions to a replica state.
     */
    public static final IntConfigParam REPLICA_MESSAGE_QUEUE_SIZE =
            new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "replicaMessageQueueSize",
                               1,               // min
                               null,            // max
                               1000,            // default
                               true,            // mutable
                               true);           // forReplication

    /**
     * The lock timeout for replay transactions.
     */
    public static final DurationConfigParam REPLAY_TXN_LOCK_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLAY_TXN_LOCK_TIMEOUT,
                                "1 ms",            // min
                                "75 min",          // max
                                "500 ms",          // default
                                false,             // mutable
                                true);             // forReplication

    /**
     * @see ReplicationConfig#ENV_SETUP_TIMEOUT
     */
    public static final DurationConfigParam ENV_SETUP_TIMEOUT =
        new DurationConfigParam
        (ReplicationConfig.ENV_SETUP_TIMEOUT,
         null,                          // min
         null,                          // max
         "10 h",                        // default 10 hrs
         false,                         // mutable
         true);

    /**
     * @see ReplicationConfig#ENV_CONSISTENCY_TIMEOUT
     */
    public static final DurationConfigParam
        ENV_CONSISTENCY_TIMEOUT =
            new DurationConfigParam(ReplicationConfig.ENV_CONSISTENCY_TIMEOUT,
                                    "10 ms",                      // min
                                    null,                         // max
                                    "5 min",                      // default
                                    false,                        // mutable
                                    true);

    /**
     * @see ReplicationConfig#ENV_UNKNOWN_STATE_TIMEOUT
     */
    public static final DurationConfigParam ENV_UNKNOWN_STATE_TIMEOUT =
        new DurationConfigParam
        (ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT,
         null,                          // min
         null,                          // max
         "0 s",                         // default
         false,                         // mutable
         true);

    /**
     * @see ReplicationConfig#REPLICA_ACK_TIMEOUT
     */
    public static final DurationConfigParam REPLICA_ACK_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLICA_ACK_TIMEOUT,
                                "10 ms",                     // min
                                null,                        // max
                                "5 s",                       // default
                                false,                       // mutable
                                true);                       // forReplication

    /**
     * @see ReplicationConfig#INSUFFICIENT_REPLICAS_TIMEOUT
     */
    public static final DurationConfigParam INSUFFICIENT_REPLICAS_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT,
                                "10 ms",                     // min
                                null,                        // max
                                "10 s",                       // default
                                false,                       // mutable
                                true);                       // forReplication
    
    /**
     * @hidden
     * @see ReplicationConfig#ARBITER_ACK_TIMEOUT
     */
    public static final DurationConfigParam ARBITER_ACK_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.ARBITER_ACK_TIMEOUT,
                                "10 ms",                     // min
                                null,                        // max
                                "2 s",                       // default
                                false,                       // mutable
                                true);                       // forReplication

    /**
     * Internal parameter enable use of the group ack message. It's on by
     * default since protocol version 6.
     */
    public static final BooleanConfigParam ENABLE_GROUP_ACKS =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "enableGroupAcks",
                               true,             // default
                               false,            // mutable
                               true);

    /**
     * The maximum message size which will be accepted by a node (to prevent
     * DOS attacks).  While the default shown here is 0, it dynamically
     * calculated when the node is created and is set to the half of the
     * environment cache size. The cache size is mutable, but changing the
     * cache size at run time (after environment initialization) will not
     * change the value of this parameter.  If a value other than cache size /
     * 2 is desired, this non-mutable parameter should be specified at
     * initialization time.
     */
    public static final LongConfigParam MAX_MESSAGE_SIZE =
        new LongConfigParam(ReplicationConfig.MAX_MESSAGE_SIZE,
                            Long.valueOf(1 << 18),        // min (256KB)
                            Long.valueOf(Long.MAX_VALUE), // max
                            Long.valueOf(0),         // default (cachesize / 2)
                            false,                   // mutable
                            true);                   // forReplication

    /**
     * Identifies the default consistency policy used by a replica. Only two
     * policies are meaningful as properties denoting environment level default
     * policies: NoConsistencyRequiredPolicy and TimeConsistencyPolicy.  They
     * can be specified as: NoConsistencyRequiredPolicy or
     * TimeConsistencyPolicy(<permissibleLag>,<timeout>). For example, a time
     * based consistency policy with a lag of 1 second and a timeout of 1 hour
     * is denoted by the string: TimeConsistencyPolicy(1000,3600000)
     */
    public static final ConfigParam CONSISTENCY_POLICY =
        new ConfigParam(ReplicationConfig.CONSISTENCY_POLICY,
                        // Default lag of 1 sec, and timeout of 1 hour
                        "TimeConsistencyPolicy(1 s,1 h)",
                        false,                   // mutable
                        true) {                  // for Replication
        @Override
        public void validateValue(String propertyValue)
            throws IllegalArgumentException {

            /* Evaluate for the checking side-effect. */
            RepUtils.getReplicaConsistencyPolicy(propertyValue);
        }
    };

    /* The ports used by a replication group */

    /**
     * The port used for replication.
     */
    public static final IntConfigParam DEFAULT_PORT =
        new IntConfigParam(ReplicationConfig.DEFAULT_PORT,
                           Integer.valueOf(1024),   // min
                           Integer.valueOf(Short.MAX_VALUE), // max
                           Integer.valueOf(5001),   // default
                           false,                   // mutable
                           true);                   // forReplication

    /**
     * Names the host (or interface) and port associated with the node in the
     * replication group, e.g. je.rep.nodeHostPort=foo.com:5001
     */
    public static final ConfigParam NODE_HOST_PORT =
        new ConfigParam(ReplicationConfig.NODE_HOST_PORT,
                        "localhost",         // default
                        false,               // mutable
                        true) {              // forReplication

        @Override
        public void validateValue(String hostAndPort)
            throws IllegalArgumentException {

            if ((hostAndPort == null) || (hostAndPort.length() == 0)) {
                throw new IllegalArgumentException
                    ("The value cannot be null or zero length: " + name);
            }
            int colonToken = hostAndPort.indexOf(":");
            String hostName = (colonToken >= 0) ?
                               hostAndPort.substring(0, colonToken) :
                               hostAndPort;
            ServerSocket testSocket = null;
            try {
                testSocket = new ServerSocket();
                /* The bind will fail if the hostName does not name this m/c.*/
                testSocket.bind(new InetSocketAddress(hostName, 0));
                testSocket.close();
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException
                    ("Property: " + name +
                     " Invalid hostname: " + hostName, e);
            } catch (IOException e) {

                /*
                 * Server socket could not be bound to any port. Hostname is
                 * not associated with this m/c.
                 */
                throw new IllegalArgumentException
                    ("Property: " + name +
                     " Invalid hostname: " + hostName, e);
            }

            if (colonToken >= 0) {
                validatePort(hostAndPort.substring(colonToken + 1));
            }
        }
    };

    /*
     * The Name uniquely identifies this node within the replication group.
     */
    public static final ConfigParam NODE_NAME =
        new IdentifierConfigParam(ReplicationConfig.NODE_NAME,
                                  "DefaultRepNodeName",// default
                                  false,               // mutable
                                  true);               // forReplication

    /*
     * Identifies the type of the node.
     */
    public static final EnumConfigParam<NodeType> NODE_TYPE =
        new EnumConfigParam<NodeType>(ReplicationConfig.NODE_TYPE,
                                      NodeType.ELECTABLE,         // default
                                      false,                      // mutable
                                      true,
                                      NodeType.class);

    /*
     * Associated a priority with this node. The priority is used during
     * elections to favor one node over another. All other considerations being
     * equal, the priority is used as a tie-breaker; the node with the higher
     * priority is selected as the master.
     */
    public static final IntConfigParam NODE_PRIORITY =
        new IntConfigParam(ReplicationMutableConfig.NODE_PRIORITY,
                           Integer.valueOf(0),   // min
                           Integer.valueOf(Integer.MAX_VALUE), // max
                           Integer.valueOf(1),   // default
                           true,                 // mutable
                           true);                // forReplication

    /*
     * Allow for Arbiter to provide Acks.
     */
    public static final BooleanConfigParam ALLOW_ARBITER_ACK =
        new BooleanConfigParam(ReplicationMutableConfig.ALLOW_ARBITER_ACK,
                               true,           // default
                               true,            // mutable
                               true);

    public static final IntConfigParam ARBITER_OUTPUT_QUEUE_SIZE =
            new IntConfigParam(ReplicationConfig.ARBITER_OUTPUT_QUEUE_SIZE,
                               Integer.valueOf(128),  // min
                               null,              // max
                               Integer.valueOf(4096), // default
                               false,             // mutable
                               false);            // forReplication

    /*
     * Identifies the Primary node in a two node group.
     */
    public static final BooleanConfigParam DESIGNATED_PRIMARY =
        new BooleanConfigParam(ReplicationMutableConfig.DESIGNATED_PRIMARY,
                               false,           // default
                               true,            // mutable
                               true);

    /*
     * An internal option used to control the use of Nagle's algorithm
     * on feeder connections. A value of true disables use of Nagle's algorithm
     * and causes output to be sent immediately without delay.
     */
    public static final BooleanConfigParam FEEDER_TCP_NO_DELAY =
            new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                   "feederTcpNoDelay",
                                   true,              // default
                                   false,            // mutable
                                   true);

    /**
     * The time interval in nanoseconds during which records from a feeder may
     * be batched before being written to the network.
     *
     * Larger values can result in fewer network packets and lower interrupt
     * processing overheads. Since the grouping is only done when the feeder
     * knows that the replica is not completely in sync, it's unlikely to have
     * an adverse impact on overall throughput. Consequently this parameter is
     * retained as an internal tuning knob.
     *
     * The HEARTBEAT_INTERVAL parameter serves as a ceiling on this time
     * interval. Parameter values larger than HEARTBEAT_INTERVAL are truncated
     * to HEARTBEAT_INTERVAL.
     */
    public static final IntConfigParam FEEDER_BATCH_NS =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                           "feederBatchNs",
                           Integer.valueOf(0),                // min
                           Integer.valueOf(Integer.MAX_VALUE),// max
                           Integer.valueOf(1000000),           // default 1 ms
                           true,                              // mutable
                           true);                             // forReplication

    /**
     * The size in KB used to batch outgoing feeder records. Upon overflow the
     * existing buffer contents are written to the network and a new batch is
     * initiated. The default value is 64K to take advantage of networks that
     * support jumbo frames.
     */
    public static final IntConfigParam FEEDER_BATCH_BUFF_KB =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                           "feederBatchBuffKb",
                           Integer.valueOf(4),                // min
                           Integer.valueOf(Integer.MAX_VALUE),// max
                           Integer.valueOf(64),               // default 64K
                           true,                              // mutable
                           true);                             // forReplication

    /**
     * @see ReplicationMutableConfig#ELECTABLE_GROUP_SIZE_OVERRIDE
     */
    public static final IntConfigParam ELECTABLE_GROUP_SIZE_OVERRIDE =
        new IntConfigParam(ReplicationMutableConfig.
                           ELECTABLE_GROUP_SIZE_OVERRIDE,
                           Integer.valueOf(0),                // min
                           Integer.valueOf(Integer.MAX_VALUE),// max
                           Integer.valueOf(0),                // default
                           true,                              // mutable
                           true);                             // forReplication

    /**
     * An internal option, accessed only via the utility
     * {@link DbResetRepGroup} utility, to reset a replication group to a
     * single new member when the replicated environment is opened.
     */
    public static final BooleanConfigParam RESET_REP_GROUP =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "resetRepGroup",
                               false,            // default
                               false,            // mutable
                               true);

    /**
     * An internal option, used with {@link #RESET_REP_GROUP}, that causes the
     * reset of the replication group to retain the original group UUID and to
     * not truncate the VLSN index.  Use this option when converting a
     * SECONDARY node to an ELECTABLE node when recovering a replication group.
     */
    public static final BooleanConfigParam RESET_REP_GROUP_RETAIN_UUID =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "resetRepGroupRetainUUID",
                               false,            // default
                               false,            // mutable
                               true);

    /**
     * An internal option to allow converting an ELECTABLE node to a SECONDARY
     * node by ignoring the electable node ID stored in the local rep group
     * DB.
     */
    public static final BooleanConfigParam IGNORE_SECONDARY_NODE_ID =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "ignoreSecondaryNodeId",
                               false,            // default
                               false,            // mutable
                               true);

    /*
     * Sets the maximum allowable skew between a Feeder and its replica. The
     * clock skew is checked as part of the handshake when the Replica
     * establishes a connection to its Feeder.
     */
    public static final DurationConfigParam MAX_CLOCK_DELTA =
        new DurationConfigParam(ReplicationConfig.MAX_CLOCK_DELTA,
                                null,               // min
                                "1 min",            // max
                                "2 s",              // default
                                false,              // mutable
                                true);              // forReplication

    /*
     * The list of helper node and port pairs.
     */
    public static final ConfigParam HELPER_HOSTS =
        new ConfigParam(ReplicationConfig.HELPER_HOSTS,
                        "",                  // default
                        true,                // mutable
                        true) {              // forReplication

        @Override
        public void validateValue(String hostPortPairs)
            throws IllegalArgumentException {

            if ((hostPortPairs == null) || (hostPortPairs.length() == 0)) {
                return;
            }
            HashSet<String> hostPortSet = new HashSet<String>();
            for (StringTokenizer tokenizer =
                 new StringTokenizer(hostPortPairs, ",");
                 tokenizer.hasMoreTokens();) {
                try {
                    String hostPortPair = tokenizer.nextToken();
                    if (!hostPortSet.add(hostPortPair)) {
                        throw new IllegalArgumentException
                            ("Property: " + name +
                             " Duplicate specification: " + hostPortPair);
                    }
                    validateHostAndPort(
                        hostPortPair,
                        Boolean.getBoolean(SKIP_HELPER_HOST_RESOLUTION));
                } catch (IllegalArgumentException iae) {
                    throw new IllegalArgumentException
                        ("Property: " + name + "Error: " + iae.getMessage(),
                         iae);
                }
            }
        }
    };

    /* Heartbeat interval in milliseconds. */
    public static final IntConfigParam HEARTBEAT_INTERVAL =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "heartbeatInterval",
         Integer.valueOf(1000),// min
         null,                 // max
         Integer.valueOf(1000),// default
         false,                // mutable
         true);                // forReplication

    /*
     * Security check interval in milliseconds. This parameter controls how
     * frequently the feeder checks that stream consumers authenticated and
     * authorized to stream the requested tables.
     */
    public static final IntConfigParam SECURITY_CHECK_INTERVAL =
        new IntConfigParam
            (EnvironmentParams.REP_PARAM_PREFIX + "securityCheckInterval",
             Integer.valueOf(1),   // min
             null,                 // max
             Integer.valueOf(1000),// default
             false,                // mutable
             true);                // forReplication

    /* Replay Op Count after which we clear the DbTree cache. */
    public static final IntConfigParam DBTREE_CACHE_CLEAR_COUNT =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "dbIdCacheOpCount",
         Integer.valueOf(1),    // min
         null,                  // max
         Integer.valueOf(5000), // default
         false,                 // mutable
         true);                 // forReplication

    public static final IntConfigParam VLSN_STRIDE =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX + "vlsn.stride",
                           Integer.valueOf(1),     // min
                           null,                   // max
                           Integer.valueOf(10),    // default
                           false,                  // mutable
                           true);                  // forReplication

    public static final IntConfigParam VLSN_MAX_MAP =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "vlsn.mappings",
         Integer.valueOf(1),      // min
         null,                    // max
         Integer.valueOf(1000),   // default
         false,                   // mutable
         true);                   // forReplication

    public static final IntConfigParam VLSN_MAX_DIST =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "vlsn.distance",
         Integer.valueOf(1),      // min
         null,                    // max
         Integer.valueOf(100000), // default
         false,                   // mutable
         true);                   // forReplication

    /*
     * Internal testing use only: Simulate a delay in the replica loop for test
     * purposes. The value is the delay in milliseconds.
     */
    public static final IntConfigParam TEST_REPLICA_DELAY =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "test.replicaDelay",
         Integer.valueOf(0), // min
         Integer.valueOf(Integer.MAX_VALUE), // max
         Integer.valueOf(0), // default
         false,              // mutable
         true);              // forReplication

    /*
     * Sets the VLSNIndex cache holding recent log items in support of the
     * feeders. The size must be a power of two.
     */
    public static final IntConfigParam VLSN_LOG_CACHE_SIZE =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "vlsn.logCacheSize",
         Integer.valueOf(0),      // min
         Integer.valueOf(1<<10),  // max
         Integer.valueOf(32),     // default
         false,                   // mutable
         true);                   // forReplication

    /*
     * The socket timeout value used by a Replica when it opens a new
     * connection to establish a replication stream with a feeder.
     */
    public static final DurationConfigParam REPSTREAM_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "repstreamOpenTimeout",
         null,               // min
         "5 min",            // max
         "5 s",              // default
         false,              // mutable
         true);              // forReplication

    /*
     * The socket timeout value used by Elections agents when they open
     * sockets to communicate with each other using the Elections protocol.
     */
    public static final DurationConfigParam ELECTIONS_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "electionsOpenTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * The maximum amount of time a Learner or Acceptor agent will wait for
     * input on a network connection, while listening for a message before
     * timing out. This timeout applies to the Elections protocol.
     */
    public static final DurationConfigParam ELECTIONS_READ_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "electionsReadTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /**
     * The master re-broadcasts the results of an election with this period.
     */
    public static final DurationConfigParam
        ELECTIONS_REBROADCAST_PERIOD =
        new DurationConfigParam
        (ReplicationConfig.ELECTIONS_REBROADCAST_PERIOD,
         null,               // min
         null,               // max
         "1 min",            // default
         false,              // mutable
         true);

    /**
     * @see ReplicationConfig#ELECTIONS_PRIMARY_RETRIES
     */
    public static final IntConfigParam ELECTIONS_PRIMARY_RETRIES =
        new IntConfigParam(ReplicationConfig.ELECTIONS_PRIMARY_RETRIES,
                           0,
                           Integer.MAX_VALUE,
                           2,
                           false,
                           true);

    /*
     * Socket open timeout for use with the RepGroupProtocol.
     */
    public static final DurationConfigParam REP_GROUP_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "repGroupOpenTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * Socket read timeout for use with the RepGroupProtocol.
     */
    public static final DurationConfigParam REP_GROUP_READ_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "repGroupReadTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * Socket open timeout for use with the Monitor Protocol.
     */
    public static final DurationConfigParam MONITOR_OPEN_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "monitorOpenTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /*
     * Socket read timeout for use with the MonitorProtocol.
     */
    public static final DurationConfigParam MONITOR_READ_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "monitorReadTimeout",
         null,               // min
         "1 min",            // max
         "10 s",             // default
         false,              // mutable
         true);              // forReplication

    /**
     * @see ReplicationConfig#REPLICA_TIMEOUT
     */
    public static final DurationConfigParam REPLICA_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLICA_TIMEOUT,
                           "2 s", // min
                           null, // max
                           "30 s", // default
                           false,              // mutable
                           true);              // forReplication

    /* @see ReplicationConfig#REPLAY_MAX_OPEN_DB_HANDLES */
    public static final IntConfigParam REPLAY_MAX_OPEN_DB_HANDLES =
        new IntConfigParam(ReplicationMutableConfig.REPLAY_MAX_OPEN_DB_HANDLES,
                           Integer.valueOf(1), // min
                           Integer.valueOf(Integer.MAX_VALUE), // max
                           Integer.valueOf(10), // default
                           true,               // mutable
                           true);              // forReplication

    /* @see ReplicationConfig#REPLAY_DB_HANDLE_TIMEOUT */
    public static final DurationConfigParam REPLAY_DB_HANDLE_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.REPLAY_DB_HANDLE_TIMEOUT,
                                "1 s",              // min
                                null,               // max
                                "30 s",             // default
                                true,              // mutable
                                true);              // forReplication

    /* @see ReplicationConfig#REPLICA_MAX_GROUP_COMMIT */
    public static final IntConfigParam REPLICA_MAX_GROUP_COMMIT =
        new IntConfigParam(ReplicationConfig.REPLICA_MAX_GROUP_COMMIT,
                           Integer.valueOf(0),      // min
                           null,                    // max
                           Integer.valueOf(200),    // default
                           false,                   // mutable
                           true);                   // forReplication

    /* @see ReplicationConfig#REPLICA_GROUP_COMMIT_INTERVAL */
    public static final DurationConfigParam REPLICA_GROUP_COMMIT_INTERVAL =
        new DurationConfigParam(ReplicationConfig.REPLICA_GROUP_COMMIT_INTERVAL,
                                "0 ms",             // min
                                null,               // max
                                "3 ms",             // default
                                false,              // mutable
                                true);              // forReplication

    /*
     * The number of heartbeat responses that must be detected as missing
     * during an otherwise idle period before the Feeder shuts down the
     * connection with the Replica.
     *
     * This value provides the basis for the "read timeout" used by the Feeder
     * when communicating with the Replica. The timeout is calculated as
     * FEEDER_HEARTBEAT_TIMEOUT * HEARTBEAT_INTERVAL. Upon a timeout the Feeder
     * closes the connection.
     *
     * Reducing this value permits the master to discover failed Replicas
     * faster. However, it increases the chances of false positives as well, if
     * the network is experiencing transient problems from which it might
     * just recover.
     */
    public static final IntConfigParam FEEDER_HEARTBEAT_TIMEOUT =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "feederHeartbeatTrigger",
         Integer.valueOf(0), // min
         Integer.valueOf(Integer.MAX_VALUE), // max
         Integer.valueOf(4), // default
         false,              // mutable
         true);

    /**
     * Used to force setting of SO_REUSEADDR to true on the HA server socket
     * when it binds to its port.
     *
     * Note that the default is false, meaning that the socket has the
     * system-specific default setting associated with it. We set it to true
     * primarily in unit tests where the interacting HA processes are all on
     * the same machine and use of this option is safe.
     *
     * This option is currently intended just for internal test use.
     */
    public static final BooleanConfigParam SO_REUSEADDR =
        new BooleanConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "soReuseAddr",
         false,             // default
         false,             // mutable
         true);

    /**
     * This option was motivated by the BDA. The BDA uses IB for intra-rack
     * node communications and 10gigE for inter-rack node communications. DNS
     * is used to map the hostname to different IP addresses depending on
     * whether the hostname was resolved from within the rack or outside it.
     * The host thus gets HA traffic on both the IB and 10gigE interfaces and
     * therefore needs to listen on both interfaces. It does so binding its
     * socket using a wild card address when this option iks turned on.
     *
     * @see ReplicationConfig#BIND_INADDR_ANY
     */
    public static final BooleanConfigParam BIND_INADDR_ANY =
        new BooleanConfigParam
        (ReplicationConfig.BIND_INADDR_ANY,
         false,             // default
         false,             // mutable
         true);

    /**
     * Determines how long to wait for a bound socket to come free. This option
     * can be useful when dealing with sockets in the TIME_WAIT state to come
     * free so they can be reused. Attempts are made to retry binding to this
     * period at intervals of 1 second until the port is bound successfully, or
     * this wait period is exceeded.
     *
     * A value of zero means that there are no retries. It does not make sense
     * to wait too much longer than the 2 min TIME_WAIT period, but we allow
     * waiting as long as 2.5 min to account for race conditions.
     *
     * This option is currently intended just for internal test use.
     */
    public static final IntConfigParam SO_BIND_WAIT_MS =
        new IntConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "retrySocketBind",
         Integer.valueOf(0), // min
         Integer.valueOf(150 * 1000), // max
         Integer.valueOf(0), // default
         false,              // mutable
         true);

    /**
     * Internal parameter used to determine the poll timeout used when
     * accepting incoming feeder connections. This timeout also determines
     * the frequency of various housekeeping tasks, e.g. detection of a
     * master to replica change, etc.
     */
    public static final DurationConfigParam FEEDER_MANAGER_POLL_TIMEOUT =
        new DurationConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                "feederManagerPollTimeout",
                                "100 ms", // min
                                 null, // max
                                "1 s", // default
                                false,              // mutable
                                true);              // forReplication
    /**
     * @see ReplicationConfig#FEEDER_TIMEOUT
     */
    public static final DurationConfigParam FEEDER_TIMEOUT =
        new DurationConfigParam(ReplicationConfig.FEEDER_TIMEOUT,
                                "2 s", // min
                                null, // max
                                "30 s", // default
                                false,              // mutable
                                true);              // forReplication

    /**
     * Used to log an info message when a commit log record exceeds this
     * time interval from the time it was created, to the time it was written
     * out to the network.
     */
    public static final DurationConfigParam TRANSFER_LOGGING_THRESHOLD =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "transferLoggingThreshold",
         "1 ms",              // min
         null,               // max
         "5 s",              // default
         false,              // mutable
         true);              // forReplication

    /**
     * Used to log an info message when the time taken to replay a single log
     * entry at a replica exceeds this threshold.
     */
    public static final DurationConfigParam REPLAY_LOGGING_THRESHOLD =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "replayLoggingThreshold",
         "1 ms",             // min
         null,               // max
         "5 s",              // default
         false,              // mutable
         true);              // forReplication

    /**
     * Changes the notion of an ack. When set to true, a replica is considered
     * to have acknowledged a commit as soon as the feeder has written the
     * commit record to the network. That is, it does not wait for the replica
     * to actually acknowledge the commit via a return message. This permits
     * the master to operate in a more async manner relative to the replica
     * provide for higher throughput.
     *
     * This config parameter is internal.
     */
    public static final BooleanConfigParam COMMIT_TO_NETWORK =
        new BooleanConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "commitToNetwork",
         false,            // default
         false,             // mutable
         true);

    public static final DurationConfigParam PRE_HEARTBEAT_TIMEOUT =
        new DurationConfigParam
        (EnvironmentParams.REP_PARAM_PREFIX + "preHeartbeatTimeoutMs",
         "1 s", // min
         null, // max
         "60 s", // default
         false,              // mutable
         true);

    /**
     * Verifies that the port is a reasonable number. The port must be outside
     * the range of "Well Known Ports" (zero through 1024).
     *
     * @param portString the string representing the port.
     */
    private static void validatePort(String portString)
        throws IllegalArgumentException {

        try {
            int port = Integer.parseInt(portString);

            if ((port <= 0) || (port > 0xffff)) {
                throw new IllegalArgumentException
                    ("Invalid port number: " + portString);
            }
            if (port <= 1023) {
                throw new IllegalArgumentException
                    ("Port number " + port +
                     " is invalid because the port must be "+
                     "outside the range of \"well known\" ports");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
                ("Invalid port number: " + portString);
        }
    }

    /**
     * Validates that the hostPort is a string of the form:
     *
     * hostName[:port]
     *
     * @param hostAndPort
     * @param skipHostnameResolution if true, don't bother checking that the
     * hostname resolves
     * @throws IllegalArgumentException
     */
    private static void validateHostAndPort(String hostAndPort,
                                            boolean skipHostnameResolution)
        throws IllegalArgumentException {

        int colonToken = hostAndPort.indexOf(":");
        String hostName = (colonToken >= 0) ?
            hostAndPort.substring(0, colonToken) :
            hostAndPort;
        if ("".equals(hostName)) {
            throw new IllegalArgumentException("missing hostname");
        }

        if (!skipHostnameResolution) {
            try {
                InetAddress.getByName(hostName);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException
                    ("Invalid hostname: " + e.getMessage());
            }
        }

        if (colonToken >= 0) {
            validatePort(hostAndPort.substring(colonToken + 1));
        }
    }

    /**
     * @see ReplicationConfig#TXN_ROLLBACK_LIMIT
     */
    public static final IntConfigParam TXN_ROLLBACK_LIMIT =
        new IntConfigParam(ReplicationConfig.
                           TXN_ROLLBACK_LIMIT,
                           Integer.valueOf(0),                // min
                           Integer.valueOf(Integer.MAX_VALUE),// max
                           Integer.valueOf(10),               // default
                           false,                             // mutable
                           true);                             // forReplication

    /**
     * @see ReplicationConfig#TXN_ROLLBACK_DISABLED
     */
    public static final BooleanConfigParam TXN_ROLLBACK_DISABLED =
        new BooleanConfigParam(ReplicationConfig.TXN_ROLLBACK_DISABLED,
                               false,           // default
                               false,           // mutable
                               true);           // forReplication

    /**
     * @see ReplicationConfig#ALLOW_UNKNOWN_STATE_ENV_OPEN
     */
    @SuppressWarnings({ "javadoc", "deprecation" })
    public static final BooleanConfigParam ALLOW_UNKNOWN_STATE_ENV_OPEN =
        new BooleanConfigParam(ReplicationConfig.ALLOW_UNKNOWN_STATE_ENV_OPEN,
                               false,           // default
                               false,           // mutable
                               true);

    /**
     * If true, the replica runs with this property will not join the
     * replication group.
     */
    public static final BooleanConfigParam DONT_JOIN_REP_GROUP =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "dontJoinRepGroup",
                               false,
                               false,
                               true);

    /**
     * Internal parameter used by the Arbiter.
     */
    public static final BooleanConfigParam ARBITER_USE =
            new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                   "arbiterUse",
                                   false,
                                   false,
                                   true);

    /**
     * Internal parameter used by the Subscriber.
     *
     * If true, the node is a replica that operates as a subscriber.
     */
    public static final BooleanConfigParam SUBSCRIBER_USE =
            new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                   "subscriberUse",
                                   false,
                                   false,
                                   true);

    /**
     * Internal parameter used by network backups.
     *
     * If true, the node is used to support network backups and operates in
     * read only mode, with various daemons disabled.
     *
     * TODO: would be nice to combine ARBITER_USE, SUBSCRIBER_USE and this into
     * one concept.
     */
    public static final BooleanConfigParam NETWORKBACKUP_USE =
            new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                   "networkBackupUse",
                                   false,
                                   false,
                                   true);

    /**
     * Internal parameter used by network backups.
     *
     * See 'Algorithm' in {@link com.sleepycat.je.rep.NetworkRestore}.
     * Is currently 50k, which represents less than 1s of replay time.
     */
    public static final IntConfigParam NETWORKBACKUP_MAX_LAG =
        new IntConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
            "networkBackupMaxLag",
            0,         // min
            null,      // max
            50 * 1000, // default
            false,     // mutable
            true);     // forReplication

    /**
     * Internal parameter to preserve record version (VLSN).  Is immutable
     * forever, i.e., it may not be changed after the environment has been
     * created.  It has the following impacts:
     *
     * . The VLSN is stored with the LN in the Btree and is available via the
     *   CursorImpl API.
     * . The VLSN is included when migrating an LN during log cleaning.
     *
     * FUTURE: Expose this in ReplicationConfig and improve doc if we make
     * record versions part of the public API.
     */
    public static final BooleanConfigParam PRESERVE_RECORD_VERSION =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "preserveRecordVersion",
                               false,         // default
                               false,         // mutable
                               true);         // forReplication

    /**
     * Whether to cache the VLSN in the BIN after the LN has been stripped by
     * eviction, unless caching is explicitly disabled using the
     * CACHE_RECORD_VERSION setting.
     *
     * This setting has no impact if PRESERVE_RECORD_VERSION is not also
     * enabled.
     *
     * FUTURE: Expose this in ReplicationConfig and improve doc if we make
     * record versions part of the public API.
     */
    public static final BooleanConfigParam CACHE_RECORD_VERSION =
        new BooleanConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                               "cacheRecordVersion",
                               true,          // default
                               false,         // mutable
                               true);         // forReplication

    /**
     * @see ReplicationConfig#PROTOCOL_OLD_STRING_ENCODING
     * TODO: Change default to false in JE 5.1.
     */
    public static final BooleanConfigParam PROTOCOL_OLD_STRING_ENCODING =
        new BooleanConfigParam(ReplicationConfig.PROTOCOL_OLD_STRING_ENCODING,
                               true,          // default
                               false,         // mutable
                               true);         // forReplication

    /**
     * A JE/HA configuration parameter specifying a data channel type
     */
    static public class ChannelTypeConfigParam extends ConfigParam {
        public static final String BASIC = "basic";
        public static final String SSL = "ssl";
        public static final String CUSTOM = "custom";

        private static final String DEBUG_NAME =
            ChannelTypeConfigParam.class.getName();

        public ChannelTypeConfigParam(String configName,
                                      String defaultValue,
                                      boolean mutable,
                                      boolean forReplication) {
            super(configName, defaultValue, mutable, forReplication);
        }

        @Override
        public void validateValue(String value) {
            if (value == null) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ": a value is required");
            }
            if (!(BASIC.equals(value) ||
                  SSL.equals(value) ||
                  CUSTOM.equals(value))) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ": " + value + " a not a valid value");
            }
        }
    }

    /**
     * Replication data channel factory configuration
     * @see ReplicationNetworkConfig#CHANNEL_TYPE
     */
    public static final ConfigParam CHANNEL_TYPE =
        new ChannelTypeConfigParam(
            ReplicationNetworkConfig.CHANNEL_TYPE,
            ChannelTypeConfigParam.BASIC,  // default
            false,                         // mutable
            true);                         // forReplication

    /**
     * Replication data channel logging identifier.
     * @see ReplicationNetworkConfig#CHANNEL_LOG_NAME
     */
    public static final ConfigParam CHANNEL_LOG_NAME =
        new ConfigParam(
            ReplicationNetworkConfig.CHANNEL_LOG_NAME,
            "",                            // default
            false,                         // mutable
            true);                         // forReplication

    /**
     * Data channel factory class
     * @see ReplicationNetworkConfig#CHANNEL_FACTORY_CLASS
     */
    public static final ConfigParam CHANNEL_FACTORY_CLASS =
        new ConfigParam(ReplicationNetworkConfig.CHANNEL_FACTORY_CLASS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * Data channel factory parameters
     * @see ReplicationNetworkConfig#CHANNEL_FACTORY_PARAMS
     */
    public static final ConfigParam CHANNEL_FACTORY_PARAMS =
        new ConfigParam(ReplicationNetworkConfig.CHANNEL_FACTORY_PARAMS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL KeyStore file
     * @see ReplicationSSLConfig#SSL_KEYSTORE_FILE
     */
    public static final ConfigParam SSL_KEYSTORE_FILE =
        new ConfigParam(ReplicationSSLConfig.SSL_KEYSTORE_FILE,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL KeyStore password
     * @see ReplicationSSLConfig#SSL_KEYSTORE_PASSWORD
     */
    public static final ConfigParam SSL_KEYSTORE_PASSWORD =
        new ConfigParam(ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL KeyStore password source class
     * @see ReplicationSSLConfig#SSL_KEYSTORE_PASSWORD_CLASS
     */
    public static final ConfigParam SSL_KEYSTORE_PASSWORD_CLASS =
        new ConfigParam(ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_CLASS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL KeyStore password source constructor parameters
     * @see ReplicationSSLConfig#SSL_KEYSTORE_PASSWORD_PARAMS
     */
    public static final ConfigParam SSL_KEYSTORE_PASSWORD_PARAMS =
        new ConfigParam(ReplicationSSLConfig.SSL_KEYSTORE_PASSWORD_PARAMS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL KeyStore type
     * @see ReplicationSSLConfig#SSL_KEYSTORE_TYPE
     */
    public static final ConfigParam SSL_KEYSTORE_TYPE =
        new ConfigParam(ReplicationSSLConfig.SSL_KEYSTORE_TYPE,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL server key alias
     * @see ReplicationSSLConfig#SSL_SERVER_KEY_ALIAS
     */
    public static final ConfigParam SSL_SERVER_KEY_ALIAS =
        new ConfigParam(ReplicationSSLConfig.SSL_SERVER_KEY_ALIAS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL client key alias
     * @see ReplicationSSLConfig#SSL_CLIENT_KEY_ALIAS
     */
    public static final ConfigParam SSL_CLIENT_KEY_ALIAS =
        new ConfigParam(ReplicationSSLConfig.SSL_CLIENT_KEY_ALIAS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL TrustStore file
     * @see ReplicationSSLConfig#SSL_TRUSTSTORE_FILE
     */
    public static final ConfigParam SSL_TRUSTSTORE_FILE =
        new ConfigParam(ReplicationSSLConfig.SSL_TRUSTSTORE_FILE,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL TrustStore type
     * @see ReplicationSSLConfig#SSL_TRUSTSTORE_TYPE
     */
    public static final ConfigParam SSL_TRUSTSTORE_TYPE =
        new ConfigParam(ReplicationSSLConfig.SSL_TRUSTSTORE_TYPE,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL cipher suites
     * @see ReplicationSSLConfig#SSL_CIPHER_SUITES
     */
    public static final ConfigParam SSL_CIPHER_SUITES =
        new ConfigParam(ReplicationSSLConfig.SSL_CIPHER_SUITES,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL protocols
     * @see ReplicationSSLConfig#SSL_PROTOCOLS
     */
    public static final ConfigParam SSL_PROTOCOLS =
        new ConfigParam(ReplicationSSLConfig.SSL_PROTOCOLS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL Authenticator
     * @see ReplicationSSLConfig#SSL_AUTHENTICATOR
     */
    public static final ConfigParam SSL_AUTHENTICATOR =
        new ConfigParam(ReplicationSSLConfig.SSL_AUTHENTICATOR,
                        "",                  // default
                        false,               // mutable
                        true) {              // forReplication

            @Override
            public void validateValue(String value) {
                if (value == null) {
                    throw new IllegalArgumentException
                        ("a value is required");
                }
                if (!SSLChannelFactory.isValidAuthenticator(value)) {
                    throw new IllegalArgumentException
                        (value + " a not a valid value");
                }
            }
        };

    /**
     * SSL Authenticator class
     * @see ReplicationSSLConfig#SSL_AUTHENTICATOR_CLASS
     */
    public static final ConfigParam SSL_AUTHENTICATOR_CLASS =
        new ConfigParam(ReplicationSSLConfig.SSL_AUTHENTICATOR_CLASS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL Authenticator parameters
     * @see ReplicationSSLConfig#SSL_AUTHENTICATOR_PARAMS
     */
    public static final ConfigParam SSL_AUTHENTICATOR_PARAMS =
        new ConfigParam(ReplicationSSLConfig.SSL_AUTHENTICATOR_PARAMS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL Host Verifier
     * @see ReplicationSSLConfig#SSL_HOST_VERIFIER
     */
    public static final ConfigParam SSL_HOST_VERIFIER =
        new ConfigParam(ReplicationSSLConfig.SSL_HOST_VERIFIER,
                        "",                  // default
                        false,               // mutable
                        true) {              // forReplication

            @Override
            public void validateValue(String value) {
                if (value == null) {
                    throw new IllegalArgumentException
                        ("a value is required");
                }
                if (!SSLChannelFactory.isValidHostVerifier(value)) {
                    throw new IllegalArgumentException
                        (value + " a not a valid value");
                }
            }
        };

    /**
     * SSL Host Verifier class
     * @see ReplicationSSLConfig#SSL_HOST_VERIFIER_CLASS
     */
    public static final ConfigParam SSL_HOST_VERIFIER_CLASS =
        new ConfigParam(ReplicationSSLConfig.SSL_HOST_VERIFIER_CLASS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * SSL Host Verifier parameters
     * @see ReplicationSSLConfig#SSL_HOST_VERIFIER_PARAMS
     */
    public static final ConfigParam SSL_HOST_VERIFIER_PARAMS =
        new ConfigParam(ReplicationSSLConfig.SSL_HOST_VERIFIER_PARAMS,
                        "",                  // default
                        false,               // mutable
                        true);               // forReplication

    /**
     * Override the current JE version, for testing only.
     */
    public static final ConfigParam TEST_JE_VERSION = new ConfigParam(
        EnvironmentParams.REP_PARAM_PREFIX + "test.jeVersion",
        "",                     // default
        false,                  // mutable
        true);                  // forReplication

    /**
     * @deprecated see {@link ReplicationConfig#REPLAY_COST_PERCENT}
     */
    public static final IntConfigParam REPLAY_COST_PERCENT =
        new IntConfigParam(ReplicationConfig.REPLAY_COST_PERCENT,
                           0,     // min
                           1000,  // max
                           150,   // default
                           false, // mutable
                           true); // forReplication

    /**
     * @see ReplicationConfig#REPLAY_FREE_DISK_PERCENT
     */
    public static final IntConfigParam REPLAY_FREE_DISK_PERCENT =
        new IntConfigParam(ReplicationConfig.REPLAY_FREE_DISK_PERCENT,
                           0,     // min
                           99,    // max
                           0,     // default
                           false, // mutable
                           true); // forReplication

    /**
     * The subscription queue poll interval.
     */
    public static final DurationConfigParam SUBSCRIPTION_POLL_INTERVAL =
        new DurationConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                "subscriptionPollInterval",
                                "10 ms",         // min
                                null,            // max
                                "1 s",           // default
                                false,           // not mutable
                                true);           // forReplication

    /**
     * The subscription queue poll timeout.
     */
    public static final DurationConfigParam SUBSCRIPTION_POLL_TIMEOUT =
        new DurationConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                "subscriptionPollTimeout",
                                "100 ms",         // min
                                null,             // max
                                "30 s",           // default
                                false,            // not mutable
                                true);            // forReplication

    /**
     * The maximum number of times to retry failed subscription connections.
     */
    public static final IntConfigParam SUBSCRIPTION_MAX_CONNECT_RETRIES =
        new IntConfigParam
            (EnvironmentParams.REP_PARAM_PREFIX +
             "subscriptionMaxConnectRetries",
             Integer.valueOf(1),                  // min
             null,                                // max
             Integer.valueOf(3),                  // default
             false,                               // not mutable
             true);                               // forReplication

    /**
     * The amount of time that the subscription thread should sleep time before
     * retrying a failed connection.
     */
    public static final DurationConfigParam SUBSCRIPTION_SLEEP_BEFORE_RETRY =
        new DurationConfigParam(EnvironmentParams.REP_PARAM_PREFIX +
                                "subscriptionSleepBeforeRetry",
                                "1 s",            // min
                                null,             // max
                                "3 s",            // default
                                false,            // not mutable
                                true);            // forReplication

}
