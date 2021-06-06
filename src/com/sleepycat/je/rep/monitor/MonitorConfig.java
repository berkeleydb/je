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

package com.sleepycat.je.rep.monitor;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.Set;

import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * Specifies the attributes used by a replication {@link Monitor}.
 * <p>
 * The following properties identify the target group.
 * <ul>
 * <li>groupName: the name of the replication group being monitored. </li>
 * <li>nodeName: the group-wide unique name associated with this
 * monitor node.</li>
 * <li>nodeHost: the hostname and port associated with this Monitor. Used
 * by group members to contact the Monitor.</li>
 * <li>helperHosts: the list of replication nodes which the Monitor uses to
 * register itself so it can receive notifications about group status
 * changes.</li>
 * </ul>
 * The following properties configure the daemon ping thread implemented
 * within the Monitor. This daemon thread lets the Monitor proactively find
 * status changes that occur when the Monitor is down or has lost network
 * connectivity.
 * <ul>
 * <li>numRetries: number of times the ping thread attempts to contact a
 * node before deeming is unreachable. </li>
 * <li>retryInterval: number of milliseconds between ping thread retries.
 * </li>
 * <li>timeout: socketConnection timeout, in milliseconds, specified
 * when the ping thread attempts to establish a connection with a replication
 * node. </li>
 * </ul>
 * @since JE 5.0
 */
public class MonitorConfig implements Cloneable {

    /**
     * An instance created using the default constructor is initialized with
     * the default settings.
     */
    public static final MonitorConfig DEFAULT = new MonitorConfig();

    /*
     * Since the MonitorConfig and ReplicationConfig have lots of common
     * properties, it uses lots of properties defined in RepParams.
     */
    private Properties props;
    private final boolean validateParams = true;

    /* These properties are mutable for a Monitor. */
    private int numRetries = 5;
    private long retryInterval = 1000;
    private int socketConnectTimeout = 10000;

    /* The replication net configuration */
    private ReplicationNetworkConfig repNetConfig;

    /**
     * An instance created using the default constructor is initialized with
     * the default settings.
     */
    public MonitorConfig() {
        props = new Properties();
        repNetConfig = ReplicationNetworkConfig.createDefault();
    }

    /* Internal use only, support the deprecated Monitor Constructor. */
    MonitorConfig(ReplicationConfig repConfig) {
        props = new Properties();
        repNetConfig = repConfig.getRepNetConfig().clone();
        setNodeName(repConfig.getNodeName());
        setGroupName(repConfig.getGroupName());
        setNodeHostPort(repConfig.getNodeHostPort());
        setHelperHosts(repConfig.getHelperHosts());

        if (!repConfig.getNodeType().isMonitor()) {
            throw new IllegalArgumentException
                ("The configured node type was: " + repConfig.getNodeType() +
                 " instead of: " + NodeType.MONITOR);
        }
    }

    /**
     * Sets the name for the replication group. The name must be made up of
     * just alpha numeric characters and must not be zero length.
     *
     * @param groupName the alpha numeric string representing the name.
     *
     * @throws IllegalArgumentException if the string name is not valid.
     */
    public MonitorConfig setGroupName(String groupName)
        throws IllegalArgumentException {

        setGroupNameVoid(groupName);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setGroupNameVoid(String groupName)
        throws IllegalArgumentException {

        DbConfigManager.setVal
            (props, RepParams.GROUP_NAME, groupName, validateParams);
    }

    /**
     * Gets the name associated with the replication group.
     *
     * @return the name of this replication group.
     */
    public String getGroupName() {
        return DbConfigManager.getVal(props, RepParams.GROUP_NAME);
    }

    /**
     * Sets the name to be associated with this <code>monitor</code>. It must
     * be unique within the group. When the <code>monitor</code> is
     * instantiated and joins the replication group, a check is done to ensure
     * that the name is unique, and a
     * {@link com.sleepycat.je.rep.RestartRequiredException} is thrown if it is
     * not.
     *
     * @param nodeName the name of this monitor.
     */
    public MonitorConfig setNodeName(String nodeName)
        throws IllegalArgumentException {

        setNodeNameVoid(nodeName);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setNodeNameVoid(String nodeName)
        throws IllegalArgumentException {

        DbConfigManager.setVal
            (props, RepParams.NODE_NAME, nodeName, validateParams);
    }

    /**
     * Returns the unique name associated with this monitor.
     *
     * @return the monitor name
     */
    public String getNodeName() {
        return DbConfigManager.getVal(props, RepParams.NODE_NAME);
    }

    /**
     * Sets the hostname and port associated with this monitor. The hostname
     * and port combination are denoted by a string of the form:
     * <pre>
     *  hostname[:port]
     * </pre>
     * The port must be outside the range of "Well Known Ports"
     * (zero through 1023).
     *
     * @param hostPort the string containing the hostname and port as above.
     */
    public MonitorConfig setNodeHostPort(String hostPort) {
        setNodeHostPortVoid(hostPort);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setNodeHostPortVoid(String hostPort) {
        DbConfigManager.setVal
            (props, RepParams.NODE_HOST_PORT, hostPort, validateParams);
    }

    /**
     * Returns the hostname and port associated with this node. The hostname
     * and port combination are denoted by a string of the form:
     * <pre>
     *  hostname:port
     * </pre>
     *
     * @return the hostname and port string of this monitor.
     */
    public String getNodeHostPort() {
        return DbConfigManager.getVal(props, RepParams.NODE_HOST_PORT);
    }

    /**
     * Identify one or more helpers nodes by their host and port pairs in this
     * format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     *
     * @param helperHosts the string representing the host and port pairs.
     */
    public MonitorConfig setHelperHosts(String helperHosts) {
        setHelperHostsVoid(helperHosts);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setHelperHostsVoid(String helperHosts) {
        DbConfigManager.setVal
            (props, RepParams.HELPER_HOSTS, helperHosts, validateParams);
    }

    /**
     * Returns the string identifying one or more helper host and port pairs in
     * this format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     *
     * @return the string representing the host port pairs.
     */
    public String getHelperHosts() {
        return DbConfigManager.getVal(props, RepParams.HELPER_HOSTS);
    }

    /**
     * @hidden
     *
     * For internal use only: Internal convenience method.
     *
     * Returns the set of sockets associated with helper nodes.
     *
     * @return the set of helper sockets, returns an empty set if there are no
     * helpers.
     */
    public Set<InetSocketAddress> getHelperSockets() {
        return HostPortPair.getSockets(getHelperHosts());
    }

    /**
     * Returns the hostname component of the nodeHost property.
     *
     * @return the hostname string
     */
    public String getNodeHostname() {
        String hostAndPort = getNodeHostPort();
        int colonToken = hostAndPort.indexOf(":");

        return (colonToken >= 0) ?
               hostAndPort.substring(0, colonToken) : hostAndPort;
    }

    /**
     * Returns the port component of the nodeHost property.
     *
     * @return the port number
     */
    public int getNodePort() {
        String hostAndPort = getNodeHostPort();
        int colonToken = hostAndPort.indexOf(":");

        String portString = (colonToken >= 0) ?
            hostAndPort.substring(colonToken + 1) :
            DbConfigManager.getVal(props, RepParams.DEFAULT_PORT);

        return Integer.parseInt(portString);
    }

    /**
     * @hidden
     * Internal use only.
     *
     * This method should only be used when the configuration object is known
     * to have an authoritative value for its socket value.
     *
     * @return the InetSocketAddress used by this monitor
     */
    public InetSocketAddress getNodeSocketAddress() {
        return new InetSocketAddress(getNodeHostname(), getNodePort());
    }

    /**
     * Sets the number of times a ping thread attempts to contact a node
     * before deeming it unreachable.
     * The default value is 5.
     */
    public MonitorConfig setNumRetries(final int numRetries) {
        setNumRetriesVoid(numRetries);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setNumRetriesVoid(final int numRetries) {
        validate(numRetries, "numRetries");
        this.numRetries = numRetries;
    }

    /**
     * Returns the number of times a ping thread attempts to contact a node
     * before deeming it unreachable.
     */
    public int getNumRetries() {
        return numRetries;
    }

    /**
     * Sets the number of milliseconds between ping thread retries. The default
     * value is 1000.
     */
    public MonitorConfig setRetryInterval(final long retryInterval) {
        setRetryIntervalVoid(retryInterval);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setRetryIntervalVoid(final long retryInterval) {
        validate(retryInterval, "retryInterval");
        this.retryInterval = retryInterval;
    }

    /**
     * Returns the number of milliseconds between ping thread retries.
     */
    public long getRetryInterval() {
        return retryInterval;
    }

    /**
     * Sets the socketConnection timeout, in milliseconds, used
     * when the ping thread attempts to establish a connection with a
     * replication node. The default value is 10,000.
     */
    public MonitorConfig setSocketConnectTimeout(final int socketConnectTimeout) {
        setSocketConnectTimeoutVoid(socketConnectTimeout);
        return this;
    }

    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setSocketConnectTimeoutVoid(final int socketConnectTimeout) {
        validate(socketConnectTimeout, "socketConnectTimeout");
        this.socketConnectTimeout = socketConnectTimeout;
    }

    /**
     * Returns the socketConnection timeout, in milliseconds, used
     * when the ping thread attempts to establish a connection with a
     * replication node.
     */
    public int getSocketConnectTimeout() {
        return socketConnectTimeout;
    }

    private void validate(Number number, String param) {
        if (number.longValue() <= 0) {
            throw new IllegalArgumentException
                ("Parameter: " + param + " should be a positive number.");
        }
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public MonitorConfig clone() {
        try {
            MonitorConfig copy = (MonitorConfig) super.clone();

            copy.props = (Properties) props.clone();
            copy.repNetConfig = repNetConfig.clone();
            return copy;
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * @hidden SSL deferred
     * Get the replication service net configuration associated with
     * this MonitorConfig.
     */
    public ReplicationNetworkConfig getRepNetConfig() {
        return repNetConfig;
    }

    /**
     * @hidden SSL deferred
     * Set the replication service net configuration associated with
     * this MonitorConfig.
     *
     * @param netConfig the new ReplicationNetworkConfig to be associated
     * with this MonitorConfig.  This must not be null.
     *
     * @throws IllegalArgumentException if the netConfig is null
     */
    public MonitorConfig setRepNetConfig(
        ReplicationNetworkConfig netConfig) {

        setRepNetConfigVoid(netConfig);
        return this;
    }
    /**
     * @hidden
     * For bean editors
     */
    public void setRepNetConfigVoid(ReplicationNetworkConfig netConfig) {
        if (netConfig == null) {
            throw new IllegalArgumentException("netConfig may not be null");
        }
        repNetConfig = netConfig;
    }
}
