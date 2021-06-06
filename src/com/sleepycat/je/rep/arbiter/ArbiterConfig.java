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

package com.sleepycat.je.rep.arbiter;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;

import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.impl.RepParams;

/**
 * The configuration parameters for an {@link Arbiter}.
 *
 * @see Arbiter#Arbiter(ArbiterConfig)
 */
public class ArbiterConfig extends ArbiterMutableConfig implements Cloneable {

    private String arbiterHome;
    private ReplicationNetworkConfig repNetConfig;
    private Handler loggingHandler;

    /**
     * Arbiter configuration.
     */
    public ArbiterConfig() {
        super();
        repNetConfig = ReplicationNetworkConfig.createDefault();
    }

    /**
     * Arbiter configuration.
     * @param props to initialize configuration object.
     */
    public ArbiterConfig(Properties props) {
        super(props);
        repNetConfig = ReplicationNetworkConfig.createDefault();
    }

    /**
     * Gets the Arbiter home directory.
     *
     * @return Path of the Arbiter home directory.
     */
    public String getArbiterHome() {
        return arbiterHome;
    }

    /**
     * Sets the Arbiter Home directory
     *
     * @param arbiterHome Path of the Arbiter home directory.
     */
    public void setArbiterHome(String arbiterHome) {
        this.arbiterHome = arbiterHome;
    }

    /**
     * Sets the name to be associated with this <code>Arbiter</code>. It must
     * be unique within the group. When the <code>Arbiter</code> is
     * instantiated and joins the replication group, a check is done to ensure
     * that the name is unique, and a
     * {@link com.sleepycat.je.rep.RestartRequiredException} is thrown if it is
     * not.
     *
     * @param nodeName the name of this arbiter.
     */
    public ArbiterConfig setNodeName(String nodeName)
        throws IllegalArgumentException {
        DbConfigManager.setVal(
                props, RepParams.NODE_NAME, nodeName, validateParams);
        return this;
    }

    /**
     * Returns the unique name associated with this Arbiter.
     *
     * @return the Arbiter name
     */
    public String getNodeName() {
        return DbConfigManager.getVal(props, RepParams.NODE_NAME);
    }

    /**
     * Sets the name for the replication group. The name must be made up of
     * just alpha numeric characters and must not be zero length.
     *
     * @param groupName the alpha numeric string representing the name.
     *
     * @throws IllegalArgumentException if the string name is not valid.
     */
    public ArbiterConfig setGroupName(String groupName)
        throws IllegalArgumentException {

            DbConfigManager.setVal(
                props, RepParams.GROUP_NAME, groupName, validateParams);
            return this;
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
     * Sets the hostname and port associated with this arbiter. The hostname
     * and port combination are denoted by a string of the form:
     * <pre>
     *  hostname[:port]
     * </pre>
     * The port must be outside the range of "Well Known Ports"
     * (zero through 1023).
     *
     * @param hostPort the string containing the hostname and port as above.
     */
    public ArbiterConfig setNodeHostPort(String hostPort) {
        DbConfigManager.setVal(
            props, RepParams.NODE_HOST_PORT, hostPort, validateParams);
        return this;
    }

    /**
     * Returns the hostname and port associated with this node. The hostname
     * and port combination are denoted by a string of the form:
     * <pre>
     *  hostname:port
     * </pre>
     *
     * @return the hostname and port string of this Arbiter.
     */
    public String getNodeHostPort() {
        return DbConfigManager.getVal(props, RepParams.NODE_HOST_PORT);
    }

    /**
     * Time to wait for the discovery of the Master during the instantiation
     * of the Arbiter. If no Master is found with in the timeout period,
     * the Arbiter constructor return with the Arbiter in the UNKNOWN state.
     *
     * @param timeout The unknown state timeout. A value of 0 turns off
     * Unknown state timeouts. The creation of the Arbiter will wait until
     * a Master is found.
     *
     * @param unit the {@code TimeUnit} of the timeout value. May be null only
     * if timeout is zero.
     *
     * @return this
     *
     * @throws IllegalArgumentException If the value of timeout is negative
     *
     */
    public ArbiterConfig setUnknownStateTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {
        DbConfigManager.setDurationVal(
                props, RepParams.ENV_UNKNOWN_STATE_TIMEOUT,
                timeout, unit, validateParams);
        return this;
    }

    /**
     * Returns the Unknown state timeout.
     *
     * <p>A value of 0 means Unknown state timeouts are not configured.</p>
     *
     * @param unit the {@code TimeUnit} of the returned value. May not be null.
     *
     * @return The transaction timeout.
     */
    public long getUnknownStateTimeout(TimeUnit unit) {
        return DbConfigManager.getDurationVal(
            props, RepParams.ENV_UNKNOWN_STATE_TIMEOUT, unit);

    }

    /**
     * Sets the heartbeat interval.
     * @param millis Interval in milliseconds.
     * @return this
     */
    public ArbiterConfig setHeartbeatInterval(int millis) {
        DbConfigManager.setIntVal(
                props, RepParams.HEARTBEAT_INTERVAL, millis, validateParams);
        return this;
    }

    /**
     * Gets the heartbeat interval in milliseconds.
     * @return Heartbeat interval.
     */
    public int getHeartbeatInterval() {
        return DbConfigManager.getIntVal(props, RepParams.HEARTBEAT_INTERVAL);
    }

    /**
     * @hidden
     * The size of the message queue the Arbiter uses to read
     * messages and used to size the output message queue for
     * responses to the master.
     *
     * @param val size of the queue
     * @return this
     */
    public ArbiterConfig setMessageQueueSize(int val) {
        DbConfigManager.setIntVal(
            props,
            RepParams.REPLICA_MESSAGE_QUEUE_SIZE, val, validateParams);
        return this;
    }

    /**
     * @hidden
     * Internal parameter enable use of the group ack message.
     *
     * @param val Boolean value.
     * @return this
     */
    public ArbiterConfig setEnableGroupAcks(boolean val) {
        DbConfigManager.setBooleanVal(
            props, RepParams.ENABLE_GROUP_ACKS, val, validateParams);
        return this;
    }

    /**
     * @hidden
     * Get boolean controlling the use of group ack message.
     * @return boolean
     */
    public boolean getEnableGroupAcks() {
        return DbConfigManager.getBooleanVal(
            props, RepParams.ENABLE_GROUP_ACKS);
    }

    /**
     * @hidden
     * Gets the size of the message queue.
     * @return size of the message queue.
     */
    public int getMessageQueueSize() {
        return DbConfigManager.getIntVal(
            props, RepParams.REPLICA_MESSAGE_QUEUE_SIZE);
    }

    /**
     * @hidden
     * The interval used when checking an inactive connection to the
     * master.
     *
     * @param timeout Timeout value
     * @param unit time unit
     * @return this
     * @throws IllegalArgumentException
     */
    public ArbiterConfig setChannelTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {
        DbConfigManager.setDurationVal(
            props, RepParams.REPLICA_TIMEOUT,
            timeout, unit, validateParams);
        return this;
    }

    /**
     * @hidden
     * Gets the timeout value.
     * @param unit TimeUnit
     * @return timeout value.
     */
    public long getChannelTimeout(TimeUnit unit) {
        return DbConfigManager.getDurationVal(
            props, RepParams.REPLICA_TIMEOUT, unit);
    }

    /**
     * @hidden
     * The timeout used when waiting for the initial heartbeat
     * when establishing a connection.
     * @param timeout Maximum time to wait.
     * @param unit TimeUnit
     * @return this
     * @throws IllegalArgumentException
     */
    public ArbiterConfig setPreHeartbeatTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {
        DbConfigManager.setDurationVal(
            props, RepParams.PRE_HEARTBEAT_TIMEOUT,
            timeout, unit, validateParams);
        return this;
    }

    /**
     * @hidden
     * The pre heartbeat timeout value.
     * @param unit TimeUnit
     * @return timeout
     */
    public long getPreHeartbeatTimeout(TimeUnit unit) {
        return DbConfigManager.getDurationVal(
            props, RepParams.PRE_HEARTBEAT_TIMEOUT, unit);
    }

    /**
     * @hidden
     * The heartbeat timeout.
     *
     * @param timeout Timeout value
     * @param unit time unit
     * @return this
     * @throws IllegalArgumentException
     */
    public ArbiterConfig setFeederTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {
        DbConfigManager.setDurationVal(
            props, RepParams.FEEDER_TIMEOUT,
            timeout, unit, validateParams);
        return this;
    }

    /**
     * @hidden
     * Gets the timeout value.
     * @param unit TimeUnit
     * @return timeout value.
     */
    public long getFeederTimeout(TimeUnit unit) {
        return DbConfigManager.getDurationVal(
            props, RepParams.FEEDER_TIMEOUT, unit);
    }

    /**
     * @hidden
     * The size of the the TCP receive buffer associated with the socket used
     * by the Arbiter to  communicate to the master.
     * @param val size of the buffer
     * @return this
     */
    public ArbiterConfig setReceiveBufferSize(int val) {
        DbConfigManager.setIntVal(
            props,
            RepParams.REPLICA_RECEIVE_BUFFER_SIZE, val, validateParams);
        return this;
    }

    /**
     * @hidden
     * Returns the receive buffer size.
     * @return buffer size.
     */
    public int getReceiveBufferSize() {
        return DbConfigManager.getIntVal(
            props, RepParams.REPLICA_RECEIVE_BUFFER_SIZE);
    }

    /**
     * @hidden
     * The socket timeout value used by an Arbiter when it opens a new
     * connection to establish a stream with a feeder.
     * @param timeout maximum time to wait
     * @param unit TimeUnit
     * @return this
     * @throws IllegalArgumentException
     */
    public ArbiterConfig setStreamOpenTimeout(long timeout, TimeUnit unit)
        throws IllegalArgumentException {
        DbConfigManager.setDurationVal(
            props, RepParams.REPSTREAM_OPEN_TIMEOUT,
            timeout, unit, validateParams);
        return this;
    }

    /**
     * @hidden
     * Returns the socket timeout value.
     * @param unit TimeUnit
     * @return Timeout value.
     */
    public long getStreamOpenTimeout(TimeUnit unit) {
        return DbConfigManager.getDurationVal(
            props, RepParams.REPSTREAM_OPEN_TIMEOUT, unit);
    }

    /**
     * @hidden
     * Get the replication service net configuration associated with
     * this MonitorConfig.
     */
    public ReplicationNetworkConfig getRepNetConfig() {
        return repNetConfig;
    }

    /**
     * @hidden
     * Set the replication service net configuration associated with
     * this MonitorConfig.
     *
     * @param netConfig the new ReplicationNetworkConfig to be associated
     * with this MonitorConfig.  This must not be null.
     *
     * @throws IllegalArgumentException if the netConfig is null
     */
    public ArbiterConfig setRepNetConfig(
        ReplicationNetworkConfig netConfig) {

        setRepNetConfigVoid(netConfig);
        return this;
    }

    /**
     * Documentation inherited from ArbiterMutableConfig.setConfigParam.
     */
    @Override
    public ArbiterConfig setConfigParam(String paramName, String value)
        throws IllegalArgumentException {

        boolean forReplication = false;
        ConfigParam param =
            EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
        if (param != null) {
            forReplication = param.isForReplication();
        }
        DbConfigManager.setConfigParam(props,
                                       paramName,
                                       value,
                                       false, /* requireMutablity */
                                       validateParams,
                                       forReplication, /* forReplication */
                                       true   /* verifyForReplication */);
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

    ArbiterMutableConfig getArbiterMutableConfig() {
        return super.copy();
    }

    /**
     */
    public ArbiterConfig clone() {
        ArbiterConfig retval = (ArbiterConfig)super.clone();
        retval.repNetConfig = repNetConfig.clone();
        retval.arbiterHome = this.arbiterHome;
        return retval;
    }

    /**
     */
    public ArbiterConfig setLoggingHandler(Handler handler) {
        loggingHandler = handler;
        return this;
    }

    /**
     * Returns the custom java.util.logging.Handler specified by the
     * application.
     */
    public Handler getLoggingHandler() {
        return loggingHandler;
    }

    /**
     * Display configuration values.
     */
    @Override
    public String toString() {
        return ("arbiterHome=" + arbiterHome + "\n" +
                "repNetConfig=" + repNetConfig + "\n" +
                super.toString());
    }
}
