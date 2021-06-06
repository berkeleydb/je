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

import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepGroupProtocol.DeleteMember;
import com.sleepycat.je.rep.impl.RepGroupProtocol.EnsureNode;
import com.sleepycat.je.rep.impl.RepGroupProtocol.FailReason;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupRequest;
import com.sleepycat.je.rep.impl.RepGroupProtocol.RemoveMember;
import com.sleepycat.je.rep.impl.RepGroupProtocol.TransferMaster;
import com.sleepycat.je.rep.impl.RepGroupProtocol.UpdateAddress;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingService;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingRunnable;
import com.sleepycat.je.utilint.LoggerUtils;

public class GroupService extends ExecutingService {

    /* The replication node */
    final RepNode repNode;
    final RepGroupProtocol protocol;

    /**
     * List of channels for in-flight requests.
     * The channel is in this collection while the request is being processed,
     * and must be removed before sending any response.
     *
     * @see #cancel
     * @see #unregisterChannel
     */
    private final Collection<DataChannel> activeChannels =
        new ArrayList<DataChannel>();

    private final Logger logger;

    /* Identifies the Group Service. */
    public static final String SERVICE_NAME = "Group";

    public GroupService(ServiceDispatcher dispatcher, RepNode repNode) {
        super(SERVICE_NAME, dispatcher);
        this.repNode = repNode;

        final DbConfigManager configManager =
            repNode.getRepImpl().getConfigManager();
        String groupName = configManager.get(GROUP_NAME);
        protocol =
            new RepGroupProtocol(groupName,
                                 repNode.getNameIdPair(),
                                 repNode.getRepImpl(),
                                 repNode.getRepImpl().getChannelFactory());
        logger = LoggerUtils.getLogger(getClass());
    }

    @Override
    protected void cancel() {
        Collection<DataChannel> channels;
        synchronized (this) {
            channels = new ArrayList<DataChannel>(activeChannels);
            activeChannels.clear();
        }
        if (!channels.isEmpty()) {
            LoggerUtils.warning
                (logger, repNode.getRepImpl(),
                 "In-flight GroupService request(s) canceled: node shutdown");
        }
        for (DataChannel channel : channels) {
            try {
                PrintWriter out =
                    new PrintWriter(Channels.newOutputStream(channel), true);
                ResponseMessage rm =
                    protocol.new Fail(FailReason.DEFAULT, "shutting down");
                out.println(rm.wireFormat());
            } finally {
                if (channel.isOpen()) {
                    try {
                        channel.close();
                    }
                    catch (IOException e) {
                        LoggerUtils.warning
                            (logger, repNode.getRepImpl(),
                             "IO error on channel close: " + e.getMessage());
                    }
                }
            }
        }
    }

    /* Dynamically invoked process methods */

    /**
     * Wraps the replication group as currently cached on this node in
     * a Response message and returns it.
     */
    @SuppressWarnings("unused")
    public ResponseMessage process(GroupRequest groupRequest) {
        RepGroupImpl group = repNode.getGroup();
        if (group == null) {
            return protocol.new Fail(groupRequest, FailReason.DEFAULT,
                                     "no group info yet");
        }
        return protocol.new GroupResponse(groupRequest, group);
    }

    /**
     * Ensures that the Monitor node, as described in the request, is a member
     * of the group.
     *
     * @param ensureNode the request message describing the monitor node
     *
     * @return EnsureOK message if the monitor node is already part of the rep
     * group, or was just made a part of the replication group. It returns a
     * Fail message if it could not be made part of the group. The message
     * associated with the response provides further details.
     */
    public ResponseMessage process(EnsureNode ensureNode) {
        RepNodeImpl node = ensureNode.getNode();
        try {
            ensureMaster();
            repNode.getRepGroupDB().ensureMember(node);
            RepNodeImpl enode =
                repNode.getGroup().getMember(node.getName());
            return protocol.new EnsureOK(ensureNode, enode.getNameIdPair());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(ensureNode, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(ensureNode, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Removes a current member from the group.
     *
     * @param removeMember the request identifying the member to be removed.
     *
     * @return OK message if the member was removed from the group.
     */
    public ResponseMessage process(RemoveMember removeMember) {
        final String nodeName = removeMember.getNodeName();
        try {
            ensureMaster();
            repNode.removeMember(nodeName);
            return protocol.new OK(removeMember);
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(removeMember, FailReason.MEMBER_NOT_FOUND,
                                     e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(removeMember, FailReason.IS_MASTER,
                                     e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(removeMember, FailReason.IS_REPLICA,
                                     e.getMessage());
        }  catch (DatabaseException e) {
            return protocol.new Fail(removeMember, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Deletes a current member from the group, which marks the node as removed
     * and deletes its entry from the rep group DB.
     *
     * @param deleteMember the request identifying the member to be deleted
     *
     * @return OK message if the member was deleted from the group
     */
    public ResponseMessage process(DeleteMember deleteMember) {
        final String nodeName = deleteMember.getNodeName();
        try {
            ensureMaster();
            repNode.removeMember(nodeName, true);
            return protocol.new OK(deleteMember);
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(deleteMember, FailReason.MEMBER_NOT_FOUND,
                                     e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(deleteMember, FailReason.IS_MASTER,
                                     e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(deleteMember, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (MemberActiveException e) {
            return protocol.new Fail(deleteMember, FailReason.MEMBER_ACTIVE,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(deleteMember, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Update the network address for a dead replica.
     *
     * @param updateAddress the request identifying the new network address for
     * the node.
     *
     * @return OK message if the address is successfully updated.
     */
    public ResponseMessage process(UpdateAddress updateAddress) {
        try {
            ensureMaster();
            repNode.updateAddress(updateAddress.getNodeName(),
                                  updateAddress.getNewHostName(),
                                  updateAddress.getNewPort());
            return protocol.new OK(updateAddress);
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(
                updateAddress, FailReason.MEMBER_NOT_FOUND, e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(updateAddress, FailReason.IS_MASTER,
                                     e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(updateAddress, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(updateAddress, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Transfer the master role from the current master to one of the specified
     * replicas.
     *
     * @param transferMaster the request identifying nodes to be considered for
     * the role of new master
     * @return null
     */
    public ResponseMessage process(TransferMaster transferMaster) {
        try {
            ensureMaster();
            final String nodeList = transferMaster.getNodeNameList();
            final Set<String> replicas = parseNodeList(nodeList);
            final long timeout = transferMaster.getTimeout();
            final boolean force = transferMaster.getForceFlag();
            String winner = repNode.transferMaster(replicas, timeout, force);
            return protocol.new TransferOK(transferMaster, winner);
        } catch (ReplicaStateException e) {
            return protocol.new Fail(transferMaster, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (MasterTransferFailureException e) {
            return protocol.new Fail(transferMaster, FailReason.TRANSFER_FAIL,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(transferMaster, FailReason.DEFAULT,
                                     e.getMessage());
        } catch (IllegalArgumentException e) {
            return protocol.new Fail(transferMaster, FailReason.DEFAULT,
                                     e.toString());
        } catch (IllegalStateException e) {
            return protocol.new Fail(transferMaster, FailReason.DEFAULT,
                                     e.toString());
        }
    }

    private Set<String> parseNodeList(String list) {
        Set<String> set = new HashSet<String>();
        StringTokenizer st = new StringTokenizer(list, ",");
        while (st.hasMoreTokens()) {
            set.add(st.nextToken());
        }
        return set;
    }

    private void ensureMaster() throws ReplicaStateException {
        if (!repNode.isMaster()) {
            throw new ReplicaStateException
                ("GroupService operation can only be performed at master");
        }
    }

    synchronized private void registerChannel(DataChannel dc) {
        activeChannels.add(dc);
    }

    /**
     * Removes the given {@code DataChannel} from our list of active channels.
     * <p>
     * Before sending any response on the channel, this method must be invoked
     * to claim ownership of it.
     * This avoids a potential race between the request processing thread in
     * the normal case, and a thread calling {@code cancel()} at env shutdown
     * time.
     *
     * @return true, if the channel is still active (usual case); false
     * otherwise, presumably because the service was shut down.
     */
    synchronized private boolean unregisterChannel(DataChannel dc) {
        return activeChannels.remove(dc);
    }

    @Override
    public Runnable getRunnable(DataChannel dataChannel) {
        return new GroupServiceRunnable(dataChannel, protocol);
    }

    class GroupServiceRunnable extends ExecutingRunnable {
        GroupServiceRunnable(DataChannel dataChannel,
                             RepGroupProtocol protocol) {
            super(dataChannel, protocol, true);
            registerChannel(dataChannel);
        }

        @Override
        protected ResponseMessage getResponse(RequestMessage request)
            throws IOException {

            ResponseMessage rm = protocol.process(GroupService.this, request);

            /*
             * If the channel has already been closed, before we got a chance to
             * produce the response, then just discard the tardy response and
             * return null.
             */
            return unregisterChannel(channel) ? rm : null;
        }

        @Override
        protected void logMessage(String message) {
            LoggerUtils.warning(logger, repNode.getRepImpl(), message);
        }
    }
}
