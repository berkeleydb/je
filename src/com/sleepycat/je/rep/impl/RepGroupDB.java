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

import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;
import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;
import static com.sleepycat.je.rep.impl.RepParams.NODE_NAME;
import static com.sleepycat.je.rep.impl.RepParams.RESET_REP_GROUP_RETAIN_UUID;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.impl.node.cbvlsn.CleanerBarrierState;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.monitor.GroupChangeEvent.GroupChangeType;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.txn.ReadonlyTxn;
import com.sleepycat.je.rep.util.DbResetRepGroup;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.VLSN;

/**
 * This class is used to encapsulate all access to the rep group data that is
 * present in every replicated JE environment. The rep group data exists
 * primarily to support dynamic group membership. Both read and update access
 * must be done through the APIs provided by this class.
 *
 * The database is simply a representation of the RepGroup. Each entry in the
 * database represents a node in RepGroup; the key is the String node name, and
 * the data is the serialized ReplicationNode.  There is a special entry keyed
 * by GROUP_KEY that holds the contents of the RepGroup (excluding the nodes)
 * itself.
 *
 * The database may be modified concurrently by multiple transactions as a
 * master processes requests to update it. It may also be accessed by multiple
 * overlapping transactions as a Replica replays the rep stream. These updates
 * need to be interleaved with operations like getGroup() that create copies of
 * the RepGroup instance. To avoid deadlocks, entries in the database are
 * accessed in order of ascending key. GROUP_KEY in particular is associated
 * with the lowest key value so that it's locked first implicitly as part of
 * any iteration and any other modifications to the database must first lock it
 * before making changes to the group itself.
 *
 * An instance of this class is created as part of a replication node and is
 * retained for the entire lifetime of that node.
 */
public class RepGroupDB {

    private final RepImpl repImpl;

    /* A convenient, cached empty group. */
    public final RepGroupImpl emptyGroup;

    private final Logger logger;

    /* The key used to store group-wide information in the database. It must
     * be the lowest key in the database, so that it's locked first during
     * database iteration.
     */
    public final static String GROUP_KEY = "$$GROUP_KEY$$";
    public final static DatabaseEntry groupKeyEntry = new DatabaseEntry();

    /* Initialize the entry. */
    static {
        StringBinding.stringToEntry(GROUP_KEY, groupKeyEntry);
    }

    private final static HashMap<String, Object> lockMap =
        new HashMap<String, Object>();

    /* The fixed DB ID associated with the internal rep group database. */
    public final static long DB_ID = DbTree.NEG_DB_ID_START - 1;

    /*
     * Number of times to retry for ACKs on the master before returning to
     * to the Replica, which will then again retry on some periodic basis.
     */
    private final static int QUORUM_ACK_RETRIES = 5;

    /* Convenience Durability and Config constants. */
    private final static Durability QUORUM_ACK_DURABILITY =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    private final static TransactionConfig QUORUM_ACK =
        new TransactionConfig();

    private final static TransactionConfig NO_ACK = new TransactionConfig();

    /*
     * TODO: Change this when we support true read only transactions.
     */
    final static TransactionConfig READ_ONLY = NO_ACK;

    private final static Durability NO_ACK_DURABILITY =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.SYNC,
                       ReplicaAckPolicy.NONE);

    private final static Durability NO_ACK_NO_SYNC_DURABILITY =
        new Durability(SyncPolicy.NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.NONE);


    static {
        /* Initialize config constants. */
        QUORUM_ACK.setDurability(QUORUM_ACK_DURABILITY);
        NO_ACK.setDurability(NO_ACK_DURABILITY);
    }

    /**
     * Create an instance. Note that the database handle is not initialized at
     * this time, since the state of the node master/replica is not known
     * at the time the replication node (and consequently this instance) is
     * created.
     * @throws DatabaseException
     */
    public RepGroupDB(RepImpl repImpl)
        throws DatabaseException {

        this.repImpl = repImpl;

        DbConfigManager configManager = repImpl.getConfigManager();
        emptyGroup = new RepGroupImpl(configManager.get(GROUP_NAME),
                                      repImpl.getCurrentJEVersion());
        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Returns all the members that are currently part of the replication
     * group, using NO_CONSISTENCY. This method can read the database directly,
     * and can be used when the replicated environment is detached and the
     * RepNode is null. It's for the latter reason that the method reads
     * uncommitted data. In detached mode, there may be transactions on the
     * database that were in progress when the node was last shutdown. These
     * transactions may have locks which will not be released until after the
     * node is re-attached and the replication stream is resumed. Using
     * uncommitted reads avoids use of locks in this circumstance. It's safe to
     * read these records, since the database will eventually be updated with
     * these changes.
     *
     * @return the group object
     * @throws DatabaseException if the object could not be obtained
     */
    public static RepGroupImpl getGroup(RepImpl rImpl,
                                        String groupName)
        throws DatabaseException {

        /* Get persistent nodes from the database */
        DatabaseImpl dbImpl = null;
        boolean foundDbImpl = false;
        try {
            dbImpl = rImpl.getGroupDb();
            foundDbImpl = true;
        } catch (DatabaseNotFoundException e) {
        }
        final RepGroupImpl group;
        if (!foundDbImpl) {

            /* Creates a temporary placeholder group for use until the real
             * definition comes over the replication stream as part of the
             * replicated group database.
             */
            group = new RepGroupImpl(groupName, true,
                                     rImpl.getCurrentJEVersion());

        } else {
            final TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setDurability(READ_ONLY.getDurability());
            txnConfig.setConsistencyPolicy(NO_CONSISTENCY);
            txnConfig.setReadUncommitted(true);

            Txn txn = null;
            try {
                txn = new ReadonlyTxn(rImpl, txnConfig);
                group = fetchGroup(groupName, dbImpl, txn);

                /*
                 * Correct summary info since we are reading uncommitted data
                 */
                group.makeConsistent();
                txn.commit();
                txn = null;
            } finally {
                if (txn != null) {
                    txn.abort();
                }
            }
        }

        /* Get nodes w/ transient id from their feeders */
        final RepNode repNode = rImpl.getRepNode();
        if (repNode != null) {
            for (final Feeder feeder :
                     repNode.feederManager().activeReplicasMap().values()) {
                final RepNodeImpl node = feeder.getReplicaNode();
                /* RepNodeImpl may be null in a test with a dummy feeder. */
                if (node != null && node.getType().hasTransientId()) {
                    group.addTransientIdNode(node);
                }
            }
        }

        return group;
    }

    public RepGroupImpl getGroup()
        throws DatabaseException {

        return getGroup(repImpl,
                        repImpl.getConfigManager().get(GROUP_NAME));
    }

    /**
     * Sets the minimum JE version required for nodes to join the replication
     * group and refreshes the group object cached in the rep group.  Throws a
     * {@link MinJEVersionUnsupportedException} if the requested version is not
     * supported by current nodes.
     *
     * <p>If this method returns successfully, nodes that are running a JE
     * version older than the one specified will not be permitted to join the
     * replication group in the future.  Use this method to implement features
     * that require all group members to meet a minimum version requirement.
     *
     * <p>The update attempts to obtain acknowledgments from a simple majority,
     * to make sure that future masters agree that the update has taken place,
     * but does not require this.
     *
     * @param newMinJEVersion the new minimum JE version
     * @throws DatabaseException if an error occurs when accessing the
     * replication group database
     * @throws MinJEVersionUnsupportedException if the requested version is not
     * supported
     */
    public void setMinJEVersion(final JEVersion newMinJEVersion)
        throws DatabaseException, MinJEVersionUnsupportedException {

        final DatabaseImpl groupDbImpl;
        try {
            groupDbImpl = repImpl.getGroupDb();
        } catch (DatabaseNotFoundException e) {
            /* Should never happen. */
            throw EnvironmentFailureException.unexpectedException(e);
        }
        MasterTxn txn =
            new MasterTxn(repImpl, QUORUM_ACK, repImpl.getNameIdPair());
        try {
            RepGroupImpl repGroup =
                fetchGroupObject(txn, groupDbImpl, LockMode.RMW);
            repGroup = fetchGroup(repGroup.getName(), groupDbImpl, txn);
            repGroup.setMinJEVersion(newMinJEVersion);
            saveGroupObject(txn, repGroup, groupDbImpl);
            txn.commit(QUORUM_ACK_DURABILITY);
            txn = null;
            LoggerUtils.info(logger, repImpl,
                "Updated minimum JE group version to " + newMinJEVersion);
        } catch (InsufficientAcksException e) {

            /*
             * Didn't receive acknowledgments from a simple majority.  OK to
             * proceed, since this operation will be repeated if the change is
             * lost.
             */
            LoggerUtils.info(logger, repImpl,
                "Proceeding without enough acks, did not update minimum JE " +
                    "group version to " + newMinJEVersion);
        } finally {
            if (txn != null) {
                txn.abort();
            }
        }
        repImpl.getRepNode().refreshCachedGroup();
    }

    /**
     * All rep group db access uses cursors with eviction disabled.
     */
    static private Cursor makeCursor(DatabaseImpl dbImpl,
                                     Txn txn,
                                     CursorConfig cursorConfig) {
        Cursor cursor = DbInternal.makeCursor(dbImpl,
                                              txn,
                                              cursorConfig);
        DbInternal.getCursorImpl(cursor).setAllowEviction(false);
        return cursor;
    }

    /**
     * Returns a representation of the nodes of the group stored in the
     * database, using the txn and handles that were passed in.
     */
    private static RepGroupImpl fetchGroup(String groupName,
                                           DatabaseImpl dbImpl,
                                           Txn txn)
        throws DatabaseException {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();
        NodeBinding nodeBinding = null;
        final GroupBinding groupBinding = new GroupBinding();

        RepGroupImpl group = null;
        Map <Integer, RepNodeImpl> nodes =
            new HashMap<Integer, RepNodeImpl>();
        final CursorConfig cursorConfig = new CursorConfig();
        cursorConfig.setReadCommitted(true);

        Cursor mcursor = null;

        try {
            mcursor = makeCursor(dbImpl, txn, cursorConfig);
            while (mcursor.getNext(keyEntry, value, LockMode.DEFAULT) ==
                   OperationStatus.SUCCESS) {

                final String key = StringBinding.entryToString(keyEntry);

                if (GROUP_KEY.equals(key)) {
                    group = groupBinding.entryToObject(value);
                    if (!group.getName().equals(groupName)) {
                        throw EnvironmentFailureException.unexpectedState
                            ("The argument: " + groupName +
                             " does not match the expected group name: " +
                             group.getName());
                    }

                    /*
                     * The group entry should always be first, so we can use it
                     * to provide the group version for reading node entries.
                     */
                    nodeBinding = new NodeBinding(group.getFormatVersion());
                } else {
                    if (nodeBinding == null) {
                        throw new IllegalStateException(
                            "Found node binding before group binding");
                    }
                    final RepNodeImpl node = nodeBinding.entryToObject(value);
                    nodes.put(node.getNameIdPair().getId(), node);
                }
            }
            if (group == null) {
                throw EnvironmentFailureException.unexpectedState
                    ("Group key: " + GROUP_KEY + " is missing");
            }
            group.setNodes(nodes);
            return group;
        } finally {
            if (mcursor != null) {
                mcursor.close();
            }
        }
    }

    /**
     * Ensures that information about this node, the current master, is in the
     * member database. If it isn't, enter it into the database. If the
     * database does not exist, create it as well.
     *
     * <p>Note that this overloading is only used by a node that is the master.
     *
     * @throws DatabaseException
     */
    public void addFirstNode()
        throws DatabaseException {

        DbConfigManager configManager = repImpl.getConfigManager();
        String groupName = configManager.get(GROUP_NAME);
        String nodeName = configManager.get(NODE_NAME);

        DatabaseImpl groupDbImpl = repImpl.createGroupDb();

        /* setup the group information as data. */
        RepGroupImpl repGroup =
            new RepGroupImpl(groupName, repImpl.getCurrentJEVersion());
        GroupBinding groupBinding =
            new GroupBinding(repGroup.getFormatVersion());
        DatabaseEntry groupEntry = new DatabaseEntry();
        groupBinding.objectToEntry(repGroup, groupEntry);

        /* Create the common group entry. */
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setDurability(NO_ACK.getDurability());
        txnConfig.setConsistencyPolicy(NO_CONSISTENCY);
        Txn txn = null;
        Cursor cursor = null;
        try {
            txn = new MasterTxn(repImpl,
                                txnConfig,
                                repImpl.getNameIdPair());

            cursor = makeCursor(groupDbImpl, txn, CursorConfig.DEFAULT);
            OperationStatus status = cursor.put(groupKeyEntry, groupEntry);
            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    ("Couldn't write first group entry " + status);
            }
            cursor.close();
            cursor = null;
            txn.commit();
            txn = null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (txn != null) {
                txn.abort();
            }
        }

        ensureMember(new RepNodeImpl(nodeName,
                                     repImpl.getHostName(),
                                     repImpl.getPort(),
                                     repImpl.getCurrentJEVersion()));
    }

    /**
     * Ensures that the membership info for the replica is in the database. A
     * call to this method is initiated by the master as part of the
     * feeder/replica handshake, where the replica provides membership
     * information as part of the handshake protocol. The membership database
     * must already exist, with the master in it, when this method is invoked.
     *
     * <p>This method should not be called for nodes with transient IDs.
     *
     * @param membershipInfo provided by the replica
     *
     * @throws InsufficientReplicasException upon failure of 2p member update
     * @throws InsufficientAcksException upon failure of 2p member update
     * @throws DatabaseException when the membership info could not be entered
     * into the membership database.
     */
    public void ensureMember(Protocol.NodeGroupInfo membershipInfo)
        throws DatabaseException {

        ensureMember(new RepNodeImpl(membershipInfo));
    }

    void ensureMember(RepNodeImpl ensureNode)
        throws DatabaseException {

        if (ensureNode.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Attempt to call ensureMember on " + ensureNode.getType() +
                " node: " + ensureNode);
        }
        DatabaseImpl groupDbImpl;
        try {
            groupDbImpl = repImpl.getGroupDb();
        } catch (DatabaseNotFoundException e) {
            /* Should never happen. */
            throw EnvironmentFailureException.unexpectedException(e);
        }

        DatabaseEntry nodeNameKey = new DatabaseEntry();
        StringBinding.stringToEntry(ensureNode.getName(), nodeNameKey);

        DatabaseEntry value = new DatabaseEntry();
        NodeBinding mib = null;

        Txn txn = null;
        Cursor cursor = null;
        try {
            txn = new ReadonlyTxn(repImpl, NO_ACK);

            /*
             * Fetch the group so we know the group format version.  Read the
             * group before reading the node entry in each case to avoid the
             * potential of deadlocks caused by reversing the order of lock
             * acquisition.
             */
            final RepGroupImpl repGroup =
                fetchGroupObject(txn, groupDbImpl, LockMode.DEFAULT);
            mib = new NodeBinding(repGroup.getFormatVersion());

            CursorConfig config = new CursorConfig();
            config.setReadCommitted(true);
            cursor = makeCursor(groupDbImpl, txn, config);

            OperationStatus status =
                cursor.getSearchKey(nodeNameKey, value, null);
            if (status == OperationStatus.SUCCESS) {
                /* Let's see if the entry needs updating. */
                RepNodeImpl miInDb = mib.entryToObject(value);
                if (miInDb.equivalent(ensureNode)) {
                    if (miInDb.isQuorumAck()) {
                        /* Present, matched and acknowledged. */
                        return;
                    }
                    ensureNode.getNameIdPair().update(miInDb.getNameIdPair());
                    /* Not acknowledged, retry the update. */
                } else {
                    /* Present but not equivalent. */
                    LoggerUtils.warning(logger, repImpl,
                                        "Incompatible node descriptions. " +
                                        "Membership database definition: " +
                                        miInDb.toString() +
                                        " Transient definition: " +
                                        ensureNode.toString());
                    if (ensureNode.getType() != miInDb.getType()) {
                        throw EnvironmentFailureException.unexpectedState(
                            "Conflicting node types for node " +
                            ensureNode.getName() +
                            ": expected " + ensureNode.getType() +
                            ", found " + miInDb.getType());
                    }
                    throw EnvironmentFailureException.unexpectedState(
                        "Incompatible node descriptions for node: " +
                        ensureNode.getName() + ", node ID: " +
                        ensureNode.getNodeId());
                }
                LoggerUtils.info(logger, repImpl,
                                 "Present but not ack'd node: " +
                                 ensureNode.getNodeId() +
                                 " ack status: " + miInDb.isQuorumAck());
            }
            cursor.close();
            cursor = null;
            txn.commit();
            txn = null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (txn != null) {
                txn.abort();
            }

        }
        createMember(ensureNode);

        /* Refresh group and Fire an ADD GroupChangeEvent. */
        refreshGroupAndNotifyGroupChange
            (ensureNode.getName(), GroupChangeType.ADD);
    }

    private void refreshGroupAndNotifyGroupChange(String nodeName,
                                                  GroupChangeType opType) {
        repImpl.getRepNode().refreshCachedGroup();
        repImpl.getRepNode().getMonitorEventManager().notifyGroupChange
            (nodeName, opType);
    }

    /**
     * Removes a node from the replication group by marking the node's entry in
     * the rep group db as removed, and optionally deleting the entry.
     *
     * <p>This method should not be called for nodes with transient IDs.
     */
    public void removeMember(final RepNodeImpl removeNode,
                             final boolean delete) {
        LoggerUtils.info(logger, repImpl,
                         (delete ? "Deleting node: " : "Removing node: ") +
                         removeNode.getName());

        if (removeNode.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Attempt to call removeMember on a node with type " +
                removeNode.getType() + ": " + removeNode);
        }

        TwoPhaseUpdate twoPhaseUpdate = new TwoPhaseUpdate(removeNode, true) {

            @Override
            void phase1Body() {
                final RepGroupImpl repGroup =
                    fetchGroupObject(txn, groupDbImpl, LockMode.RMW);
                int changeVersion = repGroup.incrementChangeVersion();
                saveGroupObject(txn, repGroup, groupDbImpl);
                node.setChangeVersion(changeVersion);
                node.setRemoved(true);
                saveNodeObject(txn, node, groupDbImpl, repGroup);
            }
            /** Override phase 2 to delete the node entry if delete is true. */
            @Override
            void phase2Body() {
                if (!delete) {
                    super.phase2Body();
                    return;
                }
                final DatabaseEntry nodeNameKey = new DatabaseEntry();
                StringBinding.stringToEntry(removeNode.getName(), nodeNameKey);
                final Cursor cursor =
                    makeCursor(groupDbImpl, txn, CursorConfig.DEFAULT);
                try {
                    final OperationStatus status = cursor.getSearchKey(
                        nodeNameKey, new DatabaseEntry(), LockMode.RMW);
                    if (status != OperationStatus.SUCCESS) {
                        throw EnvironmentFailureException.unexpectedState(
                            "Node ID: " + removeNode.getNameIdPair() +
                            " not present in group db");
                    }
                    cursor.delete();
                } finally {
                    cursor.close();
                }
            }
        };

        twoPhaseUpdate.execute();

        /* Refresh group and fire a REMOVE GroupChangeEvent. */
        refreshGroupAndNotifyGroupChange
            (removeNode.getName(), GroupChangeType.REMOVE);

        LoggerUtils.info(logger, repImpl,
                         "Successfully deleted node: " + removeNode.getName());
    }

    /* Add a new rep node into the RepGroupDB. */
    private void createMember(final RepNodeImpl node)
        throws InsufficientReplicasException,
               InsufficientAcksException,
               DatabaseException {

        LoggerUtils.fine
            (logger, repImpl, "Adding node: " + node.getNameIdPair());

        twoPhaseMemberUpdate(node, true);

        LoggerUtils.info(logger, repImpl,
                         "Successfully added node:" + node.getNameIdPair() +
                         " HostPort = " + node.getHostName() + ": " +
                         node.getPort() + " [" + node.getType() + "]");
    }

    /*
     * Update a current rep node information in the RepGroupDB.
     *
     * <p>This method should not be called for nodes with transient IDs.
     *
     * @param node the new node information
     * @param quorumAck whether to require acknowledgments from a quorum
     *
     * @throws InsufficientReplicasException upon failure of 2p member update
     * @throws InsufficientAcksException upon failure of 2p member update
     */
    public void updateMember(final RepNodeImpl node, final boolean quorumAck)
        throws DatabaseException {

        if (node.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Attempt to call updateMember on a node of type " +
                node.getType() + ": " + node);
        }

        LoggerUtils.fine(logger, repImpl, "Updating node: " + node);

        twoPhaseMemberUpdate(node, quorumAck);

        // TODO: clean up the Monitor interface.  There are several aspects of
        // that interface that need fixing; but in particular it ought to have
        // a way to inform listeners that a node has moved to a new network
        // address.  Once that's done, the following should be replaced by a
        // full refreshGroupAndNotifyGroupChange().  And actually that
        // operation should be done closer to where we know the GroupDB has
        // been changed.  In particular, if the GroupDB update suffers an IAE,
        // the exception blows by the following, even though the database
        // actually does now contain the updated value.
        //
        repImpl.getRepNode().refreshCachedGroup();

        LoggerUtils.info(logger, repImpl,
                         "Successfully updated node: " + node.getNameIdPair() +
                         " Hostport = " + node.getHostName() + ": " +
                         node.getPort() + " [" + node.getType() + "]");
    }

    /**
     * Implements the two phase update of membership information.
     *
     * In the first phase the master repeatedly tries to commit the "put"
     * operation until it gets a Quorum of acks, ensuring that the operation
     * has been made durable. Nodes that obtain this entry will start using it
     * in elections. However, the node itself will not participate in elections
     * until it has successfully completed phase 2.
     *
     * In the second phase, the entry for the member is updated to note
     * that a quorum of acks was received.
     *
     * Failure leaves the database with the member info absent, or
     * present but without the update to quorumAcks indicating that a
     * quorum has acknowledged the change.
     *
     * @param node the member info for the node.
     * @param quorumAck whether to require acknowledgments from a quorum
     *
     * @throws InsufficientReplicasException upon failure of 2p member update
     * @throws InsufficientAcksException upon failure of 2p member update
     * @throws DatabaseException upon failure.
     */
    private void twoPhaseMemberUpdate(final RepNodeImpl node,
                                      final boolean quorumAck)
        throws DatabaseException {

        TwoPhaseUpdate twoPhaseUpdate = new TwoPhaseUpdate(node, quorumAck) {
            int saveOrigId = node.getNameIdPair().getId();

            @Override
            void phase1Body() {
                RepGroupImpl repGroup =
                    fetchGroupObject(txn, groupDbImpl, LockMode.RMW);
                repGroup = fetchGroup(repGroup.getName(), groupDbImpl, txn);
                int changeVersion = repGroup.incrementChangeVersion();
                if (node.getNameIdPair().hasNullId()) {
                    node.getNameIdPair().setId(repGroup.getNextNodeId());
                }
                repGroup.checkForConflicts(node);
                saveGroupObject(txn, repGroup, groupDbImpl);
                node.setChangeVersion(changeVersion);
                final RepNodeImpl existingNode =
                    repGroup.getNode(node.getName());
                if ((existingNode != null) && (node.getJEVersion() == null)) {
                    node.updateJEVersion(existingNode.getJEVersion());
                }
                saveNodeObject(txn, node, groupDbImpl, repGroup);
            }

            @Override
            void deadlockHandler() {
                node.getNameIdPair().setId(saveOrigId, false);
            }

            @Override
            void insufficientReplicasHandler() {
                node.getNameIdPair().setId(saveOrigId, false);
            }
        };

        twoPhaseUpdate.execute();
    }

    /**
     * This method is not used when the CBVLSN is defunct -- see GlobalCBVLSN.
     * This method was not moved to GlobalCBVLSN to avoid modularity problems.
     *
     * Updates the database entry associated with the node with the new local
     * CBVLSN, if it can do so without encountering lock contention, and unless
     * the node is a secondary, arbiter, or external node.  Also updates the
     * rep node's transient  group information about the global CBVLSN. If it
     * encounters contention, it returns false, and the caller must retry at
     * some later point in time.
     *
     * Note that changes to the local CBVLSN do not update the group version
     * number since they do not impact group membership.
     *
     * @param nameIdPair identifies the node being updated
     * @param newCBVLSN the new local CBVLSN to be associated with the node.
     * @param nodeType the node type of the RepNode
     * @return true if the update succeeded.
     * @throws DatabaseException
     */
    public boolean updateLocalCBVLSN(final NameIdPair nameIdPair,
                                     final VLSN newCBVLSN,
                                     final NodeType nodeType)
        throws DatabaseException {

        DatabaseImpl groupDbImpl = null;
        try {
            groupDbImpl = repImpl.probeGroupDb();
        } catch (DatabaseException e) {
            /* Contention on the groupDbImpl, try later. */
            return false;
        }

        if (groupDbImpl == null) {
            /* Contention on the groupDbImpl, try later. */
            return false;
        }

        DatabaseEntry nodeNameKey = new DatabaseEntry();
        StringBinding.stringToEntry(nameIdPair.getName(), nodeNameKey);
        DatabaseEntry value = new DatabaseEntry();
        final CleanerBarrierState barrierState =
            new CleanerBarrierState(newCBVLSN, System.currentTimeMillis());
        Txn txn = null;
        Cursor cursor = null;
        boolean ok = false;
        try {

            /*
             * No database update for secondary, arbiter, or external nodes,
             * but set ok to true so that the rep node's group information is
             * updated.
             */
            if (nodeType.isSecondary() || nodeType.isArbiter() ||
                nodeType.isExternal()) {
                ok = true;
                return true;
            }

            final TransactionConfig txnConfig = new TransactionConfig();

            txnConfig.setDurability(NO_ACK_NO_SYNC_DURABILITY);
            /*
             * Don't wait for locks. It's ok to miss an update because we could
             * not acquire the lock, since the operation will be retried later
             * by a subsequent heartbeat message.
             */
            txnConfig.setNoWait(true);
            txn = new MasterTxn(repImpl,
                                txnConfig,
                                repImpl.getNameIdPair());

            /* Read the group first to avoid deadlocks */
            final RepGroupImpl repGroup =
                fetchGroupObject(txn, groupDbImpl, LockMode.DEFAULT);
            cursor = makeCursor(groupDbImpl, txn, CursorConfig.DEFAULT);

            OperationStatus status =
                    cursor.getSearchKey(nodeNameKey, value, LockMode.RMW);
            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    ("Node ID: " + nameIdPair + " not present in group db");
            }

            /* Let's see if the entry needs updating. */
            final NodeBinding nodeBinding =
                new NodeBinding(repGroup.getFormatVersion());
            final RepNodeImpl node = nodeBinding.entryToObject(value);
            final VLSN lastCBVLSN = node.getBarrierState().getLastCBVLSN();
            if (lastCBVLSN.equals(newCBVLSN)) {
                ok = true;
                return true;
            }

            node.setBarrierState(barrierState);
            nodeBinding.objectToEntry(node, value);
            status = cursor.putCurrent(value);
            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    ("Node ID: " + nameIdPair +
                     " stored localCBVLSN could not be updated. Status: " +
                     status);
            }
            LoggerUtils.fine(logger, repImpl,
                             "Local CBVLSN updated to " + newCBVLSN +
                             " for node " + nameIdPair);
            ok = true;
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (txn != null) {
                if (ok) {
                    txn.commit(NO_ACK_NO_SYNC_DURABILITY);
                } else {
                    txn.abort();
                }
                txn = null;
            }
            if (ok) {
                /* RepNode may be null during shutdown. [#17424] */
                RepNode repNode = repImpl.getRepNode();
                if (repNode != null) {
                    repNode.updateGroupInfo(nameIdPair, barrierState);
                }
            }
        }

        return true;
    }

    /*
     * Returns just the de-serialized special rep group object from the
     * database, using the specified lock mode.
     */
    private RepGroupImpl fetchGroupObject(final Txn txn,
                                          final DatabaseImpl groupDbImpl,
                                          final LockMode lockMode)
        throws DatabaseException {

        RepGroupDB.GroupBinding groupBinding = new RepGroupDB.GroupBinding();
        DatabaseEntry groupEntry = new DatabaseEntry();

        Cursor cursor = null;
        try {
            cursor = makeCursor(groupDbImpl, txn, CursorConfig.DEFAULT);

            final OperationStatus status =
                cursor.getSearchKey(groupKeyEntry, groupEntry, lockMode);

            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    ("Group entry key: " + GROUP_KEY +
                     " missing from group database");
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return groupBinding.entryToObject(groupEntry);
    }

    /*
     * Saves the rep group in the database.
     */
    private void saveGroupObject(Txn txn,
                                 RepGroupImpl repGroup,
                                 DatabaseImpl groupDbImpl)
        throws DatabaseException {

        final GroupBinding groupBinding =
            new GroupBinding(repGroup.getFormatVersion());
        DatabaseEntry groupEntry = new DatabaseEntry();
        groupBinding.objectToEntry(repGroup, groupEntry);

        Cursor cursor = null;
        try {
            cursor = makeCursor(groupDbImpl, txn, CursorConfig.DEFAULT);

            OperationStatus status = cursor.put(groupKeyEntry, groupEntry);
            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    ("Group entry save failed");
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /*
     * Save a ReplicationNode in the database, using the format version
     * specified by the group.
     */
    private void saveNodeObject(Txn txn,
                                RepNodeImpl node,
                                DatabaseImpl groupDbImpl,
                                RepGroupImpl repGroup)
        throws DatabaseException {

        assert !node.getType().hasTransientId();

        DatabaseEntry nodeNameKey = new DatabaseEntry();
        StringBinding.stringToEntry(node.getName(), nodeNameKey);

        final NodeBinding nodeBinding =
            new NodeBinding(repGroup.getFormatVersion());
        DatabaseEntry memberInfoEntry = new DatabaseEntry();
        nodeBinding.objectToEntry(node, memberInfoEntry);

        Cursor cursor = null;
        try {
            cursor = makeCursor(groupDbImpl, txn, CursorConfig.DEFAULT);

            OperationStatus status = cursor.put(nodeNameKey, memberInfoEntry);
            if (status != OperationStatus.SUCCESS) {
                throw EnvironmentFailureException.unexpectedState
                    ("Group entry save failed");
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Converts a numeric version string to a JEVersion, returning null for an
     * empty string.
     */
    static JEVersion parseJEVersion(final String versionString) {
        return versionString.isEmpty() ?
            null :
            new JEVersion(versionString);
    }

    /**
     * Converts a JEVersion to a numeric version string, returning an empty
     * string for null.
     */
    static String jeVersionString(final JEVersion jeVersion) {
        return (jeVersion == null) ?
            "" :
            jeVersion.getNumericVersionString();
    }

    /**
     * RepGroupImpl version 3: Add the minJEVersion field
     */
    public static class GroupBinding extends TupleBinding<RepGroupImpl> {

        /**
         * The rep group format version to use for writing, or -1 for reading.
         */
        private final int writeFormatVersion;

        /** Create an instance for reading. */
        public GroupBinding() {
            writeFormatVersion = -1;
        }

        /**
         * Create an instance for writing using the specified group format
         * version.
         */
        GroupBinding(final int writeFormatVersion) {
            if (writeFormatVersion < 0) {
                throw new IllegalArgumentException(
                    "writeFormatVersion must be non-negative: " +
                    writeFormatVersion);
            }
            this.writeFormatVersion = writeFormatVersion;
        }

        @Override
        public RepGroupImpl entryToObject(TupleInput input) {
            if (writeFormatVersion >= 0) {
                throw new IllegalStateException(
                    "GroupBinding not created for read");
            }
            final String name = input.readString();
            final UUID uuid = new UUID(input.readLong(), input.readLong());
            final int formatVersion = input.readInt();
            return new RepGroupImpl(
                name,
                uuid,
                formatVersion,
                input.readInt(),
                input.readInt(),
                ((formatVersion < RepGroupImpl.FORMAT_VERSION_3) ?
                 RepGroupImpl.MIN_FORMAT_VERSION_JE_VERSION :
                 parseJEVersion(input.readString())));
        }

        @Override
        public void objectToEntry(RepGroupImpl group, TupleOutput output) {
            if (writeFormatVersion < 0) {
                throw new IllegalStateException(
                    "GroupBinding not created for write");
            }
            output.writeString(group.getName());
            output.writeLong(group.getUUID().getMostSignificantBits());
            output.writeLong(group.getUUID().getLeastSignificantBits());
            output.writeInt(writeFormatVersion);
            output.writeInt(group.getChangeVersion());
            output.writeInt(group.getNodeIdSequence());
            if (writeFormatVersion >= RepGroupImpl.FORMAT_VERSION_3) {
                output.writeString(jeVersionString(group.getMinJEVersion()));
            }
        }
    }

    /**
     * Supports the serialization/deserialization of node info into and out of
     * the database.  Nodes are always saved using the current group format
     * version, and the node's format version is checked on reading to make
     * sure it is not newer than the current group format version, although
     * they could have an older format version if they have not been saved
     * recently.
     *
     * <p>Prior to RepGroupImpl version 3, the second field was always the
     * ordinal value of the node type, which was either 0 or 1.  Starting with
     * version 3, values greater than 1 are treated as the rep group version of
     * the format used to write the node binding, with the node type following
     * in the next field, and the jeVersion field added at the end.
     */
    public static class NodeBinding extends TupleBinding<RepNodeImpl> {

        /** The approximate maximum size of the serialized form. */
        static final int APPROX_MAX_SIZE =
            40 +                /* node name (guess) */
            4 +                 /* node ID */
            1 +                 /* group version */
            1 +                 /* NodeType */
            1 +                 /* quorumAck */
            1 +                 /* isRemoved */
            40 +                /* hostName (guess) */
            4 +                 /* port */
            8 +                 /* lastCBVLSN */
            8 +                 /* barrierTime */
            4 +                 /* changeVersion */
            10;                 /* jeVersion (approx) */

        /** The maximum node type value for version 2. */
        private static final int V2_MAX_NODE_TYPE = 1;

        /** The group format version to use for reading or writing. */
        private final int groupFormatVersion;

        /**
         * Create an instance for reading or writing using the specified group
         * format version.
         */
        public NodeBinding(final int groupFormatVersion) {
            this.groupFormatVersion = groupFormatVersion;
        }

        @Override
        public RepNodeImpl entryToObject(final TupleInput input) {
            final NameIdPair nameIdPair = NameIdPair.deserialize(input);
            final int versionOrNodeType = input.readByte();
            final boolean v2 = (versionOrNodeType <= V2_MAX_NODE_TYPE);
            if (!v2 && (versionOrNodeType > groupFormatVersion)) {
                throw new IllegalStateException(
                    "Node entry version " + versionOrNodeType + " for node " +
                    nameIdPair.getId() +
                    " is illegal because it is newer than group version " +
                    groupFormatVersion);
            }
            final int nodeTypeNum = v2 ? versionOrNodeType : input.readByte();
            return new RepNodeImpl(
                nameIdPair,
                NodeType.values()[nodeTypeNum],
                input.readBoolean(),
                input.readBoolean(),
                input.readString(),
                input.readInt(),
                new CleanerBarrierState(new VLSN(input.readLong()),
                                 input.readLong()),
                input.readInt(),
                v2 ? null : parseJEVersion(input.readString()));
        }

        /**
         * Returns whether the node can be serialized using the specified group
         * format version.
         */
        public static boolean supportsObjectToEntry(
            final RepNodeImpl node,
            final int groupFormatVersion) {

            /* Version 2 supports a limited set of node types */
            return ((groupFormatVersion > RepGroupImpl.FORMAT_VERSION_2) ||
                    (node.getType().compareTo(NodeType.ELECTABLE) <= 0));
        }

        @Override
        public void objectToEntry(final RepNodeImpl mi,
                                  final TupleOutput output) {
            if (!supportsObjectToEntry(mi, groupFormatVersion)) {
                throw new IllegalArgumentException(
                    "Node type " + mi.getType() +
                    " is not supported for group version " +
                    groupFormatVersion);
            }
            final boolean v2 =
                (groupFormatVersion <= RepGroupImpl.FORMAT_VERSION_2);
            final CleanerBarrierState syncState = mi.getBarrierState();
            mi.getNameIdPair().serialize(output);
            if (!v2) {
                output.writeByte(groupFormatVersion);
            }
            output.writeByte(mi.getType().ordinal());
            output.writeBoolean(mi.isQuorumAck());
            output.writeBoolean(mi.isRemoved());
            output.writeString(mi.getHostName());
            output.writeInt(mi.getPort());
            output.writeLong(syncState.getLastCBVLSN().getSequence());
            output.writeLong(syncState.getBarrierTime());
            output.writeInt(mi.getChangeVersion());
            if (!v2) {
                output.writeString(jeVersionString(mi.getJEVersion()));
            }
        }
    }

    /**
     * Implements two phase updates for membership changes to the group
     * database. It compartmentalizes the retry operations and exception
     * handling so that it's independent of the core logic.
     */
    private abstract class TwoPhaseUpdate {

        final RepNodeImpl node;
        final boolean quorumAck;
        final DatabaseImpl groupDbImpl;

        protected Txn txn;
        private DatabaseException phase1Exception = null;

        TwoPhaseUpdate(final RepNodeImpl node, final boolean quorumAck) {
            this.node = node;
            this.quorumAck = quorumAck;
            try {
                groupDbImpl = repImpl.getGroupDb();
            } catch (DatabaseNotFoundException e) {
                /* Should never happen. */
                throw EnvironmentFailureException.unexpectedException(e);
            }
        }

        /* Phase1 exception handlers for phase1Body-specific cleanup */
        void insufficientReplicasHandler() {}

        void deadlockHandler() {}

        /* The changes to be made in phase1 */
        abstract void phase1Body();

        /* The changes to be made in phase2. */
        void phase2Body() {
            node.setQuorumAck(true);
            final RepGroupImpl repGroup =
                fetchGroupObject(txn, groupDbImpl, LockMode.DEFAULT);
            saveNodeObject(txn, node, groupDbImpl, repGroup);
        }

        private void phase1()
            throws DatabaseException {

            for (int i = 0; i < QUORUM_ACK_RETRIES; i++ ) {
                txn = null;
                try {
                    txn = new MasterTxn(repImpl,
                                        quorumAck ? QUORUM_ACK : NO_ACK,
                                        repImpl.getNameIdPair());
                    phase1Body();
                    txn.commit(
                        quorumAck ? QUORUM_ACK_DURABILITY : NO_ACK_DURABILITY);
                    txn = null;
                    return;
                } catch (InsufficientReplicasException e) {
                    phase1Exception = e;
                    insufficientReplicasHandler();
                    /* Commit was aborted. */
                    LoggerUtils.warning(logger, repImpl,
                                        "Phase 1 retry; for node: " +
                                        node.getName() +
                                        " insufficient active replicas: " +
                                        e.getMessage());
                    continue;
                } catch (InsufficientAcksException e) {
                    phase1Exception = e;
                    /* Local commit completed but did not get enough acks. */
                    LoggerUtils.warning(logger, repImpl,
                                        "Phase 1 retry; for node: " +
                                        node.getName() +
                                        " insufficient acks: " +
                                        e.getMessage());
                    continue;
                } catch (LockConflictException e) {
                    /* Likely a timeout, can't distinguish between them. */
                    phase1Exception = e;
                    deadlockHandler();
                    LoggerUtils.warning(logger, repImpl,
                                        "Phase 1 retry; for node: " +
                                        node.getName() +
                                        " deadlock exception: " +
                                        e.getMessage());
                    continue;
                } catch (DatabaseException e) {
                    LoggerUtils.severe(logger, repImpl,
                                       "Phase 1 failed unexpectedly: " +
                                       e.getMessage());
                    if (txn != null) {
                        txn.abort();
                    }
                    throw e;
                } finally {
                    if (txn != null) {
                        txn.abort();
                    }
                }
            }
            LoggerUtils.warning(logger,
                                repImpl,
                                "Phase 1 failed: " +
                                phase1Exception.getMessage());
            throw phase1Exception;
        }

        private void phase2() {
            try {
                txn = new MasterTxn(repImpl, NO_ACK, repImpl.getNameIdPair());
                phase2Body();
                txn.commit();
                txn = null;
            } catch (DatabaseException e) {
                LoggerUtils.severe(logger, repImpl,
                                   "Unexpected failure in Phase 2: " +
                                   e.getMessage());
                throw e;
            } finally {
                if (txn != null) {
                    txn.abort();
                }
            }
        }

        void execute() {
            Object lock;
            synchronized(lockMap) {
                lock = lockMap.get(node.getName());
                if (lock == null) {
                    lock = new Object();
                    lockMap.put(node.getName(), lock);
                }
            }
            synchronized(lock) {
                phase1();
                /* Only executed if phase 1 succeeds. */
                phase2();
            }
        }
    }

    /**
     * An internal API used to obtain group information by opening a stand
     * alone environment handle and reading the RepGroupDB. Used for debugging
     * and utilities.
     *
     * @param envDir the directory containing the environment log files
     *
     * @return the group as currently defined by the environment
     */
    public static RepGroupImpl getGroup(final File envDir) {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(false);
        Environment env = new Environment(envDir, envConfig);
        Transaction txn = null;
        Database db = null;
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(true);
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(false);
            txn = env.beginTransaction(null, null);
            db = env.openDatabase(txn, DbType.REP_GROUP.getInternalName(),
                                  dbConfig);

            DatabaseEntry groupEntry = new DatabaseEntry();
            OperationStatus status = db.get(
                txn, groupKeyEntry, groupEntry, LockMode.READ_COMMITTED);
            if (status != OperationStatus.SUCCESS) {
                throw new IllegalStateException
                    ("Group entry not found " + status);
            }
            GroupBinding groupBinding = new GroupBinding();
            RepGroupImpl group = groupBinding.entryToObject(groupEntry);

            group = fetchGroup(group.getName(),
                               DbInternal.getDbImpl(db),
                               DbInternal.getTxn(txn));
            txn.commit();
            txn = null;
            return group;
        } finally {
            if (txn != null) {
                txn.abort();
            }
            if (db != null) {
                db.close();
            }
            env.close();
        }
    }

    /**
     * Deletes all the current members from the rep group database and creates
     * a new group, with just the member supplied via the configuration. This
     * method exists to support the utility {@link DbResetRepGroup}
     * <p>
     * The changes proceed in three steps:
     *
     * 1) Determine the node id sequence number. This is to ensure that rep
     * node ids are not reused. Old rep node ids are present in the logs as
     * commit records.
     *
     * 2) A new group object, with the node id sequence number determined
     * in step 1), is created and all existing nodes are deleted.
     *
     * 3) The first node is added to the rep group.
     *
     * @param lastOldVLSN the VLSN used to associate the new barrier wrt this
     * node.
     */
    public void reinitFirstNode(VLSN lastOldVLSN) {

        DbConfigManager configManager = repImpl.getConfigManager();
        String groupName = configManager.get(GROUP_NAME);
        String nodeName = configManager.get(NODE_NAME);
        String hostPortPair = configManager.get(RepParams.NODE_HOST_PORT);
        String hostname = HostPortPair.getHostname(hostPortPair);
        int port = HostPortPair.getPort(hostPortPair);
        final boolean retainUUID =
            configManager.getBoolean(RESET_REP_GROUP_RETAIN_UUID);

        final DatabaseImpl dbImpl = repImpl.getGroupDb();

        /*
         * Retrieve the previous rep group object, so we can use its node
         * sequence id.
         */
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setDurability(NO_ACK.getDurability());
        txnConfig.setConsistencyPolicy(NO_CONSISTENCY);

        NameIdPair nameIdPair = repImpl.getRepNode().getNameIdPair();
        nameIdPair.revertToNull(); /* read transaction, so null id is ok. */

        /* Now delete old nodes and the group, and establish a new group */
        Txn txn = new MasterTxn(repImpl, txnConfig, nameIdPair);
        RepGroupImpl prevRepGroup =
            fetchGroupObject(txn, dbImpl, LockMode.RMW);
        txn.commit();

        final int nodeIdSequenceStart = prevRepGroup.getNodeIdSequence();

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();

        /*
         * We have the "predicted" real node id, so set it and it will be used
         * in the commit lns that will be written in future.
         */
        final int firstNodeId = nodeIdSequenceStart + 1;
        nameIdPair.setId(firstNodeId);

        RepNodeImpl firstNode = new RepNodeImpl(
            nodeName, hostname, port, repImpl.getCurrentJEVersion());
        final CleanerBarrierState barrierState =
            new CleanerBarrierState(lastOldVLSN, System.currentTimeMillis());
        firstNode.setBarrierState(barrierState);

        txn = new MasterTxn(repImpl, txnConfig, nameIdPair);

        final CursorConfig cursorConfig = new CursorConfig();
        cursorConfig.setReadCommitted(true);
        Cursor mcursor = makeCursor(dbImpl, txn, cursorConfig);

        while (mcursor.getNext(keyEntry, value, LockMode.DEFAULT) ==
               OperationStatus.SUCCESS) {
            final String key = StringBinding.entryToString(keyEntry);

            if (GROUP_KEY.equals(key)) {
                final RepGroupImpl repGroup;
                if (retainUUID) {
                    repGroup = new GroupBinding().entryToObject(value);
                    repGroup.incrementChangeVersion();
                } else {
                    repGroup = new RepGroupImpl(
                        groupName, repImpl.getCurrentJEVersion());
                }
                GroupBinding groupBinding =
                    new GroupBinding(repGroup.getFormatVersion());
                repGroup.setNodeIdSequence(nodeIdSequenceStart);
                DatabaseEntry groupEntry = new DatabaseEntry();
                groupBinding.objectToEntry(repGroup, groupEntry);
                OperationStatus status = mcursor.putCurrent(groupEntry);
                if (!OperationStatus.SUCCESS.equals(status)) {
                    throw new IllegalStateException("Unexpected state:" +
                                                    status);
                }
            } else {
                LoggerUtils.info(logger, repImpl, "Removing node: " + key);
                mcursor.delete();
            }
        }
        mcursor.close();
        txn.commit();

        /* Now add the first node of the new group. */
        ensureMember(firstNode);
        if (firstNodeId != firstNode.getNodeId()) {
            throw new IllegalStateException("Expected nodeid:" + firstNodeId +
                                            " but found:" +
                                            firstNode.getNodeId());
        }
    }
}
