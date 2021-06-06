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

package com.sleepycat.je.serializecompatibility;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseExistsException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.DeleteConstraintException;
import com.sleepycat.je.DuplicateDataException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.ForeignConstraintException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.LogWriteException;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.PreloadStatus;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.SequenceExistsException;
import com.sleepycat.je.SequenceIntegrityException;
import com.sleepycat.je.SequenceNotFoundException;
import com.sleepycat.je.SequenceOverflowException;
import com.sleepycat.je.SequenceStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.TransactionTimeoutException;
import com.sleepycat.je.UniqueConstraintException;
import com.sleepycat.je.VersionMismatchException;
import com.sleepycat.je.XAFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.LockPreemptedException;
import com.sleepycat.je.rep.LogOverwriteException;
import com.sleepycat.je.rep.MasterReplicaTransitionException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.RollbackProhibitedException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.cbvlsn.CleanerBarrierState;
import com.sleepycat.je.rep.stream.MatchpointSearchResults;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.util.ldiff.Block;
import com.sleepycat.je.rep.util.ldiff.LDiffRecordRequestException;
import com.sleepycat.je.tree.CursorsExistException;
import com.sleepycat.je.tree.NodeNotEmptyException;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.util.LogVerificationException;
import com.sleepycat.je.util.TestUtils;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.Timestamp;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.persist.IndexNotAvailableException;
import com.sleepycat.persist.StoreExistsException;
import com.sleepycat.persist.StoreNotFoundException;
import com.sleepycat.persist.evolve.DeletedClassException;
import com.sleepycat.persist.evolve.IncompatibleClassException;
import com.sleepycat.util.IOExceptionWrapper;
import com.sleepycat.util.RuntimeExceptionWrapper;
import com.sleepycat.util.keyrange.KeyRangeException;
import com.sleepycat.util.test.SharedTestUtils;

/*
 * A utility class that lists all JE classes that support serialization. Any
 * class that may be serialized by the application and implements a
 * serialVersionUID should be included here.
 */
@SuppressWarnings("deprecation")
public class SerializeUtils {

    /* Create serialized objects. */
    public static Map<String, Object> getSerializedSet() {

        /* Create objects for constructing those exceptions and stats. */
        final StatGroup fakeStats = new StatGroup("SerializeUtils",
                                                  "For testing");
        final String message = "test";
        final DatabaseEntry entry = new DatabaseEntry();
        final Logger logger = Logger.getLogger(message);

        /* Get the Environment home, delete log files created in last test. */
        File envHome = SharedTestUtils.getTestDir();
        SharedTestUtils.cleanUpTestDir(envHome);

        /* Create a ReplicatedEnvironment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        ReplicationConfig repConfig = new ReplicationConfig();
        repConfig.setGroupName(message);
        repConfig.setNodeName(message);
        repConfig.setNodeHostPort("localhost:5001");
        repConfig.setHelperHosts("localhost:5001");

        ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envHome, repConfig, envConfig);

        assert repEnv.getState().isMaster();

        /* Get the EnvironmentImpl and locker to construct the exceptions. */
        final RepImpl envImpl = RepInternal.getNonNullRepImpl(repEnv);
        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);
        final RepNode repNode = envImpl.getRepNode();
        final MasterTxn masterTxn =
            new MasterTxn(envImpl, new TransactionConfig(),
                          envImpl.getNameIdPair());
        masterTxn.commit(Durability.COMMIT_NO_SYNC);

        /* Collection used to save the serialized objects. */
        Map<String, Object> infoSet = new HashMap<String, Object>();

        /* com.sleepycat.je package. */
        infoSet.put("com.sleepycat.je.BtreeStats", new BtreeStats());
        infoSet.put("com.sleepycat.je.CommitToken",
                    new CommitToken(new UUID(1, 2), 0));
        infoSet.put("com.sleepycat.je.DatabaseExistsException",
                    new DatabaseExistsException(message));
        infoSet.put("com.sleepycat.je.DatabaseNotFoundException",
                    new DatabaseNotFoundException(message));
        infoSet.put("com.sleepycat.je.DeadlockException",
                    new DeadlockException(locker, message));
        infoSet.put("com.sleepycat.je.DeleteConstraintException",
                    new DeleteConstraintException
                    (locker, message, message, entry, entry, 0));
        infoSet.put("com.sleepycat.je.DuplicateDataException",
                    new DuplicateDataException(message));
        infoSet.put("com.sleepycat.je.EnvironmentFailureException",
                    new EnvironmentFailureException
                    (envImpl, EnvironmentFailureReason.ENV_LOCKED));
        infoSet.put("com.sleepycat.je.EnvironmentStats",
                    new EnvironmentStats());
        infoSet.put("com.sleepycat.je.EnvironmentLockedException",
                    new EnvironmentLockedException(envImpl, message));
        infoSet.put("com.sleepycat.je.EnvironmentNotFoundException",
                    new EnvironmentNotFoundException(envImpl, message));
        infoSet.put("com.sleepycat.je.ForeignConstraintException",
                    new ForeignConstraintException
                    (locker, message, message, entry, entry, 0));
        infoSet.put("com.sleepycat.je.LockNotAvailableException",
                    new LockNotAvailableException(locker, message));
        infoSet.put("com.sleepycat.je.LockStats",
                    new LockStats(fakeStats, fakeStats, fakeStats));
        infoSet.put("com.sleepycat.je.LockTimeoutException",
                    new LockTimeoutException(locker, message));
        infoSet.put("com.sleepycat.je.LogWriteException",
                    new LogWriteException(envImpl, message));
        infoSet.put("com.sleepycat.je.PreloadStats", new PreloadStats());
        infoSet.put("com.sleepycat.je.PreloadStatus",
                    new PreloadStatus(message));
        infoSet.put("com.sleepycat.je.SecondaryIntegrityException",
                    new SecondaryIntegrityException
                    (null, locker, message, message, entry, entry, 0));
        infoSet.put("com.sleepycat.je.SequenceExistsException",
                    new SequenceExistsException(message));
        infoSet.put("com.sleepycat.je.SequenceIntegrityException",
                    new SequenceIntegrityException(message));
        infoSet.put("com.sleepycat.je.SequenceNotFoundException",
                    new SequenceNotFoundException(message));
        infoSet.put("com.sleepycat.je.SequenceOverflowException",
                    new SequenceOverflowException(message));
        infoSet.put("com.sleepycat.je.SequenceStats",
                    new SequenceStats(fakeStats));
        infoSet.put("com.sleepycat.je.ThreadInterruptedException",
                    new ThreadInterruptedException(envImpl, new Exception()));
        infoSet.put("com.sleepycat.je.TransactionStats",
                    new TransactionStats(fakeStats));
        infoSet.put("com.sleepycat.je.TransactionStats$Active",
                    new TransactionStats.Active(message, 0, 0));
        infoSet.put("com.sleepycat.je.TransactionTimeoutException",
                    new TransactionTimeoutException(locker, message));
        infoSet.put("com.sleepycat.je.UniqueConstraintException",
                    new UniqueConstraintException
                    (locker, message, message, entry, entry, 0));
        infoSet.put("com.sleepycat.je.VersionMismatchException",
                    new VersionMismatchException(envImpl, message));
        infoSet.put("com.sleepycat.je.XAFailureException",
                    new XAFailureException(locker));

        /*
         * com.sleepycat.je.jca.ra package.
         * And because these classes need j2ee.jar to compile, but we currently
         * don't have it in CVS, so ignore them now.
         */
        /******
        infoSet.put("com.sleepycat.je.jca.ra.JEConnectionFactoryImpl",
                    new JEConnectionFactoryImpl(null, null));
        infoSet.put("com.sleepycat.je.jca.ra.JEException",
                    new JEException("test"));
        infoSet.put("com.sleepycat.je.jca.ra.JEManagedConnectionFactory",
                    new JEManagedConnectionFactory());
        ******/

        /* com.sleepycat.je.log package. */
        infoSet.put("com.sleepycat.je.log.ChecksumException",
                    new ChecksumException("test"));

        /* com.sleepycat.je.rep package. */
        infoSet.put("com.sleepycat.je.rep.DatabasePreemptedException",
                    new DatabasePreemptedException(message, message, null));
        infoSet.put("com.sleepycat.je.rep.GroupShutdownException",
                    new GroupShutdownException(logger, repNode, 0));
        infoSet.put("com.sleepycat.je.rep.InsufficientAcksException",
                    new InsufficientAcksException(masterTxn, -1, 0, message));
        final NameIdPair nid = new NameIdPair("foo");
        final CleanerBarrierState barrierState =
            new CleanerBarrierState(VLSN.FIRST_VLSN, 0l);
        ReplicationNode rn =
            new RepNodeImpl(nid, NodeType.ELECTABLE, false, false,
                            "h", 5000,
                            barrierState,
                            0,
                            JEVersion.CURRENT_VERSION);
        infoSet.put("com.sleepycat.je.rep.InsufficientLogException",
                    new InsufficientLogException(envImpl.getRepNode(),
                                                 Collections.singleton(rn)));
        infoSet.put
            ("com.sleepycat.je.rep.InsufficientReplicasException",
             new InsufficientReplicasException
             (locker, ReplicaAckPolicy.NONE, -1, new HashSet<String>()));
        infoSet.put("com.sleepycat.je.rep.LockPreemptedException",
                    new LockPreemptedException(locker, new Exception()));
        infoSet.put("com.sleepycat.je.rep.LogOverwriteException",
                    new LogOverwriteException(message));
        infoSet.put("com.sleepycat.je.rep.MasterStateException",
                    new MasterStateException(message));
        infoSet.put("com.sleepycat.je.rep.MemberNotFoundException",
                    new MemberNotFoundException(""));
        infoSet.put("com.sleepycat.je.rep.MasterReplicaTransitionException",
                    new MasterReplicaTransitionException
                    (envImpl, new Exception("test")));
        infoSet.put("com.sleepycat.je.rep.ReplicaConsistencyException",
                    new ReplicaConsistencyException(message, null));
        final StateChangeEvent stateChangeEvent =
            new StateChangeEvent(State.MASTER, nid);
        infoSet.put("com.sleepycat.je.rep.ReplicaWriteException",
                    new ReplicaWriteException(locker, stateChangeEvent));
        infoSet.put
            ("com.sleepycat.je.rep.ReplicatedEnvironmentStats",
             RepInternal.makeReplicatedEnvironmentStats
             (envImpl, new StatsConfig()));
        infoSet.put("com.sleepycat.je.rep.RollbackException",
                    new RollbackException
                    (envImpl, new VLSN(1),
                     new MatchpointSearchResults(envImpl)));
        infoSet.put("com.sleepycat.je.rep.RollbackProhibitedException",
                    new RollbackProhibitedException
                    (envImpl, 0, false, new VLSN(1),
                     new MatchpointSearchResults(envImpl)));
        infoSet.put("com.sleepycat.je.rep.UnknownMasterException",
                    new UnknownMasterException(message));

        /* com.sleepycat.je.rep.util.ldiff package. */
        infoSet.put
            ("com.sleepycat.je.rep.util.ldiff.LDiffRecordRequestException",
             new LDiffRecordRequestException("test"));
        infoSet.put("com.sleepycat.je.rep.util.ldiff.Block",
                    new Block(1));

        /* com.sleepycat.je.tree package. */
        infoSet.put("com.sleepycat.je.tree.CursorsExistException",
                    new CursorsExistException());
        infoSet.put("com.sleepycat.je.tree.NodeNotEmptyException",
                    new NodeNotEmptyException());

        /* com.sleepycat.je.util package. */
        infoSet.put("com.sleepycat.je.util.LogVerificationException",
                    new LogVerificationException(message));

        /* com.sleepycat.je.utilint package. */
        infoSet.put("com.sleepycat.je.utilint.InternalException",
                    new InternalException());
        infoSet.put("com.sleepycat.je.utilint.VLSN",
                    new VLSN(1));
        infoSet.put("com.sleepycat.je.utilint.Timestamp",
                    new Timestamp(1));

        /* com.sleepycat.persist package. */
        infoSet.put("com.sleepycat.persist.IndexNotAvailableException",
                    new IndexNotAvailableException(message));
        infoSet.put("com.sleepycat.persist.StoreExistsException",
                    new StoreExistsException(message));
        infoSet.put("com.sleepycat.persist.StoreNotFoundException",
                    new StoreNotFoundException(message));

        /* com.sleepycat.persist.evolve package. */
        infoSet.put("com.sleepycat.persist.evolve.DeletedClassException",
                    new DeletedClassException(message));
        infoSet.put("com.sleepycat.persist.evolve.IncompatibleClassException",
                    new IncompatibleClassException(message));

        /* com.sleepycat.util package. */
        infoSet.put("com.sleepycat.util.IOExceptionWrapper",
                    new IOExceptionWrapper(new Throwable()));
        infoSet.put("com.sleepycat.util.RuntimeExceptionWrapper",
                    new RuntimeExceptionWrapper(new Throwable()));

        /* com.sleepycat.util.keyrange package. */
        infoSet.put("com.sleepycat.util.keyrange.KeyRangeException",
                    new KeyRangeException(message));

        /* Release the locker, close Environment and delete log files. */
        locker.operationEnd(true);
        //masterTxn.abort();
        repEnv.close();
        TestUtils.removeLogFiles("TearDown", envHome, false);

        return infoSet;
    }
}
