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

package com.sleepycat.je.rep.persist.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.sleepycat.je.Durability;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.cbvlsn.LocalCBVLSNUpdater;
import com.sleepycat.je.rep.utilint.RepTestUtils;
import com.sleepycat.je.rep.utilint.RepTestUtils.RepEnvInfo;
import com.sleepycat.je.rep.utilint.WaitForMasterListener;
import com.sleepycat.je.util.SimpleClassLoader;
import com.sleepycat.persist.IndexNotAvailableException;
import com.sleepycat.persist.evolve.IncompatibleClassException;
import com.sleepycat.util.test.SharedTestUtils;
import com.sleepycat.util.test.TestBase;

import org.junit.After;
import org.junit.Test;

/**
 * Tests DPL schema evolution that takes place as part of application upgrades
 * in a replication group.
 *
 * Here's an outline of the test procedure for the scenario described in the
 * SR [#16655] as B-1.
 *
 * 1. Start a 2 or more node group.
 * 2. Change persistent classes in some way (see below).
 * 3. Stop the replica, update the app classes, and restart it.
 * 4. Make sure the replica behaves correctly (see below), including when the
 *    Master updates the metadata by storing a new entity class and the Replica
 *    must refresh.
 *
 * More specifically, the persistent class changes and expected behaviors to
 * test are:
 *
 * -  Bump version of entity class and add a field.  Expect unchanged behavior,
 *    new field should be null.
 * -  Add new secondary key, call getSecondaryIndex.  Expect new
 *    IndexNotAvailableException from getSecondaryIndex, unchanged behavior
 *    otherwise.
 * -  Add new entity subclass with secondary key, call getSubclassIndex.
 *    Expect new IndexNotAvailableException from getSubclassIndex, unchanged
 *    behavior otherwise.
 * -  Add new entity class, call getPrimaryIndex.  Expect new
 *    IndexNotAvailableException from getPrimaryIndex, unchanged behavior
 *    otherwise.
 * -  Rename class.  Expect unchanged behavior.
 * -  Rename secondary key.  Expect unchanged behavior.
 */
public class UpgradeTest extends TestBase {

    private static final int N_APP_VERSIONS = 2;
    private static final String APP_IMPL =
        "com.sleepycat.je.rep.persist.test.AppImpl";

    private File envRoot;
    private RepEnvInfo[] repEnvInfo;
    private ReplicatedEnvironment masterEnv;
    private ReplicatedEnvironment replicaEnv1;
    private ReplicatedEnvironment replicaEnv2;
    private AppInterface masterApp;
    private AppInterface replicaApp1;
    private AppInterface replicaApp2;
    private Class[] appClasses = new Class[N_APP_VERSIONS];
    private boolean doInitDuringOpen = true;

    public UpgradeTest() {
        envRoot = SharedTestUtils.getTestDir();
    }

    @After
    public void tearDown() {
        if (repEnvInfo != null) {

            /*
             * close() was not called, test failed. Do cleanup to allow more
             * tests to run, but leave log files for debugging this test case.
             */
            try {
                close(false /*normalShutdown*/);
            } catch (Exception ignored) {
                /* This secondary exception is just noise. */
            }
        }
    }

    /**
     * Creates a 3 node group and initializes the app classes.
     */
    private void open()
        throws Exception {

        /*
         * ReplicaAckPolicy.ALL is used to ensure that when a master operation
         * is committed, the change is immediately available on the replica for
         * testing -- no waiting in the test is needed.
         */
        repEnvInfo = RepTestUtils.setupEnvInfos
            (envRoot, 3,
             RepTestUtils.createEnvConfig
                 (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                                 Durability.SyncPolicy.WRITE_NO_SYNC,
                                 Durability.ReplicaAckPolicy.ALL)),
             new ReplicationConfig());
        masterEnv = RepTestUtils.joinGroup(repEnvInfo);
        replicaEnv1 = repEnvInfo[1].getEnv();
        replicaEnv2 = repEnvInfo[2].getEnv();

        /* Load app classes with custom class loader. */
        final File evolveParentDir =
            new File(System.getProperty("testevolvedir"));
        final ClassLoader parentClassLoader =
            Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < N_APP_VERSIONS; i += 1) {
            final ClassLoader myLoader = new SimpleClassLoader
                (parentClassLoader,
                 new File(evolveParentDir, "dplUpgrade." + i));
            appClasses[i] =
                Class.forName(APP_IMPL, true /*initialize*/, myLoader);
        }

        /* Open v0 app objects. */
        masterApp = newAppObject(0);
        masterApp.open(masterEnv);
        replicaApp1 = newAppObject(0);
        replicaApp1.open(replicaEnv1);
        replicaApp2 = newAppObject(0);
        replicaApp2.open(replicaEnv2);
    }

    private void close() {
        close(true /*normalShutdown*/);
    }

    private void close(boolean normalShutdown) {
        try {
            if (normalShutdown) {
                replicaApp1.close();
                replicaApp2.close();
                masterApp.close();
                RepTestUtils.shutdownRepEnvs(repEnvInfo);
            } else {
                for (RepEnvInfo info : repEnvInfo) {
                    info.abnormalCloseEnv();
                }
            }
        } finally {
            repEnvInfo = null;
            masterEnv = null;
            replicaEnv1 = null;
            replicaEnv2 = null;
            masterApp = null;
            replicaApp1 = null;
            replicaApp2 = null;
        }
    }

    @Test
    public void testClassLoader()
        throws Exception {

        open();

        /* The AppImpl class must not be defined in the normal class loader. */
        try {
            Class.forName(APP_IMPL);
            fail();
        } catch (ClassNotFoundException expected) {
        }

        /* All AppImpl classes must be distinct / different. */
        for (int i = 0; i < N_APP_VERSIONS; i += 1) {
            for (int j = i + 1; j < N_APP_VERSIONS; j += 1) {
                assertNotSame(appClasses[i], appClasses[j]);
            }
        }

        close();
    }

    /**
     * Tests that incremental metadata changes made on the master are visible
     * (refreshed) when needed on the replicas.  Incremental metadata changes
     * occur when not all metadata is known to the DPL initially when the store
     * is opened, and additional metadata is discovered as entities are
     * written.  This is scenario A-1 in the [#16655] SR.
     *
     * This is not actually a schema upgrade test, but is conveniently tested
     * here using the upgrade test framework.
     */
    @Test
    public void testIncrementalMetadataChanges()
        throws Exception {

        open();

        /* Master writes and reads Data entity. */
        masterApp.writeData(0);
        masterApp.readData(0);

        /* Replicas read Data entity. */
        replicaApp1.readData(0);
        replicaApp2.readData(0);

        /* Master writes DataA (subclass), causing a metadata update. */
        masterApp.writeDataA(1);
        masterApp.readDataA(1);

        /* Replicas read DataA and must refresh metadata. */
        replicaApp1.readDataA(1);
        replicaApp2.readDataA(1);

        /* Read Data again for good measure. */
        masterApp.readData(0);
        replicaApp1.readData(0);
        replicaApp2.readData(0);

        close();
    }

    /**
     * Tests that when a replica having stale metadata is elected master, the
     * first metadata update on the new master causes refresh of the stale
     * metadata before the new metadata is written.  This is scenario A-2 in
     * the [#16655] SR.
     *
     * This is not actually a schema upgrade test, but is conveniently tested
     * here using the upgrade test framework.
     */
    @Test
    public void testElectedMasterWithStaleMetadata()
        throws Exception {

        open();

        /* Master writes and reads Data entity. */
        masterApp.writeData(0);
        masterApp.readData(0);

        /* Replicas read Data entity. */
        replicaApp1.readData(0);
        replicaApp2.readData(0);

        /* Master writes DataA (subclass), causing a metadata update. */
        masterApp.writeDataA(1);
        masterApp.readDataA(1);

        /*
         * Master is bounced (but not upgraded), replica1 switches roles with
         * master.
         */
        bounceMaster(0);

        /*
         * Master writes DataB, which requires a metadata change.  Before this
         * new metadata change, it must refresh metadata from disk to get the
         * definition of DataA.
         */
        masterApp.writeDataB(2);

        /*
         * Reading DataA would cause a ClassCastException if refresh did not
         * occur above, because the format ID for DataA would be incorrect.
         */
        masterApp.readDataA(1);

        /* Read all again for good measure. */
        masterApp.readData(0);
        masterApp.readDataA(1);
        masterApp.readDataB(2);
        replicaApp1.readData(0);
        replicaApp1.readDataA(1);
        replicaApp1.readDataB(2);
        replicaApp2.readData(0);
        replicaApp2.readDataA(1);
        replicaApp2.readDataB(2);

        close();
    }

    /**
     * Tests scenarios B-1 and B-2 in the [#16655] SR.
     */
    @Test
    public void testUpgrade()
        throws Exception {

        open();

        /* Master writes and reads v0 entities. */
        masterApp.writeData(0);
        masterApp.writeDataA(1);
        masterApp.writeDataB(2);
        masterApp.readData(0);
        masterApp.readDataA(1);
        masterApp.readDataB(2);

        /* Replicas read v0 entities. */
        replicaApp1.readData(0);
        replicaApp1.readDataA(1);
        replicaApp1.readDataB(2);
        replicaApp2.readData(0);
        replicaApp2.readDataA(1);
        replicaApp2.readDataB(2);

        /* Replica1 is upgraded to v1, upgrades metadata in memory. */
        bounceReplica1(1);

        /* Upgraded replica1 reads v0 entities, can't get new index. */
        try {
            replicaApp1.readData(0);
            fail();
        } catch (IndexNotAvailableException e) {
        }
        try {
            replicaApp1.readDataB(2);
            fail();
        } catch (IndexNotAvailableException e) {
        }

        /* Upgraded replica1 can't get index for new entity NewData2. */
        try {
            replicaApp1.readData2(14);
            fail();
        } catch (IndexNotAvailableException e) {
        }

        /* Replica1 can read v0 DataA, because it has no new indexes. */
        replicaApp1.readDataA(1);

        /* Replica2 (not yet upgraded) reads v0 entities without errors. */
        replicaApp2.readData(0);
        replicaApp2.readDataA(1);
        replicaApp2.readDataB(2);

        /* Replica2 is upgraded to v1, upgrades metadata in memory. */
        bounceReplica2(1);

        /* Upgraded replicas read v0 entities, can't get new index. */
        try {
            replicaApp1.readData(0);
            fail();
        } catch (IndexNotAvailableException e) {
        }
        try {
            replicaApp1.readDataB(2);
            fail();
        } catch (IndexNotAvailableException e) {
        }
        try {
            replicaApp2.readData(0);
            fail();
        } catch (IndexNotAvailableException e) {
        }
        try {
            replicaApp2.readDataB(2);
            fail();
        } catch (IndexNotAvailableException e) {
        }

        /* Upgraded replicas can't get index for new entity NewData2. */
        try {
            replicaApp1.readData2(14);
            fail();
        } catch (IndexNotAvailableException e) {
        }
        try {
            replicaApp2.readData2(14);
            fail();
        } catch (IndexNotAvailableException e) {
        }

        /* Upgraded replicas can read v0 DataA, it has no new indexes. */
        replicaApp1.readDataA(1);
        replicaApp2.readDataA(1);

        /* Read again on master for good measure. */
        masterApp.readData(0);
        masterApp.readDataA(1);
        masterApp.readDataB(2);

        /* Master is upgraded to v1, replica1 switches roles with master. */
        bounceMaster(1);

        /* Metadata is refreshed when new indexes are requested. */
        try {
            masterApp.readData(0);
            fail();
        } catch (DatabasePreemptedException expected) {
            masterApp.close();
            masterApp.open(masterEnv);
            masterApp.readData(0);
        }
        masterApp.readDataA(1);
        masterApp.readDataB(2);
        try {
            replicaApp1.readData(0);
            fail();
        } catch (DatabasePreemptedException expected) {
            replicaApp1.close();
            replicaApp1.open(replicaEnv1);
            replicaApp1.readData(0);
        }
        replicaApp1.readDataA(1);
        replicaApp1.readDataB(2);
        try {
            replicaApp2.readData(0);
            fail();
        } catch (DatabasePreemptedException expected) {
            replicaApp2.close();
            replicaApp2.open(replicaEnv2);
            replicaApp2.readData(0);
        }
        replicaApp2.readDataA(1);
        replicaApp2.readDataB(2);

        /* Master writes v1 entities. */
        masterApp.writeData(10);
        masterApp.writeDataA(11);
        masterApp.writeDataB(12);

        /* Master reads v0 and v1 entities. */
        masterApp.readData(0);
        masterApp.readData(10);
        masterApp.readDataA(1);
        masterApp.readDataA(11);
        masterApp.readDataB(2);
        masterApp.readDataB(12);
        
        /* Replicas read v0 and v1 entities. */
        replicaApp1.readData(0);
        replicaApp1.readData(10);
        replicaApp1.readDataA(1);
        replicaApp1.readDataA(11);
        replicaApp1.readDataB(2);
        replicaApp1.readDataB(12);
        replicaApp2.readData(0);
        replicaApp2.readData(10);
        replicaApp2.readDataA(1);
        replicaApp2.readDataA(11);
        replicaApp2.readDataB(2);
        replicaApp2.readDataB(12);

        /* Master writes new NewDataC subclass, all can read. */
        masterApp.writeDataC(13);
        masterApp.readDataC(13);
        replicaApp1.readDataC(13);
        replicaApp2.readDataC(13);

        /* Master writes new NewData2 entity class, all can read. */
        masterApp.writeData2(14);
        masterApp.readData2(14);
        replicaApp1.readData2(14);
        replicaApp2.readData2(14);

        close();
    }

    /**
     * Ensure that when the master is bounced, the first write will refresh
     * metadata.  The testUpgrade method, OTOH, ensures that metadata is
     * refreshed when new indexes are requested.
     */
    @Test
    public void testRefreshAfterFirstWrite()
        throws Exception {

        open();

        /* Master writes v0 entity, all nodes read. */
        masterApp.writeData(0);
        masterApp.readData(0);
        replicaApp1.readData(0);
        replicaApp2.readData(0);

        /* Replica1 is upgraded to v1, upgrades metadata in memory. */
        bounceReplica1(1);

        /* Upgraded replica1 reads v0 entities, can't get new index. */
        try {
            replicaApp1.readData(0);
            fail();
        } catch (IndexNotAvailableException e) {
        }

        /* Replica2 (not yet upgraded) reads v0 entity without errors. */
        replicaApp2.readData(0);

        /* Replica2 is upgraded to v1, upgrades metadata in memory. */
        bounceReplica2(1);

        /* Read again on master for good measure. */
        masterApp.readData(0);

        /* Master is upgraded to v1, replica1 switches roles with master. */
        bounceMaster(1);

        /* Metadata is refreshed on first write. */
        try {
            masterApp.writeData(10);
            fail();
        } catch (DatabasePreemptedException expected) {
            masterApp.close();
            masterApp.open(masterEnv);
            masterApp.writeData(10);
        }

        /* Replicas also get DatabasePreemptedException on first read. */
        try {
            replicaApp1.readData(0);
            fail();
        } catch (DatabasePreemptedException expected) {
            replicaApp1.close();
            replicaApp1.open(replicaEnv1);
            replicaApp1.readData(0);
        }
        try {
            replicaApp2.readData(0);
            fail();
        } catch (DatabasePreemptedException expected) {
            replicaApp2.close();
            replicaApp2.open(replicaEnv2);
            replicaApp2.readData(0);
        }

        /* All reads now work. */
        masterApp.readData(0);
        masterApp.readData(10);
        replicaApp1.readData(0);
        replicaApp1.readData(10);
        replicaApp2.readData(0);
        replicaApp2.readData(10);

        close();
    }

    /**
     * Tests that a reasonable exception occurs when an upgraded node is
     * elected Master *before* all other nodes have been upgraded.  This is a
     * user error, since the Master election should occur last, but it cannot
     * always be avoided, for example, when an unexpected failover occurs
     * during the upgrade process.
     *
     * There are two cases: (1) when the non-upgraded Replica node is already
     * running when an upgraded node becomes Master, and (2) when the
     * non-upgraded node is brought up as a Replica in a group with an upgraded
     * Master.  However, implementation-wise case (1) becomes case (2), because
     * in case (1) the Replica will attempt to refresh metadata, which is
     * internally the same as bringing up the Replica from scratch.  In both
     * case we instantiate a new PersistCatalog internally, and run class
     * evolution.  This should result in an IncompatibleClassException.
     */
    @Test
    public void testPrematureUpgradedMaster()
        throws Exception {

        open();

        /* Master writes v0 entity, all nodes read. */
        masterApp.writeData(0);
        masterApp.readData(0);
        replicaApp1.readData(0);
        replicaApp2.readData(0);

        /* Replica2 is upgraded to v1, then Master is upgraded to v1. */
        bounceReplica2(1);
        bounceMaster(1);

        /* Replica2 and Replica1 were swapped when the Master was bounced. */
        assertEquals(1, masterApp.getVersion());
        assertEquals(1, replicaApp1.getVersion());
        assertEquals(0, replicaApp2.getVersion());

        /* Write a v1 entity on the Master. */
        try {
            masterApp.writeData(10);
            fail();
        } catch (DatabasePreemptedException expected) {
            masterApp.close();
            masterApp.open(masterEnv);
            masterApp.writeData(10);
        }

        /* The upgraded replica can read the v1 entity. */
        try {
            replicaApp1.readData(10);
            fail();
        } catch (DatabasePreemptedException expected) {
            replicaApp1.close();
            replicaApp1.open(replicaEnv1);
            replicaApp1.readData(10);
        }

        /* The non-upgraded replica will get IncompatibleClassException. */
        try {
            replicaApp2.readData(10);
            fail();
        } catch (DatabasePreemptedException expected) {
            replicaApp2.close();
            try {
                replicaApp2.open(replicaEnv2);
                fail();
            } catch (IncompatibleClassException expected2) {
            }
        }

        /* When finally upgraded, the replica can read the v1 entity. */
        bounceReplica2(1);
        replicaApp2.readData(10);

        /* Read all for good measure. */
        masterApp.readData(0);
        masterApp.readData(10);
        replicaApp1.readData(0);
        replicaApp1.readData(10);
        replicaApp2.readData(0);
        replicaApp2.readData(10);

        close();
    }

    /**
     * Tests a somewhat contrived upgrade scenario with persistent classes A
     * and B that can be embedded in an entity without changing the entity
     * class definition or version.  For example, if a field of type Animal
     * has subclasses Cat and Dog, these subclasses will not be known to the
     * DPL until an instance of them is stored.  When a new subclass is defined
     * in the app, a metadata upgrade will not occur until an instance is
     * stored.
     *
     * The contrived scenario is:
     *
     * + App initially contains persistent class A, but no instance of A is
     *   stored.  Neither the Master nor Replica DPL is aware of class A.
     *
     * + App is modified to contain new persistent class B.
     *
     * + Replica is upgraded, but does not enter Replica Upgrade Mode because
     *   class A and B are not yet used or referenced.
     *
     * + Master writes record containing instance of A, but Replica does not
     *   read it.  Replica metadata is stale.  It so happens that A is assigned
     *   format ID 200 by the Master.
     *
     * + Master is bounced and former Replica is elected Master.
     *
     * + New Master attempts to write record with instances of B, and updates
     *   its metadata, and happens to assign B format ID 200.  If the metadata
     *   previously written by the old Master is overwritten, then metadata
     *   corruption will occur.  Class A and B will have been assigned the same
     *   format ID.
     *
     * + When any node reads record containing A, some sort of exception will
     *   probably occur as a result of the incorrect format ID, or perhaps
     *   incorrect data will be returned.
     *
     * This problem should be avoided because the new Master should detect that
     * its metadata is stale, and refresh it prior to writing the new metadata.
     * It detects this, even though it is not in Replica Upgrade Mode, by
     * reading the metadata before updating it, and checking to see if it has
     * changed since it was last read.  See PersistCatalog.writeDataCheckStale.
     *
     * The scenario is contrived in the sense that it is very unlikely that an
     * instance of A will happen to be written by the Master for the very first
     * time during the upgrade process.  But it does provide a test case for
     * the detection of metadata changes in PersistCatalog.writeData.
     */
    @Test
    public void testRefreshBeforeWrite()
        throws Exception {

        doInitDuringOpen = false;
        open();

        masterApp.insertNullAnimal();
        replicaApp1.readNullAnimal();
        replicaApp2.readNullAnimal();

        /* Replicas upgraded to v1 but do not enter Replica Upgrade Mode. */
        bounceReplica1(1);
        bounceReplica2(1);
        assertFalse(replicaApp1.getStore().isReplicaUpgradeMode());
        assertFalse(replicaApp2.getStore().isReplicaUpgradeMode());

        masterApp.insertDogAnimal();
        bounceMaster(1);
        masterApp.insertCatAnimal();

        masterApp.readDogAnimal();
        masterApp.readCatAnimal();
        replicaApp1.readDogAnimal();
        replicaApp1.readCatAnimal();
        replicaApp2.readDogAnimal();
        replicaApp2.readCatAnimal();

        close();
    }

    /**
     * Bounce replica1.  No election will take place.  If a higher appVersion
     * is specified, the bounced node will also be upgraded.
     */
    private void bounceReplica1(final int appVersion)
        throws Exception {

        replicaApp1.close();
        replicaApp1 = null;
        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() == replicaEnv1) {
                info.closeEnv();
                replicaEnv1 = info.openEnv();
                replicaApp1 = newAppObject(appVersion);
                replicaApp1.open(replicaEnv1);
                break;
            }
        }
        assertNotNull(replicaApp1);
        assertSame(masterEnv.getState(), ReplicatedEnvironment.State.MASTER);
    }

    /**
     * Bounce replica2.  No election will take place.  If a higher appVersion
     * is specified, the bounced node will also be upgraded.
     */
    private void bounceReplica2(final int appVersion)
        throws Exception {

        replicaApp2.close();
        replicaApp2 = null;
        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() == replicaEnv2) {
                info.closeEnv();
                replicaEnv2 = info.openEnv();
                replicaApp2 = newAppObject(appVersion);
                replicaApp2.open(replicaEnv2);
                break;
            }
        }
        assertNotNull(replicaApp2);
        assertSame(masterEnv.getState(), ReplicatedEnvironment.State.MASTER);
    }

    /**
     * Bounce the master, causing replica1 to switch roles with the master.  If
     * a higher appVersion is specified, the bounced node will also be
     * upgraded.
     */
    private void bounceMaster(final int appVersion)
        throws Exception {

        /* Disable updates to RepGroupDB due to LocalCBVLSN updates. */
        LocalCBVLSNUpdater.setSuppressGroupDBUpdates(true);

        for (RepEnvInfo info : repEnvInfo) {
            if (info.getEnv() == masterEnv) {

                /* 
                 * Sync up the replication group so that node2 doesn't do 
                 * hard recovery. 
                 */
                RepTestUtils.syncGroupToLastCommit(repEnvInfo, 
                                                   repEnvInfo.length);
                /* Disable replay on replicas. */
                shutdownFeeder(info.getRepNode(), replicaEnv1);
                shutdownFeeder(info.getRepNode(), replicaEnv2);

                /* Close the master. */
                masterApp.close();
                masterApp = null;
                info.closeEnv();
                masterEnv = null;

                /* Force repEnvInfo[2] to the master. */
                WaitForMasterListener masterWaiter = 
                    new WaitForMasterListener();
                replicaEnv2.setStateChangeListener(masterWaiter);
                RepNode repNode = repEnvInfo[2].getRepNode();
                repNode.forceMaster(true);
                /* Enable the LocalCBVLSN updates. */
                LocalCBVLSNUpdater.setSuppressGroupDBUpdates(false);
                masterWaiter.awaitMastership();
                assertTrue(repNode.isMaster());
                masterEnv = replicaEnv2;

                /* Replica2 was elected, swap names with replica1. */
                final ReplicatedEnvironment tmpEnv = replicaEnv1;
                replicaEnv1 = replicaEnv2;
                replicaEnv2 = tmpEnv;
                final AppInterface tmpApp = replicaApp1;
                replicaApp1 = replicaApp2;
                replicaApp2 = tmpApp;
                
                /* Replica1 (or 2, see above) has been elected master. */
                masterApp = newAppObject(appVersion);
                masterApp.adopt(replicaApp1);
                /* Former master (just upgraded) becomes replica1. */
                replicaEnv1 = info.openEnv();
                replicaApp1.open(replicaEnv1);
                break;
            }
        }
        assertNotNull(masterApp);
        assertSame(masterEnv.getState(), ReplicatedEnvironment.State.MASTER);
    }

    /* 
     * Remove a feeder from the master so that no entries can be replayed on 
     * this node. 
     */
    private void shutdownFeeder(RepNode masterNode, 
                                ReplicatedEnvironment repEnv) 
        throws Exception {

        String nodeName = repEnv.getRepConfig().getNodeName();
        RepNodeImpl removeNode = masterNode.getGroup().getNode(nodeName);
        masterNode.feederManager().shutdownFeeder(removeNode);
    }

    /**
     * Creates an instance of the specified class that implements
     * AppInterface, and returns a Proxy to the instance.  The returned Proxy
     * invokes all methods of the target instance in the context of the
     * ClassLoader of the specified class.
     */
    private AppInterface newAppObject(final int appVersion)
        throws Exception {

        AppInterface app = (AppInterface) Proxy.newProxyInstance
            (AppInterface.class.getClassLoader(),
             new Class[] { AppInterface.class },
             new MyInvocationHandler(appClasses[appVersion].newInstance()));

        app.setVersion(appVersion);
        app.setInitDuringOpen(doInitDuringOpen);

        return app;
    }

    /**
     * Simple InvocationHandler to invoke methods for a given target object in
     * the context of the ClassLoader of the target's class.
     */
    private static class MyInvocationHandler implements InvocationHandler {

        private final Object target;
        private final ClassLoader loader;

        MyInvocationHandler(final Object target) {
            this.target = target;
            this.loader = target.getClass().getClassLoader();
        }

        public Object invoke(final Object proxy,
                             final Method method,
                             final Object[] args)
            throws Throwable {

            final Thread thread = Thread.currentThread();
            final ClassLoader saveLoader = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(loader);
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            } finally {
                thread.setContextClassLoader(saveLoader);
            }
        }
    }
}
