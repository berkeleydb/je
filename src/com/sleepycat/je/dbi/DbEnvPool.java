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

package com.sleepycat.je.dbi;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Singleton collection of environments.  Responsible for environment open and
 * close, supporting this from multiple threads by synchronizing on the pool.
 *
 * To avoid multiple environment openings from being blocked by recovery in
 * getEnvironment(), the EnvironmentImpl constructor is broken into two parts,
 * with the second part (EnvironmentImpl.finishInit) doing the recovery.
 *
 * When synchronizing on two or more of the following objects the
 * synchronization order must be as follows.  Synchronization is not performed
 * in constructors, of course, because no other thread can access the object.
 *
 * Synchronization order:  Environment, DbEnvPool, EnvironmentImpl, Evictor
 */
public class DbEnvPool {
    /* Singleton instance. */
    private static DbEnvPool pool = new DbEnvPool();

    /*
     * Collection of environment handles, mapped by canonical directory
     * name->EnvironmentImpl object.
     */
    private final Map<String, EnvironmentImpl> envs;

    /* Environments (subset of envs) that share the global cache. */
    private final Set<EnvironmentImpl> sharedCacheEnvs;

    /* Test hook, during Environment creation. */
    private TestHook<EnvironmentImpl> beforeFinishInitHook;

    /**
     * Enforce singleton behavior.
     */
    private DbEnvPool() {
        envs = new HashMap<String, EnvironmentImpl>();
        sharedCacheEnvs = new HashSet<EnvironmentImpl>();
    }

    /**
     * Access the singleton instance.
     */
    public static DbEnvPool getInstance() {
        return pool;
    }

    public void setBeforeFinishInitHook(TestHook<EnvironmentImpl> hook) {
        beforeFinishInitHook = hook;
    }

    public synchronized int getNSharedCacheEnvironments() {
        return sharedCacheEnvs.size();
    }

    private EnvironmentImpl getAnySharedCacheEnv() {
        Iterator<EnvironmentImpl> iter = sharedCacheEnvs.iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    /**
     * Find a single environment, used by Environment handles and by command
     * line utilities.
     *
     * @return a non-null EnvironmentImpl.
     */
    public EnvironmentImpl getEnvironment(File envHome,
                                          EnvironmentConfig config,
                                          boolean checkImmutableParams,
                                          RepConfigProxy repConfigProxy)
        throws EnvironmentNotFoundException, EnvironmentLockedException {

        String environmentKey = null;
        EnvironmentImpl envImpl = null;
        synchronized (this) {
            environmentKey = getEnvironmentMapKey(envHome);
            envImpl = envs.get(environmentKey);

            if (envImpl != null) {

                /*
                 * If the envImpl intance returned is standalone, but users are
                 * actually creating a replicated environment, throw out an
                 * UnsupportedOperationException. We needn't worry about the
                 * read only property, since a replicated environment can't be
                 * read only.
                 */
                if (!envImpl.isReplicated() && (repConfigProxy != null)) {
                    throw new UnsupportedOperationException
                       ("This environment was previously opened as a " +
                        "standalone environment. It cannot be re-opened for " +
                        "replication.");
                }

                /*
                 * If the envImpl instance returned is replicated, but users
                 * are actually creating a standalone environment and it is not
                 * read only, then throw out an UnsupportedOperationException.
                 */
                if (envImpl.isReplicated() && (repConfigProxy == null) &&
                    !config.getReadOnly()) {
                    throw new UnsupportedOperationException
                        ("This environment was previously opened for " +
                         "replication. It cannot be re-opened in read/write " +
                         "mode for standalone operation.");
                }

                /*
                 * If envImpl is used for an Arbiter.
                 */
                if (envImpl.isArbiter()) {
                    throw new UnsupportedOperationException(
                        "An Arbiter is currently using " +
                        "this directory. " + envHome.getAbsolutePath());
                }

                envImpl.checkIfInvalid();

                if (checkImmutableParams) {

                    /*
                     * If a non-null configuration parameter was passed to the
                     * Environment ctor and the underlying EnvironmentImpl
                     * already exists, check that the configuration parameters
                     * specified match those of the currently open environment.
                     * An exception is thrown if the check fails.
                     *
                     * Don't do this check if we create the environment here
                     * because the creation might modify the parameters, which
                     * would create a Catch-22 in terms of validation.  For
                     * example, je.maxMemory will be overridden if the JVM's
                     * -mx flag is less than that setting, so the new resolved
                     * config parameters won't be the same as the passed
                     * in config.
                     */
                    envImpl.checkImmutablePropsForEquality
                        (DbInternal.getProps(config));
                }
                /* Successful, increment reference count */
                envImpl.incOpenCount();
            } else {

                /*
                 * If a shared cache is used, get another (any other,
                 * doesn't matter which) environment that is sharing the
                 * global cache.
                 */
                EnvironmentImpl sharedCacheEnv = config.getSharedCache() ?
                    getAnySharedCacheEnv() : null;

                /*
                 * Environment must be instantiated. If it can be created,
                 * the configuration must have allowCreate set.  Note that
                 * the environment is added to the SharedEvictor before the
                 * EnvironmentImpl ctor returns, by
                 * RecoveryManager.buildTree.
                 */
                envImpl =
                    (repConfigProxy == null) ?
                     new EnvironmentImpl(envHome, config, sharedCacheEnv) :
                     loadRepImpl(envHome, config, sharedCacheEnv,
                                 repConfigProxy);
                assert config.getSharedCache() == envImpl.getSharedCache();

                envImpl.incOpenCount();
                envs.put(environmentKey, envImpl);
                addToSharedCacheEnvs(envImpl);
            }
        }

        /*
         * An new EnvironmentImpl was created. Call finishInit outside the
         * synchronized block to support concurrent recovery for more than one
         * environment.
         *
         * Note that finishInit must be called even if an existing envImpl was
         * found, because initialization (recovery) for that envImpl may not be
         * complete.  finishInit is synchronized and this ensures that recovery
         * will be complete when it returns.
         *
         * If this environment finishInit() fails in any way, make sure it
         * is removed from the envs map. If it isn't, it will block all
         * future attempts to create the environment.
         */
        TestHookExecute.doHookIfSet(beforeFinishInitHook, envImpl);
        boolean success = false;
        try {
            if (envImpl.finishInit(config)) {
                /* Initialization (recovery) was performed. */
                synchronized(this) {
                    finishAdditionOfSharedCacheEnv(envImpl);
                }
            }
            success = true;
        } finally {
            if (!success) {
                synchronized(this) {
                    envs.remove(environmentKey);
                    sharedCacheEnvs.remove(envImpl);
                }
            }
        }

        return envImpl;
    }

    /**
     * Use reflection to create a RepImpl, to avoid introducing HA compilation
     * dependencies to non-replication code.
     */
     private EnvironmentImpl loadRepImpl(File envHome,
                                         EnvironmentConfig config,
                                         EnvironmentImpl sharedCacheEnv,
                                         RepConfigProxy repConfigProxy)
        throws DatabaseException {

        final String repClassName = "com.sleepycat.je.rep.impl.RepImpl";
        final String envImplName = "com.sleepycat.je.dbi.EnvironmentImpl";
        final String repProxy = "com.sleepycat.je.dbi.RepConfigProxy";
        try {
            final Class<?> repClass = Class.forName(repClassName);
            return (EnvironmentImpl)
                repClass.getConstructor(envHome.getClass(),
                                        config.getClass(),
                                        Class.forName(envImplName),
                                        Class.forName(repProxy)).
                newInstance(envHome, config, sharedCacheEnv, repConfigProxy);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                /* Propate runtime exceptions thrown by the ctor. */
                throw (RuntimeException)e.getCause();
            }
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (Exception e) {

            /*
             * This intentially violates our guideline for not catching
             * Exception in order to avoid many catches for all the checked
             * exceptions thrown by Class.forName and Class.getConstructor.  No
             * other methods in the try block throw checked exceptions.
             */
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /* Add this environment into sharedCache environments list. */
    private void addToSharedCacheEnvs(EnvironmentImpl envImpl)
        throws DatabaseException {

        if (envImpl.getSharedCache()) {
            if (sharedCacheEnvs.contains(envImpl)) {
                throw EnvironmentFailureException.unexpectedState();
            }
            sharedCacheEnvs.add(envImpl);
        }
    }

    /* Post-processing of SharedCacheEnv addition, after recovery is done. */
    private void finishAdditionOfSharedCacheEnv(EnvironmentImpl envImpl)
        throws DatabaseException {

        if (envImpl.getSharedCache()) {
            if (!sharedCacheEnvs.contains(envImpl)) {
                throw EnvironmentFailureException.unexpectedState();
            }
            assert envImpl.getEvictor().checkEnv(envImpl);
            resetSharedCache(-1, envImpl);
        }
    }

    /**
     * Called by EnvironmentImpl.setMutableConfig to perform the
     * setMutableConfig operation while synchronized on the DbEnvPool.
     *
     * In theory we shouldn't need to synchronize here when
     * envImpl.getSharedCache() is false; however, we synchronize
     * unconditionally to standardize the synchronization order and avoid
     * accidental deadlocks.
     */
    synchronized void setMutableConfig(EnvironmentImpl envImpl,
                                       EnvironmentMutableConfig mutableConfig)
        throws DatabaseException {

        envImpl.doSetMutableConfig(mutableConfig);
        if (envImpl.getSharedCache()) {
            resetSharedCache(envImpl.getMemoryBudget().getMaxMemory(),
                             envImpl);
        }
    }

    /**
     * Called by EnvironmentImpl.close to perform the close operation while
     * synchronized on the DbEnvPool.
     *
     * Synchronization on this DbEnvPool during the close is used to protect
     * its data structures.  Unfortunately, this means that a long checkpoint
     * during a close will block other closes and opens.  We may want to
     * improve this in the future.  However, at least there is a user
     * workaround: perform a full checkpoint before closing the environment.
     */
    synchronized void closeEnvironment(EnvironmentImpl envImpl,
                                       boolean doCheckpoint,
                                       boolean isAbnormalClose) {
        synchronized (envImpl) {
            /* Hold the reference count stable. */
            if (envImpl.decOpenCount()) {
                try {
                    envImpl.doClose(doCheckpoint, isAbnormalClose);
                } finally {
                    removeEnvironment(envImpl);
                }
            }
        }
    }

    /**
     * Called by EnvironmentImpl.closeAfterInvalid to perform the close
     * operation while synchronized on the DbEnvPool.
     */
    synchronized void closeEnvironmentAfterInvalid(EnvironmentImpl envImpl)
        throws DatabaseException {

        try {
            envImpl.doCloseAfterInvalid();
        } finally {
            removeEnvironment(envImpl);
        }
    }

    /**
     * Removes an EnvironmentImpl from the pool after it has been closed.  This
     * method is called while synchronized.  Note that the environment was
     * removed from the SharedEvictor by EnvironmentImpl.shutdownEvictor.
     */
    private void removeEnvironment(EnvironmentImpl envImpl) {

        final String environmentKey =
            getEnvironmentMapKey(envImpl.getEnvironmentHome());

        final boolean found = envs.remove(environmentKey) != null;

        if (sharedCacheEnvs.remove(envImpl)) {

            assert found && envImpl.getSharedCache();
            assert !envImpl.getEvictor().checkEnv(envImpl);

            if (sharedCacheEnvs.isEmpty()) {
                envImpl.getEvictor().shutdown();
                envImpl.getOffHeapCache().shutdown();
            } else {
                envImpl.getMemoryBudget().subtractCacheUsage();
                resetSharedCache(-1, null);
            }
        } else {
            assert !found || !envImpl.getSharedCache();
        }

        /*
         * Latch notes may only be cleared when there is no possibility that
         * any environment is open.
         */
        if (envs.isEmpty()) {
            LatchSupport.clear();
        }
    }

    /**
     * For unit testing only.
     */
    public synchronized void clear() {
        envs.clear();
    }

    public synchronized Collection<EnvironmentImpl> getEnvImpls() {
        return envs.values();
    }

    public synchronized boolean isOpen(final File home) {
        return envs.containsKey(getEnvironmentMapKey(home));
    }

    /* Use the canonical path name for a normalized environment key. */
    String getEnvironmentMapKey(File file)
        throws DatabaseException {

        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            /* No env is available, can't throw EnvironmentFailedException. */
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Resets the memory budget for all environments with a shared cache.
     *
     * @param newMaxMemory is the new total cache budget or is less than 0 if
     * the total should remain unchanged.  A total greater than zero is given
     * when it has changed via setMutableConfig.
     *
     * @param skipEnv is an environment that should not be reset, or null.
     * Non-null is passed when an environment has already been reset because
     * it was just created or the target of setMutableConfig.
     */
    private void resetSharedCache(long newMaxMemory, EnvironmentImpl skipEnv)
        throws DatabaseException {

        for (EnvironmentImpl envImpl : sharedCacheEnvs) {

            /*
             * To avoid spurious exceptions, don't reset invalid envs that have
             * not yet been removed.  They aren't usable, and we expect them
             * to be closed and removed very soon.
             */
            if (envImpl != skipEnv && envImpl.isValid()) {
                envImpl.getMemoryBudget().reset(newMaxMemory,
                                                false /*newEnv*/,
                                                envImpl.getConfigManager());
            }
        }
    }
}
