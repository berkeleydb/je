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

/**
 * @see com.sleepycat.je.EnvironmentFailureException
 */
public enum EnvironmentFailureReason {

    ENV_LOCKED
        (false /*invalidates*/,
         "The je.lck file could not be locked."),
    ENV_NOT_FOUND
        (false /*invalidates*/,
         "EnvironmentConfig.setAllowCreate is false so environment " +
         "creation is not permitted, but there are no log files in the " +
         "environment directory."),
    FOUND_COMMITTED_TXN
        (true /*invalidates*/,
         "One committed transaction has been found after a corrupted " + 
         "log entry. The recovery process has been stopped, and the user " + 
         "may need to run DbTruncateLog to truncate the log. Some valid " + 
         "data may be lost if the log file is truncated for recovery."),
    HANDSHAKE_ERROR
        (true /*invalidates*/,
         "Error during the handshake between two nodes. " +
         "Some validity or compatibility check failed, " +
         "preventing further communication between the nodes."),
    HARD_RECOVERY
        (true /*invalidates*/,
         "Rolled back past transaction commit or abort. Must run recovery by" +
         " re-opening Environment handles"),
    JAVA_ERROR
        (true /*invalidates*/,
         "Java Error occurred, recovery may not be possible."),
    LATCH_ALREADY_HELD
        (false /*invalidates*/,
         "Attempt to acquire a latch that is already held, " +
         "may cause a hard deadlock."),
    LATCH_NOT_HELD
        (false /*invalidates*/,
         "Attempt to release a latch that is not currently not held, " +
         "may indicate a thread safety problem."),
    LISTENER_EXCEPTION
        (true, /* invalidates. */
         "An exception was thrown from an application supplied Listener."),
    BTREE_CORRUPTION
        (true /*invalidates*/,
         "Btree corruption is detected, log is likely invalid."),
    LOG_CHECKSUM
        (true /*invalidates*/,
         "Checksum invalid on read, log is likely invalid."),
    LOG_FILE_NOT_FOUND
        (true /*invalidates*/,
         "Log file missing, log is likely invalid."),
    LOG_UNEXPECTED_FILE_DELETION
        (true /*invalidates*/,
         "A log file was unexpectedly deleted, log is likely invalid."),
    LOG_INCOMPLETE
        (true /*invalidates*/,
         "Transaction logging is incomplete, replica is invalid."),
    LOG_INTEGRITY
        (false /*invalidates*/,
         "Log information is incorrect, problem is likely persistent."),
    LOG_READ
        (true /*invalidates*/,
         "IOException on read, log is likely invalid."),
    INSUFFICIENT_LOG
        (true /*invalidates*/,
         "Log files at this node are obsolete.",
         false), // It's ok if the env doesn't exist at this point, 
                 // since this can happen before recovery is complete
    LOG_WRITE
        (true /*invalidates*/,
         "IOException on write, log is likely incomplete."),
    MASTER_TO_REPLICA_TRANSITION
        (true /*invalidates*/,
         "This node was a master and must reinitialize internal state to " +
         "become a replica. The application must close and reopen all " +
         "Environment handles."),
    MONITOR_REGISTRATION
        (false /*invalidates*/,
         "JMX JE monitor could not be registered."),
    PROGRESS_LISTENER_HALT
        (true /* invalidates */,
         "A ProgressListener registered with this environment returned " +
         "false from a call to ProgressListener.progress(), indicating that " +
         "the environment should be closed"),
    PROTOCOL_VERSION_MISMATCH
        (true /*invalidates*/,
         "Two communicating nodes could not agree on a common protocol " +
         "version."),
    ROLLBACK_PROHIBITED
        (true /*invalidates*/,
         "Node would like to roll back past committed transactions, but " +
         "would exceed the limit specified by je.rep.txnRollbackLimit. " +
         "Manual intervention required."),
    SHUTDOWN_REQUESTED
        (true /*invalidates*/,
        "The Replica was shutdown via a remote shutdown request."),
    TEST_INVALIDATE
        (true /*invalidates*/,
         "Test program invalidated the environment."),
    THREAD_INTERRUPTED
        (true /*invalidates*/,
         "InterruptedException may cause incorrect internal state, " +
         "unable to continue."),
    UNCAUGHT_EXCEPTION
        (true /*invalidates*/,
         "Uncaught Exception in internal thread, unable to continue."),
    UNEXPECTED_EXCEPTION
        (false /*invalidates*/,
         "Unexpected internal Exception, may have side effects."),
    UNEXPECTED_EXCEPTION_FATAL
        (true /*invalidates*/,
         "Unexpected internal Exception, unable to continue."),
    UNEXPECTED_STATE
        (false /*invalidates*/,
         "Unexpected internal state, may have side effects."),
    UNEXPECTED_STATE_FATAL
        (true /*invalidates*/,
         "Unexpected internal state, unable to continue."),
    VERSION_MISMATCH
        (false /*invalidates*/,
         "The existing log was written with a version of JE that is " +
         "later than the running version of JE, the log cannot be read."),
    WEDGED
        (true /*invalidates*/,
         "An internal thread could not be stopped. The current process must " +
         "be shut down and restarted before re-opening the Environment. " +
         "A full thread dump has been logged.");

    private final boolean invalidates;
    private final String description;
    
    /* 
     * Generally, environment failure exceptions should be thrown after the
     * environment has been created. One case where this is not true is when
     * an exception can be thrown both during the recovery process, and during
     * normal, post-recovery operations. In the former, we would like to throw
     * the same exception, but it's okay if the environmentImpl is null, because
     * we're still coming up.
     */
    private final boolean envShouldExist;
    
    private EnvironmentFailureReason(boolean invalidates, String description) {
        this(invalidates, description, true);
    }
    
    private EnvironmentFailureReason(boolean invalidates,
                                     String description,
                                     boolean envShouldExist) {
        this.invalidates = invalidates;
        this.description = description;
        this.envShouldExist = envShouldExist;
    }

    public boolean invalidatesEnvironment() {
        return invalidates;
    }

    @Override
    public String toString() {
        return super.toString() + ": " + description;
    }

    /**
     * If true, we expect an environment to exist when this exception is 
     * thrown, and it's okay to assert for existence.
     */
    public boolean envShouldExist() {
        return envShouldExist;
    }
}
