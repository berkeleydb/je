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

package com.sleepycat.je.util;

import java.util.NavigableSet;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.cleaner.FileProtector;
import com.sleepycat.je.cleaner.FileProtector.ProtectedActiveFileSet;
import com.sleepycat.je.cleaner.FileProtector.ProtectedFileRange;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.EmptyLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * DbBackup is a helper class for stopping and restarting JE background
 * activity in an open environment in order to simplify backup operations. It
 * also lets the application create a backup which can support restoring the
 * environment to a specific point in time.
 * <p>
 * <b>Backing up without DbBackup</b>
 * <p>
 * Because JE has an append only log file architecture, it is always possible
 * to do a hot backup without the use of DbBackup by copying all log files
 * (.jdb files) to your archival location. As long as the log files are copied
 * in alphabetical order, (numerical in effect) <i>and</i> all log files are
 * copied, the environment can be successfully backed up without any need to
 * stop database operations or background activity. This means that your
 * backup operation must do a loop to check for the creation of new log files
 * before deciding that the backup is finished. For example:
 * <pre>
 * time    files in                    activity
 *         environment
 *
 *  t0     000000001.jdb     Backup starts copying file 1
 *         000000003.jdb
 *         000000004.jdb
 *
 *  t1     000000001.jdb     JE log cleaner migrates portion of file 3 to newly
 *         000000004.jdb     created file 5 and deletes file 3. Backup finishes
 *         000000005.jdb     file 1, starts copying file 4. Backup MUST include
 *                           file 5 for a consistent backup!
 *
 *  t2     000000001.jdb     Backup finishes copying file 4, starts and
 *         000000004.jdb     finishes file 5, has caught up. Backup ends.
 *         000000005.jdb
 *</pre>
 * <p>
 * In the example above, the backup operation must be sure to copy file 5,
 * which came into existence after the backup had started. If the backup
 * stopped operations at file 4, the backup set would include only file 1 and
 * 4, omitting file 3, which would be an inconsistent set.
 * <p>
 * Also note that log file 5 may not have filled up before it was copied to
 * archival storage. On the next backup, there might be a newer, larger version
 * of file 5, and that newer version should replace the older file 5 in archive
 * storage.
 * <p>
 * Using the approach above, as opposed to using DbBackup, will copy all files
 * including {@link EnvironmentStats#getReservedLogSize reserved files} as well
 * as {@link EnvironmentStats#getActiveLogSize active files}. A large number of
 * reserved files may be present in an HA Environment, and they are essentially
 * wasted space in a backup. Using DbBackup is strongly recommended for this
 * reason, as well as to reduce the complexity of file copying.
 * <p>
 * <b>Backing up with DbBackup</b>
 * <p>
 * DbBackup helps simplify application backup by defining the set of {@link
 * EnvironmentStats#getActiveLogSize active files} that must be copied for each
 * backup operation. If the environment directory has read/write protection,
 * the application must pass DbBackup an open, read/write environment handle.
 * <p>
 * When entering backup mode, JE determines the set of active files needed for
 * a consistent backup, and freezes all changes to those files. The application
 * can copy that defined set of files and finish operation without checking for
 * the ongoing creation of new files. Also, there will be no need to check for
 * a newer version of the last file on the next backup.
 * <p>
 * In the example above, if DbBackup was used at t0, the application would only
 * have to copy files 1, 3 and 4 to back up. On a subsequent backup, the
 * application could start its copying at file 5. There would be no need to
 * check for a newer version of file 4.
 * <p>
 * When it is important to minimize the time that it takes to recover using a
 * backup, a checkpoint should be performed immediately before calling {@link
 * #startBackup}.  This will reduce recovery time when opening the environment
 * with the restored log files.  A checkpoint is performed explicitly by
 * calling {@link Environment#checkpoint} using a config object for which
 * {@link CheckpointConfig#setForce setForce(true)} has been called.
 * <p>
 * <b>Performing simple/full backups</b>
 * <p>
 * The following examples shows how to perform a full backup.  A checkpoint is
 * performed to minimize recovery time.
 * <pre class="code">
 * void myBackup(Environment env, File destDir) {
 *     DbBackup backupHelper = new DbBackup(env);
 *
 *     // Optional: Do a checkpoint to reduce recovery time after a restore.
 *     env.checkpoint(new CheckpointConfig().setForce(true));
 *
 *     // Start backup, find out what needs to be copied.
 *     backupHelper.startBackup();
 *     try {
 *         // Copy the necessary files to archival storage.
 *         String[] filesToCopy = backupHelper.getLogFilesInBackupSet();
 *         myCopyFiles(env, backupHelper, filesToCopy, destDir);
 *     } finally {
 *         // Remember to exit backup mode, or the JE cleaner cannot delete
 *         // log files and disk usage will grow without bounds.
 *        backupHelper.endBackup();
 *     }
 * }
 *
 * void myCopyFiles(
 *     Environment env,
 *     DbBackup backupHelper,
 *     String[] filesToCopy,
 *     File destDir) {
 *
 *     for (String fileName : filesToCopy) {
 *         // Copy fileName to destDir.
 *         // See {@link LogVerificationReadableByteChannel} and
 *         // {@link LogVerificationInputStream}.
 *         ....
 *
 *         // Remove protection to allow file to be deleted in order to reclaim
 *         // disk space.
 *         backupHelper.removeFileProtection(fileName);
 *     }
 * }
 * </pre>
 * When copying files to the backup directory, it is critical that each file is
 * verified before or during the copy.  If a file is copied that is corrupt
 * (due to an earlier disk failure that went unnoticed, for example), the
 * backup will be invalid and provide a false sense of security.
 * <p>
 * The {@link LogVerificationInputStream example here} shows how to implement
 * the {@code myCopyFiles} method using {@link
 * LogVerificationInputStream}. A {@link LogVerificationReadableByteChannel}
 * could also be used for higher performance copying. A filter input stream is
 * used to verify the file efficiently as it is being read.  If you choose to
 * use a script for copying files, the {@link DbVerifyLog} command line tool
 * can be used instead.
 * <p>
 * Assuming that the full backup copied files into an empty directory, to
 * restore you can simply copy these files back into another empty directory.
 * <p>
 * Always start with an empty directory as the destination for a full backup or
 * a restore, to ensure that no unused files are present.  Unused files --
 * perhaps the residual of an earlier environment or an earlier backup -- will
 * take up space, and they will never be deleted by the JE log cleaner.  Also
 * note that such files will not be used by JE for calculating utilization and
 * will not appear in the {@link DbSpace} output.
 * <p>
 * <b>Performing incremental backups</b>
 * <p>
 * Incremental backups are used to reduce the number of files copied during
 * each backup.  Compared to a full backup, there are two additional pieces of
 * information needed for an incremental backup: the number of the last file in
 * the previous backup, and a list of the active files in the environment
 * directory at the time of the current backup, i.e., the current snapshot.
 * Their purpose is explained below.
 * <p>
 * The number of the last file in the previous backup is used to avoid copying
 * files that are already present in the backup set.  This file number must be
 * obtained before beginning the backup, either by checking the backup archive,
 * or getting this value from a stored location.  For example, the last file
 * number could be written to a special file in the backup set at the time of a
 * backup, and then read from the special file before starting the next backup.
 * <p>
 * The list of files in the current snapshot, which should be obtained by
 * calling {@link #getLogFilesInSnapshot} (after calling {@link #startBackup}),
 * is used to avoid unused files after a restore, and may also be used to
 * reduce the size of the backup set.  How to use this list is described below.
 * <p>
 * Some applications need the ability to restore to the point in time of any of
 * the incremental backups that were made in the past, and other applications
 * only need to restore to the point in time of the most recent backup.
 * Accordingly, the list of current files (that is made at the time of the
 * backup), should be used in one of two ways.
 * <ol>
 *   <li>If you only need to restore to the point in time of the most recent
 *   backup, then the list should be used to delete unused files from the
 *   backup set.  After copying all files during the backup, any file that is
 *   <em>not</em> present in the list may then be deleted from the backup set.
 *   This both reduces the size of the backup set, and ensures that unused
 *   files will not be present in the backup set and therefore will not be
 *   restored.</li>
 *   <li>If you need to keep all log files from each backup so you can restore
 *   to more than one point in time, then the list for each backup should be
 *   saved with the backup file set so it can be used during a restore. During
 *   the restore, only the files in the list should be copied, starting with an
 *   empty destination directory.  This ensures that unused files will not be
 *   restored.</li>
 * </ol>
 * <p>
 * The following two examples shows how to perform an incremental backup.  In
 * the first example, the list of current files is used to delete files from
 * the backup set that are no longer needed.
 * <pre class="code">
 * void myBackup(Environment env, File destDir) {
 *
 *     // Get the file number of the last file in the previous backup.
 *     long lastFileInPrevBackup =  ...
 *
 *     DbBackup backupHelper = new DbBackup(env, lastFileInPrevBackup);
 *
 *     // Optional: Do a checkpoint to reduce recovery time after a restore.
 *     env.checkpoint(new CheckpointConfig().setForce(true));
 *
 *     // Start backup, find out what needs to be copied.
 *     backupHelper.startBackup();
 *     try {
 *         // Copy the necessary files to archival storage.
 *         String[] filesToCopy = backupHelper.getLogFilesInBackupSet();
 *         myCopyFiles(env, backupHelper, filesToCopy, destDir);
 *
 *         // Delete files that are no longer needed.
 *         // WARNING: This should only be done after copying all new files.
 *         String[] filesInSnapshot = backupHelper.getLogFilesInSnapshot();
 *         myDeleteUnusedFiles(destDir, filesInSnapshot);
 *
 *         // Update knowledge of last file saved in the backup set.
 *         lastFileInPrevBackup = backupHelper.getLastFileInBackupSet();
 *         // Save lastFileInPrevBackup persistently here ...
 *     } finally {
 *         // Remember to exit backup mode, or the JE cleaner cannot delete
 *         // log files and disk usage will grow without bounds.
 *        backupHelper.endBackup();
 *     }
 * }
 *
 * void myDeleteUnusedFiles(File destDir, String[] filesInSnapshot) {
 *     // For each file in destDir that is NOT in filesInSnapshot, it should
 *     // be deleted from destDir to save disk space in the backup set, and to
 *     // ensure that unused files will not be restored.
 * }
 *
 * See myCopyFiles further above.
 * </pre>
 * <p>
 * When performing backups as shown in the first example above, to restore you
 * can simply copy all files from the backup set into an empty directory.
 * <p>
 * In the second example below, the list of current files is saved with the
 * backup set so it can be used during a restore.  The backup set will
 * effectively hold multiple backups that can be used to restore to different
 * points in time.
 * <pre class="code">
 * void myBackup(Environment env, File destDir) {
 *
 *     // Get the file number of the last file in the previous backup.
 *     long lastFileInPrevBackup =  ...
 *
 *     DbBackup backupHelper = new DbBackup(env, lastFileInPrevBackup);
 *
 *     // Optional: Do a checkpoint to reduce recovery time after a restore.
 *     env.checkpoint(new CheckpointConfig().setForce(true));
 *
 *     // Start backup, find out what needs to be copied.
 *     backupHelper.startBackup();
 *     try {
 *         // Copy the necessary files to archival storage.
 *         String[] filesToCopy = backupHelper.getLogFilesInBackupSet();
 *         myCopyFiles(env, backupHelper, filesToCopy, destDir);
 *
 *         // Save current list of files with backup data set.
 *         String[] filesInSnapshot = backupHelper.getLogFilesInSnapshot();
 *         // Save filesInSnapshot persistently here ...
 *
 *         // Update knowledge of last file saved in the backup set.
 *         lastFileInPrevBackup = backupHelper.getLastFileInBackupSet();
 *         // Save lastFileInPrevBackup persistently here ...
 *     } finally {
 *         // Remember to exit backup mode, or the JE cleaner cannot delete
 *         // log files and disk usage will grow without bounds.
 *        backupHelper.endBackup();
 *     }
 * }
 *
 * See myCopyFiles further above.
 * </pre>
 * <p>
 * When performing backups as shown in the second example above, to restore you
 * must choose one of the file lists that was saved.  You may choose the list
 * written by the most recent backup, or a list written by an earlier backup.
 * To restore, the files in the list should be copied into an empty destination
 * directory.
 * <p>
 * <b><a href="restore">Restoring from a backup</a></b>
 * <p>
 * As described in the sections above, the restore procedure is to copy the
 * files from a backup set into an empty directory.  Depending on the type of
 * backup that was performed (see above), either all files from the backup set
 * are copied, or only the files on a list that was created during the backup.
 * <p>
 * There is one additional consideration when performing a restore, under the
 * following condition:
 * <ul>
 *     <li>Incremental backups are used, AND
 *     <ul>
 *         <li>the backup was created using DbBackup with JE 6.2 or earlier,
 *         OR</li>
 *         <li>the backup was created in a read-only JE environment.</li>
 *     </ul>
 *     </li>
 * </ul>
 * <p>
 * If the above condition holds, after copying the files an additional step is
 * needed. To enable the creation of future incremental backups using the
 * restored files, the {@link
 * com.sleepycat.je.EnvironmentConfig#ENV_RECOVERY_FORCE_NEW_FILE} parameter
 * should be set to true when opening the JE Environment for the first time
 * after the restore.  When this parameter is set to true, the last .jdb file
 * restored will not be modified when opening the Environment, and the next
 * .jdb file will be created and will become the end-of-log file.
 * <p>
 * WARNING: When the above special condition is true and this property is
 * <em>not</em> set to true when opening the environment for the first time
 * after a restore, then the backup set that was restored may not be used as
 * the basis for future incremental backups. If a future incremental backup
 * were performed based on this backup set, it would be incomplete and data
 * would be lost if that incremental backup were restored.
 * <p>
 * When JE 6.3 or later is used to create the backup, and the backup is created
 * in a read-write environment (the usual case), this extra step is
 * unnecessary. In this case, {@link #startBackup} will have added an
 * "immutable file" marker to the last file in the backup and this will prevent
 * that file from being modified, just as if the
 * {@code ENV_RECOVERY_FORCE_NEW_FILE} parameter were set to true.
 */
public class DbBackup {

    private final EnvironmentImpl envImpl;
    private final boolean envIsReadOnly;
    private final long firstFileInBackup;
    private long lastFileInBackup = -1;
    private boolean backupStarted;
    private boolean networkRestore;
    private ProtectedActiveFileSet protectedFileSet;
    private NavigableSet<Long> snapshotFiles;
    /* Status presents whether this back up is invalid because of roll back. */
    private boolean invalid;
    /* The rollback start file number. */
    private long rollbackStartedFileNumber;
    /* For unit tests. */
    private TestHook testHook;

    /**
     * Creates a DbBackup helper for a full backup.
     *
     * <p>This is equivalent to using {@link #DbBackup(Environment,long)} and
     * passing {@code -1} for the {@code lastFileInPrevBackup} parameter.</p>
     *
     * @param env with an open, valid environment handle.  If the environment
     * directory has read/write permissions, the environment handle must be
     * configured for read/write.
     *
     * @throws IllegalArgumentException if the environment directory has
     * read/write permissions, but the environment handle is not configured for
     * read/write.
     */
    public DbBackup(Environment env)
        throws DatabaseException {

        this(env, -1);
    }

    /**
     * Creates a DbBackup helper for an incremental backup.
     *
     * @param env with an open, valid environment handle.  If the environment
     * directory has read/write permissions, the environment handle must be
     * configured for read/write.
     *
     * @param lastFileInPrevBackup the last file in the previous backup set
     * when performing an incremental backup, or {@code -1} to perform a full
     * backup.  The first file in this backup set will be the file following
     * {@code lastFileInPrevBackup}.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalArgumentException if the environment directory has
     * read/write permissions, but the environment handle is not configured for
     * read/write.
     */
    public DbBackup(Environment env, long lastFileInPrevBackup) {
        this(env, DbInternal.getNonNullEnvImpl(env), lastFileInPrevBackup);
    }

    /**
     * @hidden
     * For internal use only.
     */
    public DbBackup(EnvironmentImpl envImpl) {
        this(null, envImpl, -1);
    }

    /**
     * This is the true body of the DbBackup constructor. The env param may be
     * null when this class is used internally.
     */
    private DbBackup(Environment env,
                     EnvironmentImpl envImpl,
                     long lastFileInPrevBackup) {

        /* Check that the Environment is open. */
        if (env != null) {
            DbInternal.checkOpen(env);
        }

        this.envImpl = envImpl;

        /*
         * If the environment is writable, we need a r/w environment handle
         * in order to flip the file.
         */
        envIsReadOnly = envImpl.getFileManager().checkEnvHomePermissions(true);
        if ((!envIsReadOnly) && envImpl.isReadOnly()) {
            throw new IllegalArgumentException
                ("Environment handle may not be read-only when directory " +
                 "is read-write");
        }

        firstFileInBackup = lastFileInPrevBackup + 1;
    }

    /**
     * Start backup mode in order to determine the definitive backup set needed
     * at this point in time.
     *
     * <p>This method determines the last file in the backup set, which is the
     * last log file in the environment at this point in time.  Following this
     * method call, all new data will be written to other, new log files.  In
     * other words, the last file in the backup set will not be modified after
     * this method returns.</p>
     *
     * <p><em>WARNING:</em> After calling this method, deletion of log files in
     * the backup set by the JE log cleaner will be disabled until {@link
     * #endBackup()} is called.  To prevent unbounded growth of disk usage, be
     * sure to call {@link #endBackup()} to re-enable log file deletion.
     * Additionally, the Environment can't be closed until endBackup() is
     * called.
     * </p>
     *
     * @throws com.sleepycat.je.rep.LogOverwriteException if a replication
     * operation is overwriting log files. The backup can not proceed because
     * files may be invalid. The backup may be attempted at a later time.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if a backup is already in progress
     */
    public synchronized void startBackup()
        throws DatabaseException {

        if (backupStarted) {
            throw new IllegalStateException("startBackup was already called");
        }

        /* Throw a LogOverwriteException if the Environment is rolling back. */
        if (!envImpl.addDbBackup(this)) {
            throw envImpl.createLogOverwriteException
                ("A replication operation is overwriting log files. The " +
                 "backup can not proceed because files may be invalid. The " +
                 "backup may be attempted at a later time.");
        }

        final FileProtector fileProtector = envImpl.getFileProtector();

        /*
         * For a network restore we use a different backup name/id and also
         * protect the 2 newest (highest numbered) reserved files. This is a
         * safeguard to ensure that the restored node can function as a master.
         * In a future release this approach may be improved by additionally
         * copying all reserved files to the restored node, but only after it
         * has recovered using the active files [#25783].
         */
        final long backupId = networkRestore ?
            envImpl.getNodeSequence().getNextNetworkRestoreId() :
            envImpl.getNodeSequence().getNextBackupId();

        final String backupName =
            (networkRestore ?
             FileProtector.NETWORK_RESTORE_NAME :
             FileProtector.BACKUP_NAME) +
            "-" + backupId;

        final int nReservedFiles = networkRestore ? 2 : 0;

        /*
         * Protect all files from deletion momentarily, while determining the
         * last file and protecting the active files.
         */
        final ProtectedFileRange allFilesProtected =
            fileProtector.protectFileRange(backupName + "-init", 0);

        try {
            if (envIsReadOnly) {
                /*
                 * All files are currently immutable, so the backup list is the
                 * current set of files. However, we can't add a marker to the
                 * last file in list, and therefore it will not be immutable
                 * after being restored to a read-write directory (unless the
                 * user sets ENV_RECOVERY_FORCE_NEW_FILE after restoring).
                 */
                lastFileInBackup = envImpl.getFileManager().getLastFileNum();
            } else {
                /*
                 * Flip the log so that all files in the backup list are
                 * immutable. But first, write an "immutable file" marker in
                 * the last file in the backup, so it cannot be modified after
                 * it is restored. Recovery enforces this rule.
                 */
                LogEntry marker = new SingleItemEntry<>(
                    LogEntryType.LOG_IMMUTABLE_FILE, new EmptyLogEntry());

                long markerLsn = envImpl.getLogManager().log(
                    marker, ReplicationContext.NO_REPLICATE);

                envImpl.forceLogFileFlip();
                lastFileInBackup = DbLsn.getFileNumber(markerLsn);
            }

            /*
             * Protect all active files from deletion. This includes files
             * prior to firstFileInBackup, in order to get a snapshot of all
             * active files. New files do not need protection, since the backup
             * set does not include them. lastFileInBackup will be protected by
             * protectActiveFiles because it is not the last file in the env.
             */
            protectedFileSet = fileProtector.protectActiveFiles(
                backupName, nReservedFiles, false /*protectNewFiles*/);

        } finally {
            fileProtector.removeFileProtection(allFilesProtected);
        }

        /* At this point, endBackup must be called to undo file protection. */
        backupStarted = true;

        /* Files after lastFileInBackup do not need protection. */
        protectedFileSet.truncateTail(lastFileInBackup);

        /* Snapshot the complete backup file set. */
        snapshotFiles = protectedFileSet.getProtectedFiles();

        /*
         * Now that we have the snapshot, files before firstFileInBackup do not
         * need protection.
         */
        protectedFileSet.truncateHead(firstFileInBackup);
    }

    /**
     * End backup mode, thereby re-enabling normal deletion of log files by the
     * JE log cleaner.
     *
     * @throws com.sleepycat.je.rep.LogOverwriteException if a replication
     * operation has overwritten log files. Any copied files should be
     * considered invalid and discarded.  The backup may be attempted at a
     * later time.
     *
     * @throws com.sleepycat.je.EnvironmentFailureException if an unexpected,
     * internal or environment-wide failure occurs.
     *
     * @throws IllegalStateException if a backup has not been started.
     */
    public synchronized void endBackup() {
        checkBackupStarted();
        backupStarted = false;

        assert TestHookExecute.doHookIfSet(testHook);

        envImpl.getFileProtector().removeFileProtection(protectedFileSet);

        envImpl.removeDbBackup(this);

        /* If this back up is invalid, throw a LogOverwriteException. */
        if (invalid) {
            invalid = false;
            throw envImpl.createLogOverwriteException
                ("A replication operation has overwritten log files from " +
                 "file " + rollbackStartedFileNumber + ". Any copied files " +
                 "should be considered invalid and discarded. The backup " +
                 "may be attempted at a later time.");
        }
    }

    /**
     * Can only be called in backup mode, after startBackup() has been called.
     *
     * @return the file number of the last file in the current backup set.
     * Save this value to reduce the number of files that must be copied at
     * the next backup session.
     *
     * @throws IllegalStateException if a backup has not been started.
     */
    public synchronized long getLastFileInBackupSet() {
        checkBackupStarted();
        return lastFileInBackup;
    }

    /**
     * Get the minimum list of files that must be copied for this backup. When
     * performing an incremental backup, this consists of the set of active
     * files that are greater than the last file copied in the previous backup
     * session.  When performing a full backup, this consists of the set of all
     * active files. Can only be called in backup mode, after startBackup() has
     * been called.
     *
     * <p>The file numbers returned are in the range from the constructor
     * parameter {@code lastFileInPrevBackup + 1} to the last log file at the
     * time that {@link #startBackup} was called.</p>
     *
     * @return the names of all files to be copied, sorted in alphabetical
     * order.  The return values are generally simple file names, not full
     * paths.  However, if multiple data directories are being used (i.e. the
     * {@link <a href="../EnvironmentConfig.html#LOG_N_DATA_DIRECTORIES">
     * je.log.nDataDirectories</a>} parameter is non-0), then the file names are
     * prepended with the associated "dataNNN/" prefix, where "dataNNN/" is
     * the data directory name within the environment home directory and "/"
     * is the relevant file separator for the platform.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if a backup has not been started.
     */
    public synchronized String[] getLogFilesInBackupSet() {
        checkBackupStarted();
        return getFileNames(snapshotFiles.tailSet(firstFileInBackup, true));
    }

    /**
     * Get the minimum list of files that must be copied for this backup. This
     * consists of the set of active files that are greater than the last file
     * copied in the previous backup session.  Can only be called in backup
     * mode, after startBackup() has been called.
     *
     * @param lastFileInPrevBackup file number of last file copied in the last
     * backup session, obtained from getLastFileInBackupSet().
     *
     * @return the names of all the files to be copied that come after
     * lastFileInPrevBackup.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if a backup has not been started.
     *
     * @deprecated replaced by {@link #getLogFilesInBackupSet()}; pass
     * lastFileInPrevBackup to the {@link #DbBackup(Environment,long)}
     * constructor.
     */
    @Deprecated
    public synchronized String[] getLogFilesInBackupSet(
        long lastFileInPrevBackup) {

        checkBackupStarted();

        return getFileNames(
            snapshotFiles.tailSet(lastFileInPrevBackup + 1, true));
    }

    /**
     * Get the list of all active files that are needed for the environment at
     * the point of time when backup mode started, i.e., the current snapshot.
     * Can only be called in backup mode, after startBackup() has been called.
     *
     * <p>When performing an incremental backup, this method is called to
     * determine the files that would needed for a restore.  As described in
     * the examples at the top of this class, this list can be used to avoid
     * unused files after a restore, and may also be used to reduce the size of
     * the backup set.</p>
     *
     * <p>When performing a full backup this method is normally not needed,
     * since in that case it returns the same set of files that is returned by
     * {@link #getLogFilesInBackupSet()}.</p>
     *
     * @return the names of all files in the snapshot, sorted in alphabetical
     * order.  The return values are generally simple file names, not full
     * paths.  However, if multiple data directories are being used (i.e. the
     * {@link <a href="../EnvironmentConfig.html#LOG_N_DATA_DIRECTORIES">
     * je.log.nDataDirectories</a>} parameter is non-0), then the file names are
     * prepended with the associated "dataNNN/" prefix, where "dataNNN/" is
     * the data directory name within the environment home directory and "/"
     * is the relevant file separator for the platform.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if a backup has not been started.
     */
    public synchronized String[] getLogFilesInSnapshot() {
        checkBackupStarted();

        return getFileNames(snapshotFiles);
    }

    private String[] getFileNames(NavigableSet<Long> fileSet) {

        final FileManager fileManager = envImpl.getFileManager();
        final String[] names = new String[fileSet.size()];
        int i = 0;

        for (Long file : fileSet) {
            names[i] = fileManager.getPartialFileName(file);
            i += 1;
        }

        return names;
    }

    /**
     * Removes protection for a file in the backup set. This method should be
     * called after copying a file, so that it may be deleted to avoid
     * exceeding disk usage limits.
     *
     * @param fileName a file name that has already been copied, in the format
     * returned by {@link #getLogFilesInBackupSet} .
     *
     * @since 7.5
     */
    public synchronized void removeFileProtection(String fileName) {
        checkBackupStarted();

        protectedFileSet.removeFile(
            envImpl.getFileManager().getNumFromName(fileName));
    }

    private void checkBackupStarted() {
        if (!backupStarted) {
            throw new IllegalStateException("startBackup was not called");
        }
    }

    /**
     * For internal use only.
     * @hidden
     * Returns true if a backup has been started and is in progress.
     */
    public synchronized boolean backupIsOpen() {
        return backupStarted;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Invalidate this backup if replication overwrites the log.
     */
    public void invalidate(long fileNumber) {
        invalid = true;
        this.rollbackStartedFileNumber = fileNumber;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Marks this backup as a network restore. Causes the protected file set
     * name/id to be set specially, and two reserved files to be included.
     * See {@link #startBackup()}.
     *
     * Another approach (for future consideration) is to factor out the part
     * of DbBackup that is needed by network restore into utility methods,
     * so that special code for network restore can be removed from DbBackup.
     * The shared portion should be just the startBackup code.
     */
    public void setNetworkRestore() {
        networkRestore = true;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * A test entry point used to simulate the environment is now rolling back,
     * and this TestHook would invalidate the in progress DbBackups.
     */
    public void setTestHook(TestHook testHook) {
        this.testHook = testHook;
    }
}
