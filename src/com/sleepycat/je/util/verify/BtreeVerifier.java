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

package com.sleepycat.je.util.verify;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeWalkerStatsAccumulator;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.StatsAccumulator;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.utilint.StringUtils;

public class BtreeVerifier {

    private final static LockType LOCKTYPE_NOLOCK = LockType.NONE;
    private final static ReadOptions NOLOCK_UNCHANGED = new ReadOptions();
    private final static ReadOptions READLOCK_UNCHANGED = new ReadOptions();
    
    static {
        NOLOCK_UNCHANGED.setCacheMode(CacheMode.UNCHANGED);
        NOLOCK_UNCHANGED.setLockMode(LockMode.READ_UNCOMMITTED);

        READLOCK_UNCHANGED.setCacheMode(CacheMode.UNCHANGED);
        READLOCK_UNCHANGED.setLockMode(LockMode.DEFAULT);
    }

    private final EnvironmentImpl envImpl;
    private final FileManager fileManager;
    private final LogManager logManager;
    private final DbConfigManager configMgr;
    private final Logger logger;
    private final UtilizationProfile up;
    private final FileSizeCache fsCache;
    private final ObsoleteOffsetsCache ooCache;

    private volatile boolean stopVerify = false;
    private VerifyConfig btreeVerifyConfig = new VerifyConfig();

    public static TestHook<Database> databaseOperBeforeBatchCheckHook;
    public static TestHook<Database> databaseOperDuringBatchCheckHook;

    /**
     * Creates a BtreeVerifier object for Btree verification.
     */
    public BtreeVerifier(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        this.fileManager = envImpl.getFileManager();
        this.configMgr = envImpl.getConfigManager();
        this.logManager = envImpl.getLogManager();
        this.logger = envImpl.getLogger();
        this.up = envImpl.getUtilizationProfile();
        this.fsCache = createFileSizeCache();
        this.ooCache = new ObsoleteOffsetsCache();
    }

    /**
     * Verifies all databases in the environment, including idDatabase and
     * nameDatabase.
     */
    public void verifyAll()
        throws DatabaseException {

        /*
         * This aims to guarantee that only if DataVerifier.shutdown is
         * called, then BtreeVerifier will do nothing, including not
         * verifying the nameDatabase and mapDatabase.
         *
         * Without this, the following interleaving may appear. The premise
         * is that DataVerifier.shutdown is called immediately after
         * DataVerifier is created.
         *
         *           T1                                  Timer
         *      verifyTask is created
         *      verifyTask is scheduled
         *      DataVerifier.shutdown is called
         *         verifyTask.cancel()
         *         set stop verify flag
         *         timer.cancel()
         *         check 'task == null || !task.isRunning'
         *         Return true because !task.isRunning
         *
         *                                       Due to some reason, although
         *                                       verifyTask.cancel() and
         *                                       timer.cancel() is called,
         *                                       verifyTask can still execute
         *                                       once. So DataVerifier.shutdown
         *                                       does not achieve its target.
         *  After we add the following code, even if verifyTask can execute,
         *  it will do nothing. BtreeVerifier and DbVerifyLog will just return
         *  because now we have already set the stop flag to be true.
         */
        if (stopVerify) {
            return;
        }

        DbTree dbTree = envImpl.getDbTree();

        final PrintStream out =
            (btreeVerifyConfig.getShowProgressStream() != null) ?
            btreeVerifyConfig.getShowProgressStream() : System.err;

        final String startMsg = "Start verify all databases";
        final String stopMsg = "End verify all databases";

        if (btreeVerifyConfig.getPrintInfo()) {
            out.println(startMsg);
        }
        LoggerUtils.envLogMsg(Level.INFO, envImpl, startMsg);

        try {
            /* Verify NameDb and MappingDb. */
            verifyOneDb(
                DbType.ID.getInternalName(), DbTree.ID_DB_ID, out,
                true /*verifyAll*/);

            verifyOneDb(
                DbType.NAME.getInternalName(), DbTree.NAME_DB_ID, out,
                true /*verifyAll*/);

            /*
             * Verify all the remaining databases.
             *
             * Get a cursor db on the naming tree. The cursor is used to get
             * the name for logging, as well as the ID of each DB. Each DB
             * is verified by batch, e.g. verifying 1000 records each time. So
             * for each batch, the DB ID will be used to get real-time
             * DatabaseImpl. If the databaseImpl is valid, i.e. not null and
             * not deleted, then the next batch of records will be verified.
             *
             * This aims to leave a window where the DatabaseImpl is not in use
             * between batches, to allow db truncate/remove operations to run.
             */
            class Traversal implements CursorImpl.WithCursor {

                public boolean withCursor(
                    CursorImpl cursor,
                    @SuppressWarnings("unused") DatabaseEntry key,
                    @SuppressWarnings("unused") DatabaseEntry data)
                    throws DatabaseException {

                    if (stopVerify) {
                        return false;
                    }

                    final NameLN nameLN =
                        (NameLN) cursor.lockAndGetCurrentLN(LOCKTYPE_NOLOCK);

                    if (nameLN != null && !nameLN.isDeleted()) {

                        final DatabaseId dbId = nameLN.getId();

                        final String dbName =
                            StringUtils.fromUTF8(key.getData());

                        verifyOneDb(dbName, dbId, out, true /*verifyAll*/);
                    }
                    return true;
                }
            }

            Traversal traversal = new Traversal();

            CursorImpl.traverseDbWithCursor(
                dbTree.getNameDatabaseImpl(), LOCKTYPE_NOLOCK,
                true /*allowEviction*/, traversal);

        } finally {
            if (btreeVerifyConfig.getPrintInfo()) {
                out.println(stopMsg);
            }
            LoggerUtils.envLogMsg(Level.INFO, envImpl, stopMsg);
        }
    }

    /**
     * Verify one database.
     */
    public BtreeStats verifyDatabase(String dbName, DatabaseId dbId) {

        PrintStream out = btreeVerifyConfig.getShowProgressStream();
        if (out == null) {
            out = System.err;
        }

        return verifyOneDb(dbName, dbId, out, false /*verifyAll*/);
    }

    /**
     * Verify one database, a batch at a time.
     *
     * @param verifyAll if true, we won't log INFO messages for every database
     * to avoid cluttering the trace log.
     */
    private BtreeStats verifyOneDb(
        String dbName,
        DatabaseId dbId,
        PrintStream out,
        boolean verifyAll) {

        final String startMsg = "Start verify database: " + dbName;
        final String stopMsg = "End verify database: " + dbName;

        if (btreeVerifyConfig.getPrintInfo()) {
            out.println(startMsg);
        }
        if (!verifyAll) {
            LoggerUtils.envLogMsg(Level.INFO, envImpl, startMsg);
        }

        try {
            final int batchSize = btreeVerifyConfig.getBatchSize();
            final long batchDelay =
                btreeVerifyConfig.getBatchDelay(TimeUnit.MILLISECONDS);

            /*
             * The accumulated information for this database.
             */
            final VerifierStatsAccumulator statsAcc =
                new VerifierStatsAccumulator(
                    out, btreeVerifyConfig.getShowProgressInterval());

            /* Check whether this DatabaseImpl is primary or secondary db. */
            envImpl.checkOpen();
            DbTree dbTree = envImpl.getDbTree();
            DatabaseImpl dbImpl = dbTree.getDb(dbId);

            boolean isSecondaryDb = false;
            SecondaryDatabase secDb = null;
            Database priDb = null;
            try {
                if (dbImpl == null || dbImpl.isDeleted()) {
                    return new BtreeStats();
                }

                Set<Database> referringHandles = dbImpl.getReferringHandles();
                for (Database db : referringHandles) {
                    priDb = db;
                    if (db instanceof SecondaryDatabase) {
                        isSecondaryDb = true;
                        secDb = (SecondaryDatabase) db;
                        priDb = null;
                        break;
                    }
                }
            } finally {
                dbTree.releaseDb(dbImpl);
            }

            DatabaseEntry lastKey = null;
            DatabaseEntry lastData = null;

            while (true) {
                envImpl.checkOpen();
                dbTree = envImpl.getDbTree();
                dbImpl = dbTree.getDb(dbId);

                try {
                    if (stopVerify) {
                        break;
                    }

                    if (dbImpl == null || dbImpl.isDeleted()) {
                        break;
                    }

                    if (databaseOperBeforeBatchCheckHook != null) {
                        if (priDb != null) {
                            databaseOperBeforeBatchCheckHook.doHook(priDb);
                        } else {
                            databaseOperBeforeBatchCheckHook.doHook(secDb);
                        }
                    }

                    WalkDatabaseTreeResult result = walkDatabaseTree(
                        dbImpl, isSecondaryDb, priDb, secDb, statsAcc,
                        lastKey, lastData, batchSize);

                    if (result.noMoreRecords) {
                        break;
                    }

                    lastKey = result.lastKey;
                    lastData = result.lastData;
                } finally {
                    dbTree.releaseDb(dbImpl);
                }

                if (batchDelay > 0) {
                    try {
                        Thread.sleep(batchDelay);
                    } catch (InterruptedException e) {
                        throw new ThreadInterruptedException(envImpl, e);
                    }
                }
            }

            final BtreeStats stats = new BtreeStats();
            stats.setDbImplStats(statsAcc.getStats());

            if (btreeVerifyConfig.getPrintInfo()) {
                /*
                 * Intentionally use print, not println, because
                 * stats.toString() puts in a newline too.
                 */
                out.print(stats);
            }

            return stats;

        } catch (BtreeVerificationException bve) {
            /*
             * A persistent corruption is detected due to the btree
             * corruption, or a checksum exception was encountered when
             * trying to read the entry from disk to determine whether
             * the corruption is persistent.
             */
            if (bve.getCause() instanceof ChecksumException) {
                /*
                 * When a checksum exception occurs during processing of a
                 * Btree corruption, the checksum error should override,
                 * because it means that the log entry on disk is probably
                 * meaningless. In other words, this is really a media
                 * corruption, not a corruption caused by a bug.
                 */
                throw VerifierUtils.createMarkerFileFromException(
                    RestoreRequired.FailureType.LOG_CHECKSUM,
                    bve.getCause(),
                    envImpl,
                    EnvironmentFailureReason.LOG_CHECKSUM);
            } else {
                throw VerifierUtils.createMarkerFileFromException(
                    RestoreRequired.FailureType.BTREE_CORRUPTION,
                    bve,
                    envImpl,
                    EnvironmentFailureReason.BTREE_CORRUPTION);
            }
        } finally {
            if (btreeVerifyConfig.getPrintInfo()) {
                out.println(stopMsg);
            }
            if (!verifyAll) {
                LoggerUtils.envLogMsg(Level.INFO, envImpl, stopMsg);
            }
        }
    }

    /*
     * This method is called in StatsAccumulator.verifyNode, which means that
     * this method will execute every time it encounters one upperIN or BIN.
     *
     * In this method, only the basic structure issue of IN and the dangling
     * LSN issue for upperIN are checked. The dangling LSN issue for BIN
     * and other features verification, e.g. VERIFY_SECONDARIES,
     * VERIFY_DATA_RECORDS and VERIFY_OBSOLETE_RECORDS, are checked when
     * the cursor positions at each slot.
     */
    private void basicBtreeVerify(Node node) {
        /*
         * When accessing upper IN, shared latch is used most of the time. It
         * is OK to hold this latch longer than usual (because it is shared).
         * So the dangling LSN issue for all slots of this upperIN can be
         * checked without releasing the latch.
         */
        if (node.isUpperIN()) {
            verifyDanglingLSNAndObsoleteRecordsAllSlots(node);
        }

        /*
         * For upperIN and BIN, their basic structure is checked here. This may
         * also hold the latch for a long time.
         */
        verifyCommonStructure(node);
    }

    /*
     * Possible basic structure may contain:
     *  1. keyPrefix
     *  2. inMemorySize
     *  3. parent IN
     *  4. ordered Keys
     *  5. identifier Key and so on.
     *
     * On 1, the keyPrefix cannot be re-calculated from the full keys here,
     * since the full keys are not stored in the IN. We could get the full key
     * from the LNs, but this would be very slow.
     *
     * On 2, the inMemorySize may be slightly inaccurate, and this would not be
     * considered corruption. It is recalculated during checkpoints to account
     * for errors.
     *
     * For 3, we should verify that the node's parent is correct, i.e. the
     * parent should have a slot that refers to the child using the correct
     * key. But this has already been done in the current code:
     *      There are three places to call IN.accumulateStats, i.e. calling
     *      acc.processIN:
     *        1. Tree.getNextIN
     *        2. Tree.search
     *        3. Tree.searchSubTree
     * 
     *      At these places, before calling IN.accumulateStats, the current
     *      code uses latchChildShared or latchChild to check whether the
     *      parent is right when holding the parent latch and child latch.
     *
     * For 4 and 5, we can check for corruption here.
     * For 4, whole keys need to be obtained using IN.getKey.
     * For 5, user's comparator function needs to be called if exists.
     */
    private void verifyCommonStructure(Node node) {
        assert node.isIN();
        IN in = (IN) node;

        verifyOrderedKeys(in);
        verifyIdentifierKey(in);
    }

    /*
     * Here we can not get DatabaseImpl from IN, because the IN may be
     * read directly from file.
     */
    private int verifyOrderedKeysInternal(IN in, DatabaseImpl dbImpl) {
        Comparator<byte[]> userCompareToFcn = dbImpl.getKeyComparator();

        for (int i = 1; i < in.getNEntries(); i++) {
            byte[] key1 = in.getKey(i);
            byte[] key2 = in.getKey(i - 1);

            int s = Key.compareKeys(key1, key2, userCompareToFcn);
            if (s <= 0) {
                return i;
            }
        }
        return 0;
    }

    private void verifyOrderedKeys(IN in) {
        DatabaseImpl dbImpl = in.getDatabase();
        final int corruptIndex = verifyOrderedKeysInternal(in, dbImpl);
        if (corruptIndex == 0) {
            return;
        }

        final Pair<Long, Long> targetLsns = getTargetLsns(in);

        /* For security/privacy, we cannot output keys. */
        final String label = "IN keys are out of order. ";
        final String msg1 = label +
            in.toSafeString(corruptIndex - 1, corruptIndex);

        IN inFromFile = getINFromFile(targetLsns, dbImpl, msg1);

        try {
            final int newCorruptIndex =
                verifyOrderedKeysInternal(inFromFile, dbImpl);

            if (newCorruptIndex == 0) {
                throw EnvironmentFailureException.unexpectedState(
                    envImpl, transientMsg(msg1));
            } else {
                final String msg2 = label +
                    inFromFile.toSafeString(
                        newCorruptIndex - 1, newCorruptIndex);

                throw new BtreeVerificationException(persistentMsg(msg2));
            }
        } finally {
            inFromFile.releaseLatchIfOwner();
        }
    }

    private void verifyIdentifierKey(IN in) {

        DatabaseImpl dbImpl = in.getDatabase();
        if (verifyIdentifierKeyInternal(in, dbImpl)) {
            return;
        }

        final Pair<Long, Long> targetLsns = getTargetLsns(in);

        /* For security/privacy, we cannot output keys. */
        final String label = "IdentifierKey not present in any slot. ";
        final String msg1 = label + in.toSafeString(null);

        IN inFromFile = getINFromFile(targetLsns, dbImpl, msg1);

        try {
            if (verifyIdentifierKeyInternal(inFromFile, dbImpl)) {
                throw EnvironmentFailureException.unexpectedState(
                    envImpl, transientMsg(msg1));
            } else {
                final String msg2 = label + inFromFile.toSafeString(null);

                throw new BtreeVerificationException(persistentMsg(msg2));
            }
        } finally {
            inFromFile.releaseLatchIfOwner();
        }
    }

    private boolean verifyIdentifierKeyInternal(IN in, DatabaseImpl dbImpl) {

        /*
         * This check can only be done for full BIN, not upperIn and BIN-delta.
         * Besides, if the slot number is 0, then we may also not check this.
         */
        if (in.isUpperIN() || in.isBINDelta() || in.getNEntries() == 0) {
            return true;
        }

        byte[] identifierKey = in.getIdentifierKey();
        if (identifierKey == null) {
            return false;
        }

        /*
         * There are two problematic cases about identifierKey which are caused
         * by some errors in previous code:
         *
         * (1). The identifierKey is a prefix key due to the DupConvert bug.
         *
         *    When reading log files written by JE 4.1 or earlier, the
         *    identifier key may be incorrect because DupConvert did not
         *    convert it correctly. DupConvert converts the identifier key to
         *    a prefix key, so it will not match the complete key in any slot.
         *
         *    We should probably fix DupConvert. But even if we fix it now,
         *    it won't help users of JE 5.0 and above who have already upgraded
         *    from JE 4.1 or earlier, because DupConvert is only used when
         *    reading log files written by JE 4.1 or earlier.
         *
         *    This issue seems harmless, at least no user reports errors caused
         *    by it. So we can choose to ignore this issue. Normally, we can
         *    identify this issue by checking the end of the key for the
         *    PREFIX_ONLY value. But unfortunately this will also ignore
         *    identifier keys that happen to have the PREFIX_ONLY value at the
         *    end of a complete key(in the user's data).
         *
         *    Considering the following second issue, we choose to not check
         *    identifierKey for environments who is initially created with
         *    LogEntryType.LOG_VERSION being LT 15, where 15 is just the new
         *    log version of JE after we fix the following second issue.
         *
         * (2). The identifierKey is not in any slot due to the BIN-delta
         * mutation bug.
         * 
         *     The fullBIN identifierKey may have changed when reconstituteBIN
         *     called BIN.compress. The previous code forgot to reset it. Now
         *     we fix this by reseting the identifier in BIN.mutateToFullBIN.
         *
         *     For the problematic identifierKey which is caused by the
         *     BIN-delta mutation bug, we do not have good methods to correct
         *     them. We can only detect them.
         *
         *     The problem with detecting them is that we know it is incorrect
         *     in past releases, but even when it is incorrect, we don't know
         *     the impact on the app in a particular case. It is possible that
         *     the app is working OK, even though the identifier key is
         *     incorrect. So if we detect it and the app stops working
         *     (because we invalidate the env) then we may be making things
         *     worse for the app -- this may not be what the user wants.
         *
         *  So combing above (1) and (2), we need to add a way to know the
         *  earliest log version of the env. Then we can only validate the
         *  identifierKey when this version is >= 15, where 15 is just the new
         *  log version of JE after we fix (2). See DbTree.initialLogVersion
         *  and LogEntryType.LOG_VERSION.
         */
        if (envImpl.getDbTree().getInitialLogVersion() < 15) {
            return true;
        }

        Comparator<byte[]> userCompareToFcn = dbImpl.getKeyComparator();

        for (int i = 0; i < in.getNEntries(); i++) {
            byte[] key = in.getKey(i);
            if (Key.compareKeys(identifierKey, key, userCompareToFcn) == 0) {
                return true;
            }
        }

        return false;
    }

    /*
     * For upperIN, we verify all the slots at one time.
     *
     * Note that for upperINs, we only need to verify dangling LSN issue
     * and basic structure issue. The former is checked here and basic
     * structure issue is checked in following verifyCommonStructure.
     */
    private void verifyDanglingLSNAndObsoleteRecordsAllSlots(Node node) {
        assert node.isUpperIN();
        IN in = (IN) node;
        for (int i = 0; i < in.getNEntries(); i++) {
            verifyDanglingLSNAndObsoleteRecordsOneSlot(i, in, false /*isBin*/);
        }
    }

    private void verifyDanglingLSNAndObsoleteRecordsOneSlot(
        int index,
        IN in,
        boolean isBin) {

        /* If the slot of BIN is defunct, then just return. */
        if (isBin && ((BIN) in).isDefunct(index)) {
            return;
        }

        verifyDanglingLSN(index, in, isBin);
        verifyObsoleteRecords(index, in, isBin);
    }

    /*
     * Verify the dangling LSN issue for each slot of BIN or IN.
     */
    private void verifyDanglingLSN(int index, IN in, boolean isBin) {

        /*
         * If the environment is opened with setting LOG_MEMORY_ONLY be
         * true, there will be no log files. We just ignore it.
         */
        if (envImpl.isMemOnly()) {
            return;
        }

        DatabaseImpl dbImpl = in.getDatabase();

        DanglingLSNCheckResult result =
            verifyDanglingLSNInternal(index, in, isBin, dbImpl);

        if (result.problematicIndex < 0) {
            return;
        }

        final Pair<Long, Long> targetLsns = getTargetLsns(in);

        /* For security/privacy, we cannot output keys. */
        final String label = "LSN is invalid. ";
        final String msg1 =
            label + result.getReason() +
            in.toSafeString(result.problematicIndex);

        IN inFromFile = getINFromFile(targetLsns, dbImpl, msg1);

        try {
            boolean findAgain = false;
            for (int i = 0; i < inFromFile.getNEntries(); i++) {
                result =
                    verifyDanglingLSNInternal(i, inFromFile, isBin, dbImpl);
                if (result.problematicIndex >= 0) {
                    findAgain = true;
                    break;
                }
            }

            if (!findAgain) {
                throw EnvironmentFailureException.unexpectedState(
                    envImpl, transientMsg(msg1));
            } else {
                final String msg2 =
                    label + result.getReason() +
                    inFromFile.toSafeString(result.problematicIndex);

                throw new BtreeVerificationException(persistentMsg(msg2));
            }
        } finally {
            inFromFile.releaseLatchIfOwner();
        }
    }

    private DanglingLSNCheckResult verifyDanglingLSNInternal(
        int index,
        IN in,
        boolean isBin,
        DatabaseImpl databaseImpl) {

        /*
         * For BIN, if the database has duplicates or the the LN is an
         * embedded LN, or the slot is deleted, we do not check the
         * dangling LSN issue.
         */
        if (isBin &&
            (in.isEmbeddedLN(index) || databaseImpl.getSortedDuplicates() ||
            databaseImpl.isLNImmediatelyObsolete() ||
            ((BIN) in).isDefunct(index))) {
            return DanglingLSNCheckResult.NO_DANGLING_LSN;
        }

        final long curLsn = in.getLsn(index);
        if (DbLsn.isTransientOrNull(curLsn)) {
            return DanglingLSNCheckResult.NO_DANGLING_LSN;
        }
        final long fileNum = DbLsn.getFileNumber(curLsn);
        final long fileOffset = DbLsn.getFileOffset(curLsn);

        /*
         * Check whether the corresponding file exist and whether the
         * LSN's offset is less than the file's length.
         */
        final int lastLoggedSize = in.getLastLoggedSize(index);
        final FileSizeInfo fsInfo = getFileSize(fileNum);
        if (fileOffset + lastLoggedSize > fsInfo.size) {
            if (fsInfo.size == -1) {
                return new DanglingLSNCheckResult(index, true, fsInfo);
            }
            return new DanglingLSNCheckResult(index, false, fsInfo);
        }

        return DanglingLSNCheckResult.NO_DANGLING_LSN;
    }

    private static class DanglingLSNCheckResult {

        private static final DanglingLSNCheckResult NO_DANGLING_LSN =
            new DanglingLSNCheckResult(-1, true, null);

        /*
         * -1 means that no dangling LSN issue exists. An integer which
         * is gte 0 shows that location of the problematic slot.
         */
        int problematicIndex;

        /*
         * True means the issue is because the file does not exist. False
         * means that the issue is because the log entry exceeds the end
         * of the file.
         */
        boolean fileNotExist;
        FileSizeInfo fsInfo;

        DanglingLSNCheckResult(
            int problematicIndex,
            boolean fileNotExist,
            FileSizeInfo fsInfo) {
            this.problematicIndex = problematicIndex;
            this.fileNotExist = fileNotExist;
            this.fsInfo = fsInfo;
        }

        String getReason() {
            return (fileNotExist ? "File does not exist. " :
                "Offset[+lastLoggerSize] exceeds the end of the file. ") +
                "fileSize=" + fsInfo.size + ". " + fsInfo.getReason();
        }
    }

    private static class FileSizeInfo {
        boolean sizeFromLastFile;

        /*
         * True if the file size was previously in the FileSizeCache,
         * false if it is calculated and added to the cache.
         */
        boolean sizeFromCache;
        int size;
        
        FileSizeInfo(
            boolean sizeFromLastFile,
            boolean sizeFromCache,
            int size) {
            this.sizeFromLastFile = sizeFromLastFile;
            this.sizeFromCache = sizeFromCache;
            this.size = size;
        }

        String getReason() {
            return (sizeFromLastFile ? "File size from last file" :
                (sizeFromCache ? "File size previously cached" :
                    "File size added to cache")) + ". ";
        }
    }

    /**
     * @return if the FileSizeInfo.size is gte 0, then it means that
     * the file does exist.
     */
    private FileSizeInfo getFileSize(long fileNum) {
        /*
         * The last file is a special case, because its totalSize is changing
         * and this file in the FileSummary is not volatile. For the last file
         * we can use getNextLsn to get the fileNum and offset of the last
         * file.
         */
        long nextLsn = fileManager.getNextLsn();
        if (fileNum == DbLsn.getFileNumber(nextLsn)) {
            return new FileSizeInfo(
                true, false, (int) DbLsn.getFileOffset(nextLsn));
        } else {
            Pair<Boolean, Integer> result = fsCache.getFileSize(fileNum);
            return new FileSizeInfo(false, result.first(), result.second());
        }
    }

    private interface FileSizeCache {

        /**
         * @return {wasCached, size}
         */
        Pair<Boolean, Integer> getFileSize(long fileNum);
    }

    private FileSizeCache createFileSizeCache() {

        /*
         * Currently we don't use the UtilizationProfile for getting file
         * sizes because testing has shown it is inaccurate. This needs
         * further debugging.
         */
        final boolean USE_UP = false;
        if (USE_UP) {
            return new UPFileSizeCache();
        } else {
            return new DirectFileSizeCache();
        }
    }

    /**
     * Used to get file sizes directly from the File class.
     */
    private class DirectFileSizeCache implements FileSizeCache {

        private final Map<Long, Integer> cache;

        DirectFileSizeCache() {
            cache = new HashMap<>();
        }

        @Override
        public Pair<Boolean, Integer> getFileSize(long fileNum) {

            Integer size = cache.get(fileNum);
            if (size != null) {
                return new Pair<>(true, size);
            }

            final File file = new File(fileManager.getFullFileName(fileNum));
            size = (int) file.length();
            cache.put(fileNum, size);
            return new Pair<>(false, size);
        }
    }

    /*
     * Use a map to cache the file total size info.
     * 1. First call UtilizationProfile.getFileSizeSummaryMap to get the
     *    initial copy info.
     * 2. When a file is not present in the cached map, call
     *    UtilizationProfile.getFileSize to get it and add its total size
     *    to the cached map.
     * 3. The last file is a special case, because its totalSize is changing
     *    and this file in the FileSummary is not volatile. For the last file,
     *    we handle it in getFileSize, i.e. using getNextLsn to get the fileNum
     *    and offset of the last file.
     */
    private class UPFileSizeCache implements FileSizeCache {

        final SortedMap<Long, Integer> fileSizeSummaryMap;

        UPFileSizeCache() {
            fileSizeSummaryMap = up.getFileSizeSummaryMap();
        }

        @Override
        public Pair<Boolean, Integer> getFileSize(long fileNum) {

            if (fileSizeSummaryMap.containsKey(fileNum)) {
                return new Pair<>(true, fileSizeSummaryMap.get(fileNum));
            }

            int size = up.getFileSize(fileNum);
            if (size != -1) {
                fileSizeSummaryMap.put(fileNum, size);
            }
            return new Pair<>(false, size);
        }
    }

    /*
     * Verify the obsolete records issue for each slot of BIN or IN.
     */
    private void verifyObsoleteRecords(int index, IN in, boolean isBin) {
        if (!btreeVerifyConfig.getVerifyObsoleteRecords()) {
            return;
        }

        final DatabaseImpl databaseImpl = in.getDatabase();
        /* 
         * For BIN, if the database is duplicate or the the LN is
         * embedded LN, we do not check the dangling LSN issue.
         */
        if (isBin &&
            (in.isEmbeddedLN(index) || databaseImpl.getSortedDuplicates() ||
            databaseImpl.isLNImmediatelyObsolete())) {
            return;
        }

        final long curLsn = in.getLsn(index);
        final long fileNum = DbLsn.getFileNumber(curLsn);

        /*
         * TODO: How to check the corruption is persistent?
         *    For dangling LSN, we can read the latest written entry from the
         *    log. Although the CRUD operations may cause some slots of the
         *    read log entry to be obsolete, for normal case, the file
         *    containing these slots should have not been deleted. [Is this
         *    right?]. So checking the logged entry is rational.
         *    
         *    But for checking obsolete records, the slots of the read log
         *    entry, at the current time point, can really locate at the
         *    obsolete offsets. Then is it still rational to re-check the
         *    read log entry?. The answer is true.
         *
         *    If an IN slot has an LSN that is obsolete, and that slot was
         *    added or change recently and has not been flushed to disk,
         *    then the corruption is not persistent. So re-fetching the IN from
         *    disk is needed only to see if the LSN is persistently present
         *    in the slot.
         */
        final long[] offsets = ooCache.getOffsets(fileNum);

        /*
         * If the active lsn exists in the obsolete lsn offsets, throw
         * EFE.unexpectedException.
         */
        if (Arrays.binarySearch(offsets, DbLsn.getFileOffset(curLsn)) >= 0) {
            throw new EnvironmentFailureException(
                envImpl,
                EnvironmentFailureReason.UNEXPECTED_EXCEPTION_FATAL,
                "Active lsn is obsolete: " + DbLsn.getNoFormatString(curLsn) +
                    in.toSafeString(index));
        }
    }

    /*
     * Similar to FileSummaryCache but holds obsolete LSN offsets.
     *
     * This cache may contain outdated information, since LSNs may become
     * obsolete during the verification process, and the cache is not updated.
     * This is OK because:
     *  - an obsolete LSN can never become active again, and
     *  - there is no requirement to detect corruption that occurs during the
     *    scan.
     */
    private class ObsoleteOffsetsCache {
        final SortedMap<Long, long[]> obsoleteOffsetsMap;

        ObsoleteOffsetsCache() {
            obsoleteOffsetsMap = new TreeMap<>();
        }

        long[] getOffsets(long fileNum) {
            if (obsoleteOffsetsMap.containsKey(fileNum)) {
                return obsoleteOffsetsMap.get(fileNum);
            }

            long[] offsets = up.getObsoleteDetailSorted(fileNum);
            obsoleteOffsetsMap.put(fileNum, offsets);
            return offsets;
        }
    }

    private String persistentMsg(String msg) {
        return "Btree corruption was detected and is persistent. Re-opening " +
            "the Environment is not possible without restoring from backup " +
            " or from another node. " + msg;
    }

    private String transientMsg(String msg) {
        return "Btree corruption was detected in memory, but does not appear" +
            "to be persistent. Re-opening the Environment may be possible. " +
            msg;
    }

    private Pair<Long, Long> getTargetLsns(IN in) {
        long targetLsn1;
        long targetLsn2 = DbLsn.NULL_LSN;
        if (in.isUpperIN()) {
            targetLsn1 = in.getLastFullLsn();
            targetLsn2 = DbLsn.NULL_LSN;
        } else {
            BIN bin = (BIN) in;
            long lastDeltaVersion = bin.getLastDeltaLsn();
            if (lastDeltaVersion == DbLsn.NULL_LSN) {
                /*
                 * The most recently written logrec for this BIN instance
                 * is full BIN.
                 */
                targetLsn1 = bin.getLastFullLsn();
            } else {
                /*
                 * The most recently written logrec for this BIN instance
                 * is BIN-delta.
                 */
                targetLsn1 = lastDeltaVersion;
                targetLsn2 = bin.getLastFullLsn();
            }
        }
        return new Pair<>(targetLsn1, targetLsn2);
    }

    /*
     * When detecting btree corruption, we want to directly read the related
     * BIN, or BIN-delta, or both from the log file to confirm whether the
     * corruption is persistent.
     *
     * @return latched IN.
     */
    private IN getINFromFile(
        final Pair<Long, Long> targetLsns,
        final DatabaseImpl dbImpl,
        final String msg) {

        WholeEntry entry;
        WholeEntry optionalFullBinEntry = null;

        /* Read the entry directly from log */
        try {
            entry = logManager.getLogEntryDirectFromFile(targetLsns.first());
            if (targetLsns.second() != DbLsn.NULL_LSN) {
                optionalFullBinEntry =
                    logManager.getLogEntryDirectFromFile(targetLsns.second());
            }
            if (entry == null && optionalFullBinEntry == null) {
                throw EnvironmentFailureException.unexpectedState(
                    envImpl, transientMsg(msg));
            }
        } catch (ChecksumException ce) {
            throw new BtreeVerificationException(null, ce);
        }

        IN inFromFile = null;
        if (entry != null) {
            inFromFile = (IN) entry.getEntry().getMainItem();
        }

        if (optionalFullBinEntry != null) {
            BIN optionalFullBin =
                (BIN) optionalFullBinEntry.getEntry().getMainItem();
            if (inFromFile != null) {
                ((BIN) inFromFile).reconstituteBIN(
                    dbImpl, optionalFullBin, false);
            }
            inFromFile = optionalFullBin;
        }

        inFromFile.latchNoUpdateLRU(dbImpl);
        return inFromFile;
    }

    private static class WalkDatabaseTreeResult {
        DatabaseEntry lastKey;
        DatabaseEntry lastData;
        boolean noMoreRecords;

        private static final WalkDatabaseTreeResult NO_MORE_RECORDS =
            new WalkDatabaseTreeResult(null, null, true);

        WalkDatabaseTreeResult(
            DatabaseEntry lastKey,
            DatabaseEntry lastData,
            boolean noMoreRecords) {

            this.lastKey = lastKey;
            this.lastData = lastData;
            this.noMoreRecords = noMoreRecords;
        }
    }

    private boolean findFirstRecord(
        DatabaseImpl dbImpl,
        Cursor cursor,
        DatabaseEntry lastKey,
        DatabaseEntry lastData) {

        DatabaseEntry usedKey = new DatabaseEntry();
        DatabaseEntry usedData = new DatabaseEntry();
        /* The first record of this db. */
        if (lastKey == null) {
            return cursor.get(
                usedKey, usedData, Get.FIRST, NOLOCK_UNCHANGED) != null;
        }

        /* Find the first record according to (lastKey, lastData). */
        usedKey = new DatabaseEntry(
            lastKey.getData(), lastKey.getOffset(), lastKey.getSize());
        usedData = new DatabaseEntry(
            lastData.getData(), lastData.getOffset(), lastData.getSize());

        boolean isDuplicated = dbImpl.getSortedDuplicates();
        OperationResult result = null;
        if (isDuplicated) {
            result = cursor.get(
                usedKey, usedData, Get.SEARCH_BOTH_GTE, NOLOCK_UNCHANGED);
            if (result != null) {
                if (!usedData.equals(lastData)) {
                    /* Find next dup of lastKey. */
                    return true;
                }

                /*
                 * Find lastKey/lastData. Move to the next dup of lastKey or
                 * move to the first dup of next key.
                 */
                return cursor.get(
                    usedKey, usedData, Get.NEXT, NOLOCK_UNCHANGED) != null;
            } else {
                result = cursor.get(
                    usedKey, usedData, Get.SEARCH_GTE, NOLOCK_UNCHANGED);
                if (result == null) {
                    /* No more records. */
                    return false;
                }

                if (!usedKey.equals(lastKey)) {
                    /* Find the first dup of next key. */
                    return true;
                }

                /*
                 * Find the first dup of lastKey. Skip over dups of lastKey
                 * to the first dup of next key. This may miss "phantoms" but
                 * that is OK -- see comments 26 and 28 in [#25960].
                 */
                return cursor.get(
                    usedKey, usedData, Get.NEXT_NO_DUP,
                    NOLOCK_UNCHANGED) != null;
            }
        } else {
            result = cursor.get(
                usedKey, usedData, Get.SEARCH_GTE, NOLOCK_UNCHANGED);
            if (result == null) {
                /* No more records. */
                return false;
            }

            if (!usedKey.equals(lastKey)) {
                /* Find next key. */
                return true;
            }

            /* Find lastKey. Move to next key. */
            return cursor.get(
                usedKey, usedData, Get.NEXT, NOLOCK_UNCHANGED) != null;
        }
    }

    /**
     * Verify one batch of records for the given DB.
     */
    private WalkDatabaseTreeResult walkDatabaseTree(
        DatabaseImpl dbImpl,
        boolean isSecondaryDb,
        Database priDb,
        SecondaryDatabase secDb,
        TreeWalkerStatsAccumulator statsAcc,
        DatabaseEntry lastKey,
        DatabaseEntry lastData,
        int batchSize) {

        /* Traverse the database. */
        Tree tree = dbImpl.getTree();
        EnvironmentImpl.incThreadLocalReferenceCount();
        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);
        Cursor cursor = DbInternal.makeCursor(dbImpl, locker, null, false);
        CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
        cursorImpl.setTreeStatsAccumulator(statsAcc);
        tree.setTreeStatsAccumulator(statsAcc);

        /*
         * Use local caching to reduce DbTree.getDb overhead.  Do not call
         * releaseDb after getDb with the dbCache, since the entire dbCache
         * will be released at the end of this method.
         */
        final Map<DatabaseId, DatabaseImpl> dbCache = new HashMap<>();

        try {
            /*
             * Four parts need to be checked: basic, index, primary record and
             * obsolete. 'basic' and 'obsolete' are checked for each slot
             * for both secondary db and primary db, and they do not need
             * the data portion.
             *
             * Data portion is needed only for the following two situations:
             * 1. Db is secondary and index needs to be checked
             * 2. Db is primary, verifySecondaries and verifyDataRecords are
             *    both true.
             *
             * Actually, now we have the following combinations:
             * verifySecondaries/verifyDataRecords   Meaning
             *
             *  No       No        Do not read the primary LN.  
             *                     Do not verify any secondaries.
             *
             *  Yes      No        Do not read the primary LN. 
             *                     Check that the secondary records refer to
             *                     existing primary records.
             *
             *  No       Yes       Read the LN as a basic check.
             *                     Do not verify any secondaries.
             *
             *  Yes      Yes       Read the LN as a basic check.
             *                     Check that the secondary records refer to
             *                     existing primary records.
             *                     Check that primary records refer to
             *                     existing secondary records.
             *
             * According to above combinations, only when verifySecondaries
             * and verifyDataRecords are both true, for a primary database,
             * we will check that primary records refer to existing secondary
             * records.
             *
             * But only if verifyDataRecords is true, for a primary database,
             * we need to check that the primary LN is valid, i.e. we need
             * to read data portion. This is why we do not use
             * verifyPrimaryDataRecords to replace (!isSecondaryDb &&
             * btreeVerifyConfig.getVerifyDataRecords()) when determining
             * whether we need to read the data portion.
             */
            boolean verifyPrimaryDataRecords =
                priDb != null &&
                btreeVerifyConfig.getVerifySecondaries() &&
                btreeVerifyConfig.getVerifyDataRecords();
            boolean verifySecondary =
                isSecondaryDb &&
                btreeVerifyConfig.getVerifySecondaries();
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            if (!(verifySecondary ||
                (priDb != null && btreeVerifyConfig.getVerifyDataRecords()))) {

                foundData.setPartial(0, 0, true);
            }

            /* Whether the first record for this round check exists. */
            if (!findFirstRecord(dbImpl, cursor, lastKey, lastData)) {
                return WalkDatabaseTreeResult.NO_MORE_RECORDS;
            }

            /*
             * The previous readPrimaryAfterGet implementation has a problem
             * when used in btree verification: it cannot detect
             * corruption when secDirtyRead is true and the primary record
             * is NOT_FOUND. In this situation, we don't have any locks,
             * so we don't know the true current state of either the primary or
             * secondary record.
             *
             * Therefore, for the index verification, we need to lock the
             * secondary first. And then use non-blocking lock to lock
             * primary record to avoid deadlock. If we cannot lock the primary
             * record, we can just skip the verification.
             * 
             * If verifyPrimaryDataRecords is true, we will first get the
             * record without acquiring a lock in this method and then try
             * to acquire a Read lock in verifyPrimaryData. So in
             * walkDatabaseTee we use READLOCK_UNCHANGED only when
             * verifySecondary is true.
             */
            int recordCount = 0;
            while (++recordCount <= batchSize) {

                /* Stop the verification process asap. */
                if (stopVerify) {
                    return WalkDatabaseTreeResult.NO_MORE_RECORDS;
                }

                try {
                    /*
                     * <1> For primary database:
                     * 1. The cursor.get(CURRENT, NEXT) used in this method
                     * all use lockMode NOLOCK_UNCHANGED. So there will not
                     * be a LockConflictException.
                     * 2. The (foundKey, foundData) will not be used in this
                     * method, so it is OK that their data array is null.
                     * The cursor.get(CURRENT, NEXT) in this method only aims
                     * to locate position. Note that, although we may verify
                     * primary record data, we will do that in
                     * verifyPrimaryData.
                     *
                     * <2> For secondary database (NOT verify secondary): the
                     * same with primary database.
                     *
                     * <3> For secondary database (verify secondary), a
                     * simple approach is problematic:
                     * 1. Before verifying the secondary record, we first need
                     * to READ lock the secondary record. So
                     * LockConflictException may be thrown.
                     * 2. The (foundKey, foundData) will be used to find the
                     * corresponding primary record. So foundData (priKey) can
                     * not be null.
                     * 3. We need to use nonSticky==true to avoid a deadlock
                     * when calling cursor.get(NEXT). But if cursor.get(NEXT)
                     * cannot succeed due to LockConflictException or
                     * something else, the cursorImpl will be reset, i.e. its
                     * previous location will be lost. This is not what we
                     * expect.
                     *
                     * The solution:
                     * 1. Use nonSticky==true
                     * 2. Use LockMode.READ_UNCOMMITTED when doing Get.NEXT.
                     *    This can resolve 3 above.
                     *    Because Get.NEXT will not acquire lock, if more
                     *    records exist, Get.NEXT can always succeed,
                     *    i.e. Get.NEXT can move to next record. So
                     *    'nonSticky==true' will not cause the cursorImpl to
                     *    move to an invalid position.
                     * 3. Use Get.CURRENT with LockMode.DEFAULT to
                     *    lock the record and read the record.
                     *    This can resolve 1 above.
                     *    This will acquire a READ lock on the record.
                     * 4. If Get.CURRENT in (3) returns null, i.e. the record
                     *    may have been deleted, then we will throw an internal
                     *    exception to cause the cursor to move to next slot.
                     *    This will resolve 2 above.
                     */
                    if (!isSecondaryDb || !verifySecondary) {
                        if (recordCount == 1) {
                            cursor.get(
                                foundKey, foundData, Get.CURRENT,
                                NOLOCK_UNCHANGED);
                        }
                    } else {
                        OperationResult result = cursor.get(
                            foundKey, foundData, Get.CURRENT,
                            READLOCK_UNCHANGED);
                        if (result == null) {
                            throw new MoveToNextRecordException();
                        }
                    }

                    /*
                     * Note that if we change this code to set nonSticky to be
                     * false for the cursor, then Get.NEXT will create a new
                     * CursorImpl, and we must refresh the CursorImpl variable.
                     */
                    cursorImpl.latchBIN();
                    BIN bin = cursorImpl.getBIN();
                    try {
                        verifyDanglingLSNAndObsoleteRecordsOneSlot(
                            cursorImpl.getIndex(), bin, true);
                    } finally {
                        cursorImpl.releaseBIN();
                    }

                    if (databaseOperDuringBatchCheckHook != null) {
                        if (priDb != null) {
                            databaseOperDuringBatchCheckHook.doHook(priDb);
                        } else {
                            databaseOperDuringBatchCheckHook.doHook(secDb);
                        }
                    }

                    /*
                     * When verifying index or foreign constraint, we
                     * first READ-lock the secondary record and then try
                     * to non-blocking READ-lock the primary record. Using
                     * non-blocking is to avoid deadlocks, since we are locking
                     * in the reverse of the usual order.
                     *
                     * If the non-blocking lock fails with
                     * LockNotAvailableException, we will not be able to detect
                     * corruption and we should ignore this exception and
                     * continue verification. In this case the primary record
                     * is write-locked and is being modified by another thread,
                     * so it is OK to skip this verification step in this case.
                     * This is a compromise.
                     */
                    if (verifySecondary) {

                        /*
                         * When isCorrupted returns true we should stop
                         * verifying this db, just like when
                         * SecondaryIntegrityException is thrown.
                         */
                        if (DbInternal.isCorrupted(secDb)) {
                            return WalkDatabaseTreeResult.NO_MORE_RECORDS;
                        }

                        /* For secondary database, check index integrity. */
                        verifyIndex(
                            dbImpl, secDb, cursor, foundKey, foundData);

                        /* For secondary database, check foreign constraint. */
                        verifyForeignConstraint(
                            secDb, cursor, foundKey, dbCache);
                    }

                    /* For a primary database, verify data. */
                    if (verifyPrimaryDataRecords) {
                        verifyPrimaryData(dbImpl, priDb, cursor, dbCache);
                    }

                    /*
                     * Even if we do not need the data part, for example, for
                     * a secondary database which does not need to check
                     * index issue, we may still need the data part to locate
                     * the first record of next batch. So for the last record
                     * of this batch, we need to get the data part.
                     */
                    if (recordCount == batchSize - 1) {
                        foundData = new DatabaseEntry();
                    }

                    /*
                     * For the last record of each batch, we should do all
                     * above check. But we can NOT continue to get NEXT
                     * record.
                     */
                    if (recordCount == batchSize) {
                        break;
                    }

                    if (cursor.get(
                            foundKey, foundData, Get.NEXT,
                            NOLOCK_UNCHANGED) == null) {
                        return WalkDatabaseTreeResult.NO_MORE_RECORDS;
                    }

                } catch (StopDbVerificationException sve) {
                    /*
                     * StopDbVerificationException is thrown when
                     * 1. In verifyIndex, a SecondaryIntegrityException, which
                     *    is caused by index corruption, or a
                     *    IllegalStateException, which is caused by accessing
                     *    the closed primary database,  is caught.
                     * 2. In verifyForeignConstraint, the DatabaseImpl of the
                     *    foreign database can not be gotten or the
                     *    corresponding foreign record does not exist. 
                     * For both situations, we must stop verification of this
                     * db, but we should allow verification of other dbs to
                     * continue.
                     *
                     * No warning message needs to be logged here. For SIE,
                     * a message has already been logged when throwing SIE at
                     * the lower level.
                     */

                    return WalkDatabaseTreeResult.NO_MORE_RECORDS;

                } catch (LockConflictException|MoveToNextRecordException e) {
                    /*
                     * LockConflictException can be thrown by
                     * Cursor.get(CURRENT) with READLOCK_UNCHANGED, which
                     * could be due to a normal timeout. Just move the cursor
                     * to next record.
                     *
                     * MoveToNextRecordException indicates that
                     * cursor.get(CURRENT) returns null because the record has
                     * been deleted. Just move the cursor to next record.
                     *
                     * These two exceptions should not prevent verification of
                     * other records in the same db, so we simply ignore it.
                     *
                     * If the cursor.get(NEXT, NOLOCK_UNCHANGED) here catches
                     * an exception, which will not be LockConflictException
                     * because NOLOCK_UNCHANGED is used, this is an unknown
                     * and unexpected exception, we just handle it in the same
                     * way as the following RuntimeException.
                     *
                     * TODO: A verification statistic is needed to find out
                     * how many times this happens. This should be returned
                     * and logged at the end of verification.
                     */
                    try {
                        if (cursor.get(
                            foundKey, foundData, Get.NEXT,
                            NOLOCK_UNCHANGED) == null) {
                            return WalkDatabaseTreeResult.NO_MORE_RECORDS;
                        }
                    } catch (RuntimeException re) {
                        LoggerUtils.logMsg(
                            logger, envImpl, Level.SEVERE,
                            "Exception aborted btree verification of db " +
                                dbImpl.getDebugName() +
                                ", verification of all dbs will stop. " + e);

                        setStopVerifyFlag(true);
                        return WalkDatabaseTreeResult.NO_MORE_RECORDS;
                    }

                } catch (EnvironmentFailureException|
                         BtreeVerificationException e) {
                    throw e;

                } catch (RuntimeException e) {
                    /*
                     * Consider all other exceptions. e.g. the
                     * OperationFailureException thrown by cursor.get which
                     * is not LockConflicExceptionException, to be fatal to
                     * the entire verification process, since we don't know
                     * what caused them.
                     */
                    LoggerUtils.logMsg(
                        logger, envImpl, Level.SEVERE,
                        "Exception aborted btree verification of db " +
                            dbImpl.getDebugName() +
                            ", verification of all dbs will stop. " + e);

                    setStopVerifyFlag(true);
                    return WalkDatabaseTreeResult.NO_MORE_RECORDS;
                }
            }

            return new WalkDatabaseTreeResult(foundKey, foundData, false);

        } finally {
            cursorImpl.setTreeStatsAccumulator(null);
            tree.setTreeStatsAccumulator(null);
            EnvironmentImpl.decThreadLocalReferenceCount();

            cursor.close();
            locker.operationEnd();

            /* Release all cached DBs. */
            envImpl.getDbTree().releaseDbs(dbCache);
        }
    }

    private void verifyIndex(
        final DatabaseImpl dbImpl,
        final SecondaryDatabase secDb,
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry priKey)
        throws StopDbVerificationException {

        assert secDb != null;

        try {
            dbImpl.getEnv().getSecondaryAssociationLock().
                readLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(dbImpl.getEnv(), e);
        }

        try {
            final SecondaryAssociation secAssoc =
                DbInternal.getSecondaryAssociation(secDb);
            if (secAssoc.isEmpty()) {
                return;
            }

            final Database priDb = secAssoc.getPrimary(priKey);
            if (priDb == null) {
                return;
            }
            
            /*
             * We only need to check whether the primary record exists, we
             * do not need the data.
             */
            final DatabaseEntry priData = new DatabaseEntry();
            priData.setPartial(0, 0, true);

            /*
             * Currently the secondary record is locked. In order to avoid
             * deadlock, here we use the non-blocking lock. In order to
             * release the lock on the primary record, we create a new
             * Locker to acquire the lock and release the lock in finally
             * block.
             */
            final Locker locker =
                LockerFactory.getInternalReadOperationLocker(envImpl);
            locker.setDefaultNoWait(true);

            try {
                /*
                 * Cursor.readPrimaryAfterGet may return true or false, but for
                 * both cases, they do NOT indicate index corruption. Only
                 * throwing SecondaryIntegrityException means index corruption.
                 */
                DbInternal.readPrimaryAfterGet(
                    cursor, priDb, key, priKey, priData, LockMode.DEFAULT,
                    false /*secDirtyRead*/, false /*lockPrimaryOnly*/,
                    true /*verifyOnly*/, locker, secDb, secAssoc);
            } catch (LockNotAvailableException e) {
                /* Ignored -- see comment in walkDatabaseTree. */
            } finally {
                /* Release primary record lock. */
                locker.operationEnd();
            }
        } catch (SecondaryIntegrityException sie) {
            /*
             * Because currently the primary database is not marked as
             * CORRUPT, if we catch SIE here, it indicates that this SIE
             * was thrown by Cursor.readPrimaryAfterGet. Log related error
             * message here.
             */
            LoggerUtils.logMsg(
                logger, envImpl, Level.WARNING,
                "Secondary corruption is detected during btree " +
                    "verification. " + sie);

            throw new StopDbVerificationException();

        } catch (IllegalStateException ise) {
           /*
            * IllegalStateException is thrown when the primary database,
            * which is obtained via SecondaryAssociation.getPrimary, is
            * accessed after it is closed. For non-KVS apps, a secondary
            * database may only map to one unique primary database, and this
            * database will have already been closed. Therefore we just stop
            * the verification of the secondary database. In KVS, other primary
            * DBs (partitions) may still be open, but stopping verification of
            * the index is still acceptable.
            */
            throw new StopDbVerificationException();
        } finally {
            dbImpl.getEnv().getSecondaryAssociationLock().readLock().unlock();
        }
    }

    private void verifyForeignConstraint(        
        final SecondaryDatabase secDb,
        final Cursor cursor,
        final DatabaseEntry secKey,
        final Map<DatabaseId, DatabaseImpl> dbCache)
        throws StopDbVerificationException {

        assert secDb != null;

        final Database foreignDb =
            DbInternal.getPrivateSecondaryConfig(secDb).getForeignKeyDatabase();
        if (foreignDb == null) {
            return;
        }

        final DatabaseId foreignDbId;
        try {
            foreignDbId = DbInternal.getDbImpl(foreignDb).getId();
        } catch (IllegalStateException|OperationFailureException e) {
            throw new StopDbVerificationException();
        }

        envImpl.checkOpen();
        final DbTree dbTree = envImpl.getDbTree();
        final DatabaseImpl foreignDbImpl =
            dbTree.getDb(foreignDbId, -1, dbCache);

        if (foreignDbImpl == null || foreignDbImpl.isDeleted()) {
            /* This database is deleted. */
            throw new StopDbVerificationException();
        }

        /*
         * We only need to check whether the corresponding record exists
         * in the foreign database.
         */
        final DatabaseEntry tmpData = new DatabaseEntry();
        tmpData.setPartial(0, 0, true);     

        /* Use the non-blocking lock. */
        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);
        locker.setDefaultNoWait(true);

        try (final Cursor foreignCursor =
                 DbInternal.makeCursor(foreignDbImpl, locker, null,
                 true/*retainNonTxnLocks*/)) {

            final OperationResult result;
            try {
                result = foreignCursor.get(
                    secKey, tmpData, Get.SEARCH, READLOCK_UNCHANGED);
            } catch (LockNotAvailableException lnae) {
                /* Ignored -- see comment in walkDatabaseTree. */
                return;
            } finally {
                locker.operationEnd();;
            }

            /*
             * When a foreign key issue is found, we should first
             * generate SecondaryIntegrityException (rather than
             * ForeignConstraintException) to set the secondary database as
             * corrupt, and then throw StopDbVerificationException to cause
             * walkDatabaseTree to stop checking this secondary database.
             */
            if (result == null) {

                setSecondaryDbCorrupt(
                    secDb,
                    DbInternal.getCursorImpl(cursor).getLocker(),
                    "Secondary key does not exist in foreign database " +
                    DbInternal.getDbDebugName(foreignDb),
                    secKey,
                    null/*priKey*/,
                    DbInternal.getCursorImpl(cursor).getExpirationTime());

                throw new StopDbVerificationException();
            }
        }
    }

    private void verifyPrimaryData(
        final DatabaseImpl dbImpl,
        final Database priDb,
        final Cursor cursor,
        final Map<DatabaseId, DatabaseImpl> dbCache) {

        assert priDb != null;

        try {
            dbImpl.getEnv().getSecondaryAssociationLock().
                readLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(dbImpl.getEnv(), e);
        }

        try {
            final SecondaryAssociation secAssoc =
                DbInternal.getSecondaryAssociation(priDb);
            if (secAssoc.isEmpty()) {
                return;
            }

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            /*
             * 1. Read the primary data portion with Read lock.
             * 2. If null is returned, this primary record is deleted. We
             *    just ignore it.
             * 3. If non-null is returned, the cursor, which is used by
             *    walkDatabaseTree, owns a read lock on the primary record.
             * 4. If LockConflictException is thrown, then this primary
             *    record is locked. Just return.
             */
            try {
                if (cursor.get(
                    key, data, Get.CURRENT, READLOCK_UNCHANGED) == null) {
                    return;
                }
            } catch (LockConflictException e) {
                return;
            }

            /*
             * If checkSecondaryKeysExist cannot find the secondary record,
             * it will throw SIE. At that time, the cursor used in
             * checkSecondaryKeysExist is not at a meaningful slot, so we get
             * the expirationTime of the corresponding primary record here
             * and then pass it to checkSecondaryKeysExist.
             */
            for (final SecondaryDatabase secDb : secAssoc.getSecondaries(key)) {
                /*
                 * If the primary database is removed from the
                 * SecondaryAssociation, then we will skip checking any
                 * secondary database.
                 *
                 * Besides, if the primary database is removed from the
                 * SecondaryAssociation, secAssoc.getPrimary may throw
                 * exception. 
                 */
                try {
                    if (secAssoc.getPrimary(key) != priDb ) {
                        return;
                    }
                } catch (Exception e) {
                    return;
                }
                
                /*
                 * If the secondary database is in population phase, it
                 * may be reasonable that the BtreeVerifier can not find
                 * the corresponding secondary records for the checked
                 * primary record, because the primary record has not been
                 * populated to the secondary database.
                 */
                if (secDb.isIncrementalPopulationEnabled()) {
                    continue;
                }

                checkSecondaryKeysExist(
                    priDb, secDb, key, data, dbCache, secAssoc,
                    DbInternal.getCursorImpl(cursor).getExpirationTime());
            }
        } finally {
            dbImpl.getEnv().getSecondaryAssociationLock().readLock().unlock();
        }
    }

    private void checkSecondaryKeysExist(
        final Database priDb,
        final SecondaryDatabase secDb,
        DatabaseEntry priKey,
        DatabaseEntry priData,
        final Map<DatabaseId, DatabaseImpl> dbCache,
        final SecondaryAssociation secAssoc,
        final long expirationTime) {

        if (DbInternal.isCorrupted(secDb)) {
            /*
             * If the secondary database is set to be CORRUPT, then we will
             * not check this database. Just quick return.
             */
            return;
        }

        final SecondaryConfig secondaryConfig =
            DbInternal.getPrivateSecondaryConfig(secDb);
        final SecondaryKeyCreator keyCreator = secondaryConfig.getKeyCreator();
        final SecondaryMultiKeyCreator multiKeyCreator =
            secondaryConfig.getMultiKeyCreator();

        if (keyCreator == null && multiKeyCreator == null) {
            assert priDb.getConfig().getReadOnly();
            return;
        }

        final DatabaseId secDbId;
        try {
            secDbId = DbInternal.getDbImpl(secDb).getId(); 
        } catch (IllegalStateException|OperationFailureException e) {
            /*
             * We want to continue to check the following primary records,
             * so we just return.
             */
            return;
        }

        envImpl.checkOpen();
        final DbTree dbTree = envImpl.getDbTree();
        final DatabaseImpl secDbImpl = dbTree.getDb(secDbId, -1, dbCache);

        if (secDbImpl == null || secDbImpl.isDeleted()) {
            /*
             * We want to continue to check the following primary records,
             * so we just return.
             */
            return;
        }

        final String errMsg =
            "Secondary is corrupt: the primary record contains a " +
            "key that is not present in this secondary database.";

        if (keyCreator != null) {
            /* Each primary record may have a single secondary key. */
            assert multiKeyCreator == null;

            DatabaseEntry secKey = new DatabaseEntry();
            if (!keyCreator.createSecondaryKey(
                    secDb, priKey, priData, secKey)) {
                /* This primary record has no secondary keys. */
                return;
            }

            checkOneSecondaryKeyExists(
                secDb, secDbImpl, priKey, secKey, expirationTime,
                errMsg, priDb, secAssoc);

            return;
        }

        /* Each primary record may have multiple secondary keys. */

        /* Get secondary keys. */
        final Set<DatabaseEntry> secKeys = new HashSet<>();
        multiKeyCreator.createSecondaryKeys(
            secDb, priKey, priData, secKeys);
        if (secKeys.isEmpty()) {
            /* This primary record has no secondary keys. */
            return;
        }

        /*
         * Check each secondary key.
         */
        for (final DatabaseEntry secKey : secKeys) {

            if (!checkOneSecondaryKeyExists(
                secDb, secDbImpl, priKey, secKey, expirationTime,
                errMsg, priDb, secAssoc)) {
                return;
            }
        }
    }

    private boolean checkOneSecondaryKeyExists(
        final SecondaryDatabase secDb,
        final DatabaseImpl secDbImpl,
        final DatabaseEntry priKey,
        final DatabaseEntry secKey,
        final long expirationTime,
        final String errMsg,
        final Database priDb,
        final SecondaryAssociation secAssoc) {

        final Locker locker =
            LockerFactory.getInternalReadOperationLocker(envImpl);

        try (final Cursor checkCursor = DbInternal.makeCursor(
             secDbImpl, locker, null, false/*retainNonTxnLocks*/)) {

            if (checkCursor.get(secKey, priKey, Get.SEARCH_BOTH,
                NOLOCK_UNCHANGED) == null) {

                /* Same reason with that in verifyPrimaryData. */
                try {
                    if (secAssoc.getPrimary(priKey) != priDb ||
                        secDb.isIncrementalPopulationEnabled()) {
                        return false;
                    }
                } catch (Exception e) {
                    return false;
                }

                /*
                 * Can not find the corresponding secondary key.
                 * So an index issue exists.
                 */
                setSecondaryDbCorrupt(
                    secDb, locker, errMsg, secKey, priKey,
                    expirationTime);

                return false;
            }
        } finally {
            locker.operationEnd();
        }

        return true;
    }

    private void setSecondaryDbCorrupt(
        final SecondaryDatabase secDb,
        final Locker locker,
        final String errMsg,
        final DatabaseEntry secKey,
        final DatabaseEntry priKey,
        final long expirationTime) {

        if (!DbInternal.isCorrupted(secDb)) {

            final SecondaryIntegrityException sie =
                new SecondaryIntegrityException(
                    secDb, locker, errMsg, DbInternal.getDbDebugName(secDb),
                    secKey, priKey, expirationTime);

            LoggerUtils.logMsg(
                 logger, envImpl, Level.WARNING,
                 "Secondary corruption is detected during btree " +
                     "verification. " + sie);
        }
    }

    void setStopVerifyFlag(boolean val) {
        stopVerify = val;
    }

    public void setBtreeVerifyConfig(VerifyConfig btreeVerifyConfig) {
        this.btreeVerifyConfig = btreeVerifyConfig;
    }

    private class VerifierStatsAccumulator extends StatsAccumulator {
        VerifierStatsAccumulator(
            PrintStream progressStream,
            int progressInterval) {
            super(progressStream, progressInterval);
        }

        @Override
        public void verifyNode(Node node) {

            /*
             * Exceptions thrown by basicBtreeVerify should invalidate the
             * env, so we cannot simply log the error and continue here. We
             * must allow the exception to be thrown upwards.
             */
            basicBtreeVerify(node);
        }
    }

    /*
     * StopDbVerificationException is thrown when
     * 1. In verifyIndex, a SecondaryIntegrityException, which
     *    is caused by index corruption, or a
     *    IllegalStateException, which is caused by accessing
     *    the closed primary database,  is caught.
     * 2. In verifyForeignConstraint, the DatabaseImpl of the
     *    foreign database cannot be gotten or the
     *    corresponding foreign record does not exist. 
     * This exception causes walkDatabaseTree stop checking the
     * secondary database.
     */
    private static class StopDbVerificationException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /*
     * Thrown in walkDatabaseTree to indicate that cursor.get(CURRENT) returns
     * null because the record has been deleted. Just let the cursor move to
     * next record.
     */
    private static class MoveToNextRecordException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Thrown during btree verification if a persistent btree corruption is
     * detected.
     *
     * This is an internal exception and ideally it should be a checked
     * exception(not a RuntimeException) so that we can confirm statically
     * that it is always handled. But this would require changes to
     * CursorImpl.WithCursor interface, so for now a runtime exception is used.
     */
    private static class BtreeVerificationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public BtreeVerificationException(final String message) {
            super(message);
        }

        public BtreeVerificationException(
            final String message,
            final Throwable cause) {

            super(message);
            initCause(cause);
        }
    }
}
