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

package com.sleepycat.je.cleaner;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.VLSN;

/**
 * The FileProtector is primarily responsible for protecting files from being
 * deleted due to log cleaning, when they are needed for other purposes. As
 * such it is a gatekeeper for reading files. In addition it maintains certain
 * metadata:
 *  - the size of each file, which is needed for calculating disk usage;
 *  - the first and last VLSNs in each reserved file, which are needed for
 *    maintaining the VLSNIndex;
 *  - the total size of active and reserved files, for disk usage statistics.
 *
 * All files are in three categories:
 * + Active: readable by all. The minimum set of files needed to function.
 * + Reserved: should be read only by feeders and low level utilities.
 *   They have been cleaned and will not become active again. They can be
 *   "condemned" and deleted if they are not being read.
 * + Condemned: not readable and effectively invisible. They will be deleted
 *   ASAP. They will not become active or reserved again. A file is typically
 *   in the Condemned category for a very short time, while being deleted.
 *
 * Reserved files can be temporarily protected, i.e., prevented from being
 * condemned and deleted. Only reserved files can be condemned and deleted. All
 * active files are implicitly protected, but are also protected explicitly by
 * consumers because they may become reserved while being consumed.
 *
 * Consumers of the File Protection Service
 * ----------------------------------------
 *
 * + DiskOrderedScanner (DiskOrderedCursor and DatabaseCount)
 *   - Protects all currently active files. By currently active we mean active
 *     at the time they are protected. If they become reserved during the
 *     scan, they must continue to be protected.
 *   - Also protects any new files written during the scan.
 *
 * + DbBackup:
 *   - Protects-and-lists all currently active files, or a subset of the
 *     currently active files in the case of an incremental backup.
 *   - Provides API to remove files from protected set as they are copied, to
 *     allow file deletion.
 *
 * + NetworkRestore:
 *   - Protects-and-lists all currently active files using DbBackup.
 *   - Also protects the two newest reserved files at the time that the active
 *     files are protected.
 *   - Removes files from protected set as they are copied, to allow file
 *     deletion.
 *
 * + Syncup:
 *   - Protects all files (active and reserved) in an open ended range starting
 *     with the file of the VLSNIndex range start. Barren files (with no
 *     replicable entries) are not protected.
 *
 * + Feeder:
 *   - Protects all files (active and reserved) in an open ended range starting
 *     with the file of the current VLSN. Barren files (with no replicable
 *     entries) are not protected.
 *   - Advances lower bound of protected range as VLSN advances, to allow file
 *     deletion.
 *
 * + Cleaner:
 *   - Transforms active files into reserved files after cleaning them.
 *   - Condemns and deletes reserved files to honor disk limits. Truncates head
 *     of VLSNIndex when necessary to stay with disk thresholds.
 *
 * Syncup, Feeders and the VLSNIndex
 * ---------------------------------
 * During syncup, a ProtectedFileRange is used to protect files in the entire
 * range of the VLSNIndex. Syncup must also prevent head truncation of the
 * VLSNIndex itself because the file readers (used by both master and replica)
 * use the VLSNIndex to position in the file at various times.
 *
 * A feeder file reader also protect all files from the current VLSN onward
 * using a ProtectedFileRange. We rely on syncup to initialize the feeder's
 * ProtectedFileRange safely, while the syncup ProtectedFileRange is in effect.
 * The feeder reader will advance the lower bound of its ProtectedFileRange as
 * it reads forward to allow files to be deleted. It also uses the VLNSIndex to
 * skip over gaps in the file, although it is unclear whether this is really
 * necessary.
 *
 * Therefore the syncup and feeder ProtectedFileRanges are special in that
 * they also prevent head truncation of the VLSNIndex.
 *
 * The cleaner truncates the head of the VLSNIndex to allow deletion of files
 * when necessary to stay within disk usage limits. This truncation must not
 * be allowed when a syncup is in progress, and must not be allowed to remove
 * the portion of the VLSN range used by a feeder. This is enforced using a
 * special ProtectedFileRange (vlsnIndexRange) that protects the entire
 * VLSNIndex range. The vlsnIndexRange is advanced when necessary to delete
 * files that it protects, but only if those files are not protected by syncup
 * or feeders. See {@link #checkVLSNIndexTruncation}, {@link
 * com.sleepycat.je.rep.vlsn.VLSNTracker#tryTruncateFromHead} and {@link
 * com.sleepycat.je.rep.vlsn.VLSNTracker#protectRangeHead}.
 *
 * We takes pains to avoid synchronizing on FileProtector while truncating the
 * VLSNIndex head, which is a relatively expensive operation. (The
 * FileProtector is meant to be used by multiple threads without a lot of
 * blocking and should perform only a fairly small amount of work while
 * synchronized.) The following approach is used to truncate the VLSNIndex head
 * safely:
 *
 * -- To prevent disk usage limit violations, Cleaner.manageDiskUsage first
 *    tries to delete reserved files without truncating the VLSNIndex. If this
 *    is not sufficient, it then tries to truncate the VLSNIndex head. If the
 *    VLSNIndex head can be truncated, then it tries again to delete reserved
 *    files, since more files should then be unprotected.
 *
 * -- VLSNTracker synchronization is used to protect the VLSNIndex range. The
 *    vlsnIndexRange ProtectedFileRange is advanced only while synchronized on
 *    the VLSNTracker.
 *
 * -- VLSNTracker.tryTruncateFromHead (which is synchronized) calls
 *    FileProtector.checkVLSNIndexTruncation to determine where to truncate the
 *    index. Reserved files can be removed from the VLSNIndex range only if
 *    they are not protected by syncup and feeders.
 *
 * -- The VLSNTracker range is then truncated, and the vlsnIndexRange is
 *    advanced to allow file deletion, all while synchronized on the tracker.
 *
 * -- When a syncup starts, it adds a ProtectedFileRange with the same
 *    startFile as the vlsnIndexRange. This is done while synchronized on the
 *    VLSNTracker and it prevents the vlsnIndexRange from advancing during the
 *    syncup.
 *
 * -- When a syncup is successful, on the master the Feeder is initialized and
 *    it adds a ProtectedFileRange to protect the range of the VLSNIndex that
 *    it is reading. This is done BEFORE removing the ProtectedFileRange that
 *    was added at the start of the syncup. This guarantees that the files and
 *    VLSNIndex range used by the feeder will not be truncated/deleted.
 *
 * Note that the special vlsnIndexRange ProtectedFileRange is excluded from
 * LogSizeStats to avoid confusion and because this ProtectedFileRange does not
 * ultimately prevent VLSNIndex head truncation or file deletion.
 *
 * Barren Files
 * ------------
 * Files with no replicable entries do not need to be protected by syncup or
 * feeeders. See {@link #protectFileRange(String, long, boolean)}. Barren files
 * may be created when cleaning is occurring but app writes are not, for
 * example, when recovering from a cache size configuration error. In this
 * situation it is important to delete the barren files to reclaim disk space.
 *
 * Such "barren" files are identified by having null begin/end VLSNs. The
 * begin/end VLSNs for a file are part of the cleaner metadata that is
 * collected when the cleaner processes a file. These VLSNs are for replicable
 * entries only, not migrated entries that happen to contain a VLSN.
 */
public class FileProtector {

    /* Prefixes for ProtectedFileSet names. */
    public static final String BACKUP_NAME = "Backup";
    public static final String DATABASE_COUNT_NAME = "DatabaseCount";
    public static final String DISK_ORDERED_CURSOR_NAME = "DiskOrderedCursor";
    public static final String FEEDER_NAME = "Feeder";
    public static final String SYNCUP_NAME = "Syncup";
    public static final String VLSN_INDEX_NAME = "VLSNIndex";
    public static final String NETWORK_RESTORE_NAME = "NetworkRestore";

    private static class ReservedFileInfo {
        long size;
        VLSN endVLSN;

        ReservedFileInfo(long size, VLSN endVLSN) {
            this.size = size;
            this.endVLSN = endVLSN;
        }
    }

    private final EnvironmentImpl envImpl;

    /* Access active files only via getActiveFiles. */
    private final NavigableMap<Long, Long> activeFiles = new TreeMap<>();

    private final NavigableMap<Long, ReservedFileInfo> reservedFiles =
        new TreeMap<>();

    private final NavigableMap<Long, Long> condemnedFiles = new TreeMap<>();

    private final Map<String, ProtectedFileSet> protectedFileSets =
        new HashMap<>();

    /* Is null if the env is not replicated. */
    private ProtectedFileRange vlsnIndexRange;

    FileProtector(final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
    }

    private void addFileProtection(ProtectedFileSet pfs) {

        if (protectedFileSets.putIfAbsent(pfs.getName(), pfs) != null) {

            throw EnvironmentFailureException.unexpectedState(
                "ProtectedFileSets already present name=" + pfs.getName());
        }
    }

    /**
     * Removes protection by the given ProtectedFileSet to allow files to be
     * deleted.
     */
    public synchronized void removeFileProtection(ProtectedFileSet pfs) {

        final ProtectedFileSet oldPfs =
            protectedFileSets.remove(pfs.getName());

        if (oldPfs == null) {
            throw EnvironmentFailureException.unexpectedState(
                "ProtectedFileSet not found name=" + pfs.getName());
        }

        if (oldPfs != pfs) {
            throw EnvironmentFailureException.unexpectedState(
                "ProtectedFileSet mismatch name=" + pfs.getName());
        }
    }

    /**
     * Calls {@link #protectFileRange(String, long, boolean)} passing false for
     * protectVlsnIndex.
     */
    public synchronized ProtectedFileRange protectFileRange(
        final String name,
        final long rangeStart) {

        return protectFileRange(name, rangeStart, false);
    }

    /**
     * Returns a ProtectedFileRange that protects files with numbers GTE a
     * lower bound. The upper bound is open ended. The protectVlsnIndex param
     * should be true for feeder/syncup file protection only.
     *
     * @param rangeStart is the first file to be protected in the range.
     *
     * @param protectVlsnIndex is whether to prevent the VLSNIndex head from
     * advancing, which also implies that barren files (with no replicable
     * entries) are not protected.
     */
    public synchronized ProtectedFileRange protectFileRange(
        final String name,
        final long rangeStart,
        final boolean protectVlsnIndex) {

        final ProtectedFileRange fileRange =
            new ProtectedFileRange(name, rangeStart, protectVlsnIndex);

        addFileProtection(fileRange);
        return fileRange;
    }

    /**
     * Calls {@link #protectActiveFiles(String, int, boolean)} passing 0 for
     * nReservedFiles and true for protectNewFiles.
     */
    public synchronized ProtectedActiveFileSet protectActiveFiles(
        final String name) {

        return protectActiveFiles(name, 0, true);
    }

    /**
     * Returns a ProtectedActiveFileSet that protects all files currently
     * active at the time of construction. These files are protected even if
     * they later become reserved. Note that this does not include the last
     * file at the time of construction. Additional files can also be
     * protected -- see params.
     *
     * @param nReservedFiles if greater than zero, this number of the newest
     * (highest numbered) reserved files are also protected.
     *
     * @param protectNewFiles if true, the last file and any new files created
     * later are also protected.
     */
    public synchronized ProtectedActiveFileSet protectActiveFiles(
        final String name,
        final int nReservedFiles,
        final boolean protectNewFiles) {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();

        final NavigableSet<Long> protectedFiles =
            new TreeSet<>(activeFiles.keySet());

        if (nReservedFiles > 0) {
            int n = nReservedFiles;
            for (Long file : reservedFiles.descendingKeySet()) {
                protectedFiles.add(file);
                n -= 1;
                if (n <= 0) {
                    break;
                }
            }
        }

        final Long rangeStart = protectNewFiles ?
            (protectedFiles.isEmpty() ? 0 : (protectedFiles.last() + 1)) :
            null;

        final ProtectedActiveFileSet pfs =
            new ProtectedActiveFileSet(name, protectedFiles, rangeStart);

        addFileProtection(pfs);
        return pfs;
    }

    /**
     * Get new file info lazily to prevent synchronization and work in the CRUD
     * code path when a new files is added.
     */
    private synchronized NavigableMap<Long, Long> getActiveFiles() {

        final FileManager fileManager = envImpl.getFileManager();

        /*
         * Add all existing files when the env is first opened (except for the
         * last file -- see below). This is a relatively expensive but one-time
         * initialization.
         */
        if (activeFiles.isEmpty()) {

            final Long[] files = fileManager.getAllFileNumbers();

            for (int i = 0; i < files.length - 1; i++) {
                final long file = files[i];

                final File fileObj =
                    new File(fileManager.getFullFileName(file));

                activeFiles.put(file, fileObj.length());
            }
        }

        /*
         * Add new files that have appeared. This is very quick, because no
         * synchronization is required to get the last file number. Do not
         * add the last file, since its length may still be changing.
         */
        final long lastFile = DbLsn.getFileNumber(fileManager.getNextLsn());

        final long firstNewFile = activeFiles.isEmpty() ?
            0 : activeFiles.lastKey() + 1;

        for (long file = firstNewFile; file < lastFile; file += 1) {

            final File fileObj =
                new File(fileManager.getFullFileName(file));

            /* New files should be active before being reserved and deleted. */
            if (!fileObj.exists() && !envImpl.isMemOnly()) {
                throw EnvironmentFailureException.unexpectedState(
                    "File 0x" + Long.toHexString(file) +
                    " lastFile=" + Long.toHexString(lastFile));
            }

            activeFiles.put(file, fileObj.length());
        }

        return activeFiles;
    }

    /**
     * Moves a file from active status to reserved status.
     */
    synchronized void reserveFile(Long file, VLSN endVLSN) {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();

        final Long size = activeFiles.remove(file);

        if (size == null) {
            throw EnvironmentFailureException.unexpectedState(
                "Only active files (not the last file) may be" +
                    " cleaned/reserved file=0x" + Long.toHexString(file) +
                    " exists=" + envImpl.getFileManager().isFileValid(file) +
                    " reserved=" + reservedFiles.containsKey(file) +
                    " nextLsn=" + DbLsn.getNoFormatString(
                        envImpl.getFileManager().getNextLsn()));
        }

        final ReservedFileInfo info = new ReservedFileInfo(size, endVLSN);
        final ReservedFileInfo prevInfo = reservedFiles.put(file, info);
        assert prevInfo == null;
    }

    /**
     * Returns the number of active files, including the last file.
     */
    synchronized int getNActiveFiles() {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();
        int count = getActiveFiles().size();

        if (activeFiles.isEmpty() ||
            activeFiles.lastKey() <
                envImpl.getFileManager().getCurrentFileNum()) {
            count += 1;
        }

        return count;
    }

    /**
     * Returns the number of reserved files.
     */
    synchronized int getNReservedFiles() {
        return reservedFiles.size();
    }

    /**
     * Returns a copy of the reserved files along with the total size.
     */
    public synchronized Pair<Long, NavigableSet<Long>> getReservedFileInfo() {
        long size = 0;
        for (final ReservedFileInfo info : reservedFiles.values()) {
            size += info.size;
        }
        return new Pair<>(size, new TreeSet<>(reservedFiles.keySet()));
    }

    /**
     * Returns whether the given file is active, including the last file
     * whether or not it has been created on disk yet. If false is returned,
     * the file is reserved or deleted.
     */
    synchronized boolean isActiveOrNewFile(Long file) {

        final NavigableMap<Long, Long> activeFiles = getActiveFiles();

        return activeFiles.isEmpty() ||
            file > activeFiles.lastKey() ||
            activeFiles.containsKey(file);
    }

    /**
     * Returns whether the given file is in the reserved file set.
     */
    synchronized boolean isReservedFile(Long file) {
        return reservedFiles.containsKey(file);
    }

    /**
     * Returns a previously condemned file or condemns the oldest unprotected
     * reserved file and returns it. If the returned file cannot be deleted by
     * the caller, {@link #putBackCondemnedFile} should be called so the file
     * deletion can be retried later.
     *
     * @param fromFile the lowest file number to return. Used to iterate over
     * reserved files that are protected.
     *
     * @return {file, size} pair or null if a condemned file is not available.
     */
    synchronized Pair<Long, Long> takeCondemnedFile(long fromFile) {

        if (!condemnedFiles.isEmpty()) {
            final Long file = condemnedFiles.firstKey();
            final Long size = condemnedFiles.remove(file);
            return new Pair<>(file, size);
        }

        if (reservedFiles.isEmpty()) {
            return null;
        }

        fileLoop:
        for (final Map.Entry<Long, ReservedFileInfo> entry :
                reservedFiles.tailMap(fromFile).entrySet()) {

            final Long file = entry.getKey();
            final ReservedFileInfo info = entry.getValue();

            for (final ProtectedFileSet pfs : protectedFileSets.values()) {
                if (pfs.isProtected(file, info)) {
                    continue fileLoop;
                }
            }

            reservedFiles.remove(file);
            return new Pair<>(file, info.size);
        }

        return null;
    }

    /**
     * Puts back a condemned file after a file returned by {@link
     * #takeCondemnedFile} could not be deleted.
     */
    synchronized void putBackCondemnedFile(Long file, Long size) {
        final Long oldSize = condemnedFiles.put(file, size);
        assert oldSize == null;
    }

    static class LogSizeStats {
        final long activeSize;
        final long reservedSize;
        final long protectedSize;
        final Map<String, Long> protectedSizeMap;

        LogSizeStats(final long activeSize,
                     final long reservedSize,
                     final long protectedSize,
                     final Map<String, Long> protectedSizeMap) {
            this.activeSize = activeSize;
            this.reservedSize = reservedSize;
            this.protectedSize = protectedSize;
            this.protectedSizeMap = protectedSizeMap;
        }
    }

    /**
     * Returns sizes occupied by active, reserved and protected files.
     */
    synchronized LogSizeStats getLogSizeStats() {

        /* Calculate active size. */
        final NavigableMap<Long, Long> activeFiles = getActiveFiles();
        long activeSize = 0;

        for (final long size : activeFiles.values()) {
            activeSize += size;
        }

        /* Add size of last file, which is not included in activeFiles. */
        final long lastFileNum = activeFiles.isEmpty() ?
            0 : activeFiles.lastKey() + 1;

        final File lastFile = new File(
            envImpl.getFileManager().getFullFileName(lastFileNum));

        if (lastFile.exists()) {
            activeSize += lastFile.length();
        }

        /* Calculate reserved and protected sizes. */
        long reservedSize = 0;
        long protectedSize = 0;
        final Map<String, Long> protectedSizeMap = new HashMap<>();

        for (final Map.Entry<Long, ReservedFileInfo> entry :
                reservedFiles.entrySet()) {

            final Long file = entry.getKey();
            final ReservedFileInfo info = entry.getValue();
            reservedSize += info.size;
            boolean isProtected = false;

            for (final ProtectedFileSet pfs : protectedFileSets.values()) {

                if (pfs == vlsnIndexRange || !pfs.isProtected(file, info)) {
                    continue;
                }

                isProtected = true;

                protectedSizeMap.compute(
                    pfs.getName(),
                    (k, v) -> ((v != null) ? v : 0) + info.size);
            }

            if (isProtected) {
                protectedSize += info.size;
            }
        }

        return new LogSizeStats(
            activeSize, reservedSize, protectedSize, protectedSizeMap);
    }

    /**
     * Sets the ProtectedFileRange that protects files in VLSNIndex range
     * from being deleted. The range start is changed during VLSNIndex
     * initialization and when the head of the index is truncated. It is
     * changed while synchronized on VLSNTracker so that changes to the
     * range and changes to the files it protects are made atomically. This
     * is important for
     * {@link com.sleepycat.je.rep.vlsn.VLSNTracker#protectRangeHead}.
     */
    public void setVLSNIndexProtectedFileRange(ProtectedFileRange pfs) {
        vlsnIndexRange = pfs;
    }

    /**
     * Determines whether the VLSNIndex ProtectedFileRange should be advanced
     * to reclaim bytesNeeded. This is possible if one or more reserved files
     * are not protected by syncup and feeders. The range of files to be
     * truncated must be at the head of the ordered set of reserved files, and
     * the highest numbered file must contain a VLSN so we know where to
     * truncate the VLSNIndex.
     *
     * @param bytesNeeded the number of bytes we need to free.
     *
     * @param preserveVLSN is the boundary above which the VLSN range may not
     * advance. The deleteEnd returned will be less than preserveVLSN.
     *
     * @return {deleteEnd, deleteFileNum} pair if the protected file range
     * should be advanced, or null if advancing is not currently possible.
     *  -- deleteEnd is the last VLSN to be truncated.
     *  -- deleteFileNum the file having deleteEnd as its last VLSN.
     */
    public synchronized Pair<VLSN, Long> checkVLSNIndexTruncation(
        final long bytesNeeded,
        final VLSN preserveVLSN) {

        /*
         * Determine how many reserved files we need to delete, and find the
         * last file/VLSN in that set of files, which is the truncation point.
         */
        VLSN truncateVLSN = VLSN.NULL_VLSN;
        long truncateFile = -1;
        long deleteBytes = 0;

        fileLoop:
        for (final Map.Entry<Long, ReservedFileInfo> entry :
                reservedFiles.entrySet()) {

            final Long file = entry.getKey();
            final ReservedFileInfo info = entry.getValue();

            for (final ProtectedFileSet pfs : protectedFileSets.values()) {

                if (pfs == vlsnIndexRange || !pfs.protectVlsnIndex) {
                    continue;
                }

                if (pfs.isProtected(file, info)) {
                    break fileLoop;
                }
            }

            final VLSN lastVlsn = info.endVLSN;

            if (!lastVlsn.isNull()) {
                if (lastVlsn.compareTo(preserveVLSN) > 0) {
                    break;
                }
                truncateVLSN = lastVlsn;
                truncateFile = file;
            }

            deleteBytes += info.size;

            if (deleteBytes >= bytesNeeded) {
                break;
            }
        }

        return truncateVLSN.isNull() ? null :
            new Pair<>(truncateVLSN, truncateFile);
    }

    /**
     * A ProtectedFileSet is used to prevent a set of files from being deleted.
     * Implementations must meet two special requirements:
     *
     * 1. After a ProtectedFileSet is added using {@link #addFileProtection},
     * its set of protected files (the set for which {@link #isProtected}
     * returns true) may only be changed by shrinking the set. Files may not be
     * added to the set of protected files. (One exception is that newly
     * created files are effectively to a file set defined as an opened ended
     * range.)
     *
     * 2. Shrinking the protected set can be done without synchronization on
     * FileProtector. However, implementations should ensure that changes made
     * in one thread are visible to all threads.
     *
     * The intention is to allow protecting a set of files that are to be
     * processed in some way, and allow easily shrinking this set as the files
     * are processed, so that the processed files may be deleted. Changes to
     * the protected set should be visible to all threads so that periodic disk
     * space reclamation tasks can delete unprotected files ASAP. {@link
     * ProtectedFileRange} is a simple class that meets these requirements.
     */
    public static abstract class ProtectedFileSet {

        private final String name;
        private final boolean protectVlsnIndex;

        private ProtectedFileSet(final String name,
                                 final boolean protectVlsnIndex) {
            this.name = name;
            this.protectVlsnIndex = protectVlsnIndex;
        }

        /**
         * Identifies protecting entity, used in LogSizeStats. Must be unique
         * across all file sets added to the FileProtector.
         */
        private String getName() {
            return name;
        }

        /**
         * Whether the given file is protected.
         */
        abstract boolean isProtected(Long file, ReservedFileInfo info);

        @Override
        public String toString() {
            return "ProtectedFileSet:" + name;
        }
    }

    /**
     * A ProtectedFileSet created using {@link #protectFileRange}.
     *
     * Protection may be removed dynamically to allow file deletion using
     * {@link #advanceRange}. The current lower bound can be obtained using
     * {@link #getRangeStart()}.
     */
    public static class ProtectedFileRange extends ProtectedFileSet {

        private volatile long rangeStart;
        private final boolean protectBarrenFiles;

        ProtectedFileRange(
            final String name,
            final long rangeStart,
            final boolean protectVlsnIndex) {

            super(name, protectVlsnIndex);
            this.rangeStart = rangeStart;
            protectBarrenFiles = !protectVlsnIndex;
        }

        @Override
        boolean isProtected(final Long file,
                            final ReservedFileInfo info) {

            return file >= rangeStart &&
                (protectBarrenFiles || !info.endVLSN.isNull());
        }

        /**
         * Returns the current rangeStart. This method is not synchronized and
         * rangeStart is volatile to allow checking this value without
         * blocking.
         */
        public long getRangeStart() {
            return rangeStart;
        }

        /**
         * Moves the lower bound of the protected file range forward. Used to
         * allow file deletion as protected files are processed.
         */
        public synchronized void advanceRange(final long rangeStart) {

            if (rangeStart < this.rangeStart) {
                throw EnvironmentFailureException.unexpectedState(
                    "Attempted to advance to a new rangeStart=0x" +
                        Long.toHexString(rangeStart) +
                        " that precedes the old rangeStart=0x" +
                        Long.toHexString(this.rangeStart));
            }

            this.rangeStart = rangeStart;
        }
    }

    /**
     * A ProtectedFileSet created using {@link #protectActiveFiles}.
     *
     * Protection may be removed dynamically to allow file deletion using
     * {@link #truncateHead(long)}, {@link #truncateTail(long)} and
     * {@link #removeFile(Long)}. A copy of the currently protected files can
     * be obtained using {@link #getProtectedFiles()}.
     */
    public static class ProtectedActiveFileSet extends ProtectedFileSet {

        private NavigableSet<Long> protectedFiles;
        private Long rangeStart;

        ProtectedActiveFileSet(
            final String name,
            final NavigableSet<Long> protectedFiles,
            final Long rangeStart) {

            super(name, false /*protectVlsnIndex*/);
            this.protectedFiles = protectedFiles;
            this.rangeStart = rangeStart;
        }

        @Override
        synchronized boolean isProtected(final Long file,
                                         final ReservedFileInfo info) {

            return (rangeStart != null && file >= rangeStart) ||
                protectedFiles.contains(file);
        }

        /**
         * Returns a copy of the currently protected files, not including any
         * new files.
         */
        public synchronized NavigableSet<Long> getProtectedFiles() {
            return new TreeSet<>(protectedFiles);
        }

        /**
         * Removes protection for files GT lastProtectedFile. Protection of
         * new files is not impacted.
         */
        public synchronized void truncateTail(long lastProtectedFile) {
            protectedFiles = protectedFiles.headSet(lastProtectedFile, true);
        }

        /**
         * Removes protection for files LT firstProtectedFile. Protection of
         * new files is not impacted.
         */
        public synchronized void truncateHead(long firstProtectedFile) {
            protectedFiles = protectedFiles.tailSet(firstProtectedFile, true);
        }

        /**
         * Removes protection for a given file.
         */
        public synchronized void removeFile(final Long file) {

            protectedFiles.remove(file);

            /*
             * This only works if protected files are removed in sequence, but
             * that's good enough -- new files will rarely need to be deleted.
             */
            if (file.equals(rangeStart)) {
                rangeStart += 1;
            }
        }
    }

    /**
     * For debugging.
     */
    @SuppressWarnings("unused")
    synchronized void verifyFileSizes() {
        final FileManager fm = envImpl.getFileManager();
        final Long[] numArray = fm.getAllFileNumbers();
        final NavigableMap<Long, Long> activeFiles = getActiveFiles();
        for (int i = 0; i < numArray.length - 1; i++) {
            final Long n = numArray[i];
            final long trueSize = new File(fm.getFullFileName(n)).length();
            if (activeFiles.containsKey(n)) {
                final long activeSize = activeFiles.get(n);
                if (activeSize != trueSize) {
                    System.out.format(
                        "active file %,d size %,d but true size %,d %n",
                        n, activeSize, trueSize);
                }
            } else if (reservedFiles.containsKey(n)) {
                final long reservedSize = reservedFiles.get(n).size;
                if (reservedSize != trueSize) {
                    System.out.format(
                        "reserved file %,d size %,d but true size %,d %n",
                        n, reservedSize, trueSize);
                }
            } else {
                System.out.format(
                    "true file %x size %,d missing in FileProtector%n",
                    n, trueSize);
            }
        }
    }
}
