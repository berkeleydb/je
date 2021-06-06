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

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;

/**
 * Contains methods for calculating utilization and for selecting files to
 * clean.
 *
 * Note that we do clean files that are protected from deletion by HA/DataSync.
 * If we did not clean them and a large number of files were to become
 * unprotected at once, a large amount of log cleaning may suddenly be
 * necessary.  Cleaning the files avoids this.  Better still would be to delete
 * the metadata, but that would require writing a log entry to indicate the
 * file is ready to be deleted, to avoid cleaning from scratch after a crash.
 * [#16643] [#19221]
 *
 * Historical note: Prior to JE 6.0, LN utilization adjustments were needed
 * because the LN last logged size was not stored in the BIN [#18633].
 * Originally in JE 5, the corrected average LN size was used to adjust
 * utilization. This was changed later in JE 5 to a correction factor since
 * different log files may have different average LN sizes [#21106]. Then in
 * JE 6.0 the last logged size was added to the BIN, the default for
 * {@link com.sleepycat.je.EnvironmentConfig#CLEANER_ADJUST_UTILIZATION} was
 * changed to false and a warning was added that the feature will be removed in
 * the future [#22275]. Finally in JE 6.3 the LN adjustment code and data in
 * CheckpointEnd were removed and the parameter was deprecated [#24090].
 *
 * Unlike with LNs, we do not store the last logged size of INs, so their
 * obsolete size is computed as an average and this has the potential to cause
 * over/under-cleaning. This problem is not known to occur, but if there are
 * over/under-cleaning problems we should examine the recalculated info that is
 * output as part of the CleanerRun INFO message.
 *
 * === Expired Data and Utilization ===
 *
 * Per-file histograms are calculated by the {@link ExpirationTracker} and
 * stored in an internal database and cache by the {@link ExpirationProfile}.
 * The histograms are used to calculate the expired bytes for a file at a
 * particular time. Since obsolete (not expired) data can overlap with expired
 * data, upper and lower bounds for overall utilization are determined. When
 * the lower bound is below minUtilization, cleaning occurs.
 *
 * The file that has the lowest average utilization (midway between its upper
 * and lower bounds) is selected for cleaning. If the file's upper and lower
 * bounds are not close together (or the same), and the upper bound is above a
 * threshold, then two-pass cleaning is performed. See
 * {@link EnvironmentParams#CLEANER_TWO_PASS_GAP} and
 * {@link EnvironmentParams#CLEANER_TWO_PASS_THRESHOLD}.
 *
 * The first pass of two-pass cleaning reads the file but doesn't do any real
 * cleaning (no side effects). If this pass finds that the true utilization of
 * the file is above the threshold (the same threshold as above), then cleaning
 * does not take place and instead the histogram is updated. In this case the
 * obsolete and expired data (in the old histogram) overlap, and it is the
 * overlap that caused utilization to be estimated incorrectly. The new/updated
 * histogram is calculated such that there is no overlap, improving utilization
 * accuracy. If the first pass finds that true utilization is below the
 * threshold, then normal cleaning (pass two) occurs. Two-pass cleaning
 * protects against "over cleaning".
 *
 * The use of the overall utilization lower bound to drive cleaning is
 * considered sufficient to protect against "under cleaning". Therefore, a disk
 * space threshold is unnecessary.
 *
 * Gradual expiration is used to prevent cleaning spikes on day or hour
 * boundaries. For purposes of driving cleaning, the utilization lower bound is
 * calculated by distributing the bytes that expired in the current day/hour
 * period evenly over that day/hour.
 */
public class UtilizationCalculator implements EnvConfigObserver {

    private final EnvironmentImpl env;
    private final Cleaner cleaner;
    private final Logger logger;
    private FilesToMigrate filesToMigrate;
    private volatile int currentMinUtilization = -1;
    private volatile int currentMaxUtilization = -1;
    private volatile int predictedMinUtilization = -1;
    private volatile int predictedMaxUtilization = -1;

    UtilizationCalculator(EnvironmentImpl env, Cleaner cleaner) {
        this.env = env;
        this.cleaner = cleaner;
        logger = LoggerUtils.getLogger(getClass());
        filesToMigrate = new FilesToMigrate(env);
        env.addConfigObserver(this);
    }

    int getCurrentMinUtilization() {
        return currentMinUtilization;
    }

    int getCurrentMaxUtilization() {
        return currentMaxUtilization;
    }

    int getPredictedMinUtilization() {
        return predictedMinUtilization;
    }

    int getPredictedMaxUtilization() {
        return predictedMaxUtilization;
    }

    /**
     * Returns the best file that qualifies for cleaning or probing, or null
     * if no file qualifies.
     *
     * This method is called by FileSelector and synchronization order is:
     * 1-FileSelector, 2-UtilizationCalculator, 3-ExpirationProfile.
     *
     * @param fileSummaryMap the map containing file summary info.
     *
     * @param forceCleaning is true to always select a file, even if its
     * utilization is above the minimum utilization threshold.
     *
     * @return {file number, required utilization for 2-pass cleaning},
     * or null if no file qualifies for cleaning.
     */
    synchronized Pair<Long, Integer> getBestFile(
        final SortedMap<Long, FileSummary> fileSummaryMap,
        final boolean forceCleaning) {

        /* Paranoia.  There should always be 1 file. */
        if (fileSummaryMap.size() == 0) {
            LoggerUtils.logMsg(logger, env, Level.SEVERE,
                               "Can't clean, map is empty.");
            return null;
        }

        final FileSelector fileSelector = cleaner.getFileSelector();
        final Set<Long> inProgressFiles = fileSelector.getInProgressFiles();

        /* Refresh expiration info so it reflects the current time. */
        final ExpirationProfile expProfile = cleaner.getExpirationProfile();
        final long currentTime = TTL.currentSystemTime();
        expProfile.refresh(currentTime);

        /*
         * Use local variables for mutable properties.  Using values that are
         * changing during a single file selection pass would not produce a
         * well defined result.
         *
         * Note that age is a distance between files not a number of files,
         * that is, deleted files are counted in the age.
         */
        final int totalThreshold = cleaner.minUtilization;
        final int fileThreshold = cleaner.minFileUtilization;
        final int twoPassThreshold = cleaner.twoPassThreshold;
        final int twoPassGap = cleaner.twoPassGap;
        final int minAge = cleaner.minAge;
        final boolean gradualExpiration = cleaner.gradualExpiration;
        final boolean expirationEnabled = env.isExpirationEnabled();

        /*
         * Cleaning must refrain from rearranging the portion of log processed
         * as recovery time. Do not clean a file greater or equal to the first
         * active file used in recovery, which is either the last log file or
         * the file of the first active LSN in an active transaction, whichever
         * is earlier.
         *
         * TxnManager.getFirstActiveLsn() (firstActiveTxnLsn below) is
         * guaranteed to be earlier or equal to the first active LSN of the
         * checkpoint that will be performed before deleting the selected log
         * file. By selecting a file prior to this point we ensure that will
         * not clean any entry that may be replayed by recovery.
         *
         * For example:
         * 200 ckptA start, determines that ckpt's firstActiveLsn = 100
         * 400 ckptA end
         * 600 ckptB start, determines that ckpt's firstActiveLsn = 300
         * 800 ckptB end
         *
         * Any cleaning that executes before ckpt A start will be constrained
         * to files <= lsn 100, because it will have checked the TxnManager.
         * If cleaning executes after ckptA start, it may indeed clean after
         * ckptA's firstActiveLsn, but the cleaning run will wait to ckptB end
         * to delete files.
         */
        long firstActiveFile = fileSummaryMap.lastKey();
        final long firstActiveTxnLsn = env.getTxnManager().getFirstActiveLsn();

        if (firstActiveTxnLsn != DbLsn.NULL_LSN) {

            long firstActiveTxnFile =
                DbLsn.getFileNumber(firstActiveTxnLsn);

            if (firstActiveFile > firstActiveTxnFile) {
                firstActiveFile = firstActiveTxnFile;
            }
        }

        /*
         * Note that minAge is at least one and may be configured to a higher
         * value to prevent cleaning recently active files.
         */
        final long lastFileToClean = firstActiveFile - minAge;

        /*
         * Given the total, obsolete and expired bytes:
         *
         * min utilization: 100 * ((obsolete + expired) / total)
         * max utilization: 100 * (max(obsolete, expired) / total)
         * avg utilization: (max - min) / 2
         *
         * Calculate min/max utilization in several categories:
         *
         * + Current total utilization, not counting the results of cleaning
         *   that has not yet occurred, and using the current expired sizes.
         *
         * + Predicted total utilization, estimating the results of cleaning
         *   yet to occur, and using a gradual expired size to prevent cleaning
         *   spikes after hour/day boundaries.
         *
         * + Utilization for the "best" file to use when cleaning normally. The
          *  file with the lowest avg utilization is selected.
         *
         * + Also determine the "best gradual" file with the lowest gradual
         *   max utilization. Note that when a file is selected due to the
         *   minFileUtilization threshold, gradual utilization must be used.
         *   Otherwise, cleaning due to this threshold would defeat gradual
         *   expiration in general.
         */
        Long bestFile = null;
        int bestFileAvgUtil = 101;
        int bestFileMinUtil = 0;
        int bestFileMaxUtil = 0;
        Long bestGradualFile = null;
        int bestGradualFileMaxUtil = 101;
        long currentTotalSize = 0;
        long currentMinObsoleteSize = 0;
        long currentMaxObsoleteSize = 0;
        long predictedTotalSize = 0;
        long predictedMinObsoleteSize = 0;
        long predictedMaxObsoleteSize = 0;

        for (final Map.Entry<Long, FileSummary> entry :
             fileSummaryMap.entrySet()) {

            final Long file = entry.getKey();
            final long fileNum = file;

            if (!env.getFileProtector().isActiveOrNewFile(file)) {
                continue;
            }

            final FileSummary summary = entry.getValue();

            final int expiredSize;
            final int expiredGradualSize;

            if (expirationEnabled) {

                final Pair<Integer, Integer> expiredSizes =
                    expProfile.getExpiredBytes(fileNum, currentTime);

                expiredSize = Math.min(
                    expiredSizes.first(), summary.totalSize);

                expiredGradualSize = gradualExpiration ?
                    Math.min(expiredSizes.second(), summary.totalSize) :
                    expiredSize;

            } else {
                expiredSize = 0;
                expiredGradualSize = 0;
            }

            /*
             * If the file is safe-to-delete, it is entirely obsolete by
             * definition. This is more accurate than using getObsoleteSize.
             */
            final int obsoleteSize = summary.getObsoleteSize();

            final int minObsoleteSize = Math.max(
                obsoleteSize, expiredSize);

            final int maxObsoleteSize = Math.min(
                obsoleteSize + expiredSize,
                summary.totalSize);

            final int minGradualObsoleteSize = Math.max(
                obsoleteSize, expiredGradualSize);

            final int maxGradualObsoleteSize = Math.min(
                obsoleteSize + expiredGradualSize,
                summary.totalSize);

            currentTotalSize += summary.totalSize;
            currentMinObsoleteSize += minObsoleteSize;
            currentMaxObsoleteSize += maxObsoleteSize;

            /*
             * If the file has been clean or is being cleaned, assume the
             * file's data will occupy only its currently utilized bytes, after
             * cleaning and deletion, based on the min obsolete size. This is
             * an intentionally overly optimistic prediction of the results of
             * cleaning, and is used to prevent over-cleaning, especially one
             * due to a backlog created using inaccurate predictions.
             */
            if (inProgressFiles.contains(file)) {
                final int utilizedSize = summary.totalSize - minObsoleteSize;
                predictedTotalSize += utilizedSize;
                continue;
            }

            predictedTotalSize += summary.totalSize;
            predictedMinObsoleteSize += minGradualObsoleteSize;
            predictedMaxObsoleteSize += maxGradualObsoleteSize;

            /* Skip files that are too young to be cleaned. */
            if (fileNum > lastFileToClean) {
                continue;
            }

            /*
             * Pick the "best" file -- the one having the lowest avg
             * utilization so far.
             */
            final int thisMinUtil = FileSummary.utilization(
                maxObsoleteSize, summary.totalSize);

            final int thisMaxUtil = FileSummary.utilization(
                minObsoleteSize, summary.totalSize);

            final int thisAvgUtil = (thisMinUtil + thisMaxUtil) / 2;

            if (bestFile == null || thisAvgUtil < bestFileAvgUtil) {
                bestFile = file;
                bestFileAvgUtil = thisAvgUtil;
                bestFileMinUtil = thisMinUtil;
                bestFileMaxUtil = thisMaxUtil;
            }

            /*
             * Pick the "best gradual" file -- the one having the lowest max
             * gradual utilization so far.
             */
            final int thisGradualMaxUtil = FileSummary.utilization(
                minGradualObsoleteSize, summary.totalSize);

            if (bestGradualFile == null ||
                thisGradualMaxUtil < bestGradualFileMaxUtil) {

                bestGradualFile = file;
                bestGradualFileMaxUtil = thisGradualMaxUtil;
            }
        }

        final int currentMinUtil = FileSummary.utilization(
            currentMaxObsoleteSize, currentTotalSize);

        final int currentMaxUtil = FileSummary.utilization(
            currentMinObsoleteSize, currentTotalSize);

        final int predictedMinUtil = FileSummary.utilization(
            predictedMaxObsoleteSize, predictedTotalSize);

        final int predictedMaxUtil = FileSummary.utilization(
            predictedMinObsoleteSize, predictedTotalSize);

        currentMinUtilization = currentMinUtil;
        currentMaxUtilization = currentMaxUtil;
        predictedMinUtilization = predictedMinUtil;
        predictedMaxUtilization = predictedMaxUtil;

        /*
         * 1. If total min utilization is below the threshold, clean the
         *    "best" file, which is the one with the lowest avg utilization.
         *
         * 2. Else if the "best gradual" file has a max gradual utilization
         *    that is below the threshold for a single file, clean it.
         *
         * 3. Else if there are more files to migrate, clean the next file to
         *    be migrated.
         *
         * 4. Else if cleaning is forced for unit testing, clean the best file.
         */
        final Long fileChosen;
        final String reason;

        if (predictedMinUtil < totalThreshold) {

            fileChosen = bestFile;
            reason = "predicted min util is below minUtilization";

        } else if (bestGradualFileMaxUtil < fileThreshold) {

            fileChosen = bestGradualFile;
            reason = "file has avg util below minFileUtilization";

        } else if (filesToMigrate.hasNext(fileSummaryMap, inProgressFiles)) {

            fileChosen = filesToMigrate.next(fileSummaryMap, inProgressFiles);
            reason = "there are more forceCleanFiles";

        } else if (forceCleaning) {

            fileChosen = bestFile;
            reason = "forced for testing";

        } else {
            fileChosen = null;
            reason = "no file selected";
        }

        String bestFileMsg = "";
        String twoPassMsg = "";
        int pass1RequiredUtil = -1;

        if (fileChosen != null && fileChosen.equals(bestFile)) {

            bestFileMsg =
                ", chose file with util min: " + bestFileMinUtil +
                " max: " + bestFileMaxUtil +
                " avg: " + bestFileAvgUtil;

            /*
             * If the difference between the file's min and max utilization is
             * more than twoPassGap, and its max utilization is more then
             * twoPassThreshold, use two pass cleaning. When using two-pass
             * cleaning, skip the second pass if its recalculated utilization
             * is higher than twoPassThreshold. In other words, if the benefit
             * of cleaning is low, don't actually clean it.
             */
            if (bestFileMaxUtil > twoPassThreshold &&
                bestFileMaxUtil - bestFileMinUtil >= twoPassGap) {

                pass1RequiredUtil = twoPassThreshold;
                twoPassMsg = ", 2-pass cleaning";
            }
        }

        final Level logLevel = (fileChosen != null) ? Level.INFO : Level.FINE;

        if (logger.isLoggable(logLevel)) {

            LoggerUtils.logMsg(
                logger, env, logLevel,
                "Clean file " +
                ((fileChosen != null) ?
                    ("0x" + Long.toHexString(fileChosen)) : "none") +
                ": " + reason + twoPassMsg +
                ", current util min: " + currentMinUtil +
                " max: " + currentMaxUtil +
                ", predicted util min: " + predictedMinUtil +
                " max: " + predictedMaxUtil +
                bestFileMsg);
        }

        return (fileChosen != null) ?
            new Pair<>(fileChosen, pass1RequiredUtil) :
            null;
    }

    /**
     * Process notifications of mutable property changes.
     *
     * @throws IllegalArgumentException via FilesToMigrate ctor and
     * parseForceCleanFiles.
     */

    public synchronized void envConfigUpdate(DbConfigManager cm,
                                EnvironmentMutableConfig ignore) {

        filesToMigrate = new FilesToMigrate(env);
    }
}
