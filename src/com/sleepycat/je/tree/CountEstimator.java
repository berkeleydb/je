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

package com.sleepycat.je.tree;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.txn.LockType;

/**
 * Estimates the number of non-deleted BIN entries between two end points,
 * using information returned in TrackingInfo from Tree.getParentINForChildIN.
 * Used for estimating dup counts, e.g., for join query optimization.  Accuracy
 * is limited by the number of samples taken to compute the average number of
 * entries at each level.  Currently only two samples (at the end points) are
 * taken, and because the tree is not balanced making the number of entries
 * highly variable, the count can easily be off by a factor of two.
 */
public class CountEstimator {

    /* If exceeded, there must be a bug of some kind. */
    private static final int MAX_RETRIES_AFTER_SPLIT = 100;

    /**
     * Returns an estimate of the number of records between two end points
     * specified by begin/end cursor positions.
     */
    public static long count(DatabaseImpl dbImpl,
                             CursorImpl beginCursor,
                             boolean beginInclusive,
                             CursorImpl endCursor,
                             boolean endInclusive) {

        /* If the two cursors are at the same position, return 1. */
        if (beginCursor.isOnSamePosition(endCursor)) {
            return 1;
        }

        /* Compute estimate for cursors at different positions. */
        final CountEstimator estimator = new CountEstimator(dbImpl);

        return estimator.count(beginCursor, endCursor) +
               (beginInclusive ? 1 : 0) +
               (endInclusive ? 1 : 0);
    }

    private final DatabaseImpl dbImpl;

    private List<TrackingInfo> beginStack;
    private List<TrackingInfo> endStack;

    private final List<List<TrackingInfo>> avgEntriesStacks =
        new ArrayList<List<TrackingInfo>>();

    private int levelCount;
    private int rootLevel;
    private float[] avgEntries;

    private CountEstimator(DatabaseImpl dbImpl) {
        this.dbImpl = dbImpl;
    }
    
    private long count(CursorImpl beginCursor, CursorImpl endCursor) {

        for (int numRetries = 0;; numRetries += 1) {

            /*
             * If we have retried too many times, give up.  This is probably
             * due to a bug of some kind, and we shouldn't loop forever.
             */
            if (numRetries > MAX_RETRIES_AFTER_SPLIT) {
                throw EnvironmentFailureException.unexpectedState();
            }

            /*
             * Set up the initial info for the computation.  Retry if a split
             * occurs.
             */
            beginStack = beginCursor.getAncestorPath();
            if (beginStack == null) {
                continue;
            }
            endStack = endCursor.getAncestorPath();
            if (endStack == null) {
                continue;
            }

            if (!findCommonAncestor()) {
                continue;
            }

            /* Get the the average entries from the two end points.  */
            getAvgEntries(beginCursor, endCursor);

            /*
             * Return the count.  FUTURE: Taking more samples between the two
             * end points would increase accuracy.
             */
            return countTotal();
        }
    }

    /**
     * Find the common ancestor node for the two end points, which we'll call
     * the root level.  If no common ancestor can be found, return false to
     * restart the process, because a split must have occurred in between
     * getting the two stacks for the end points.
     */
    private boolean findCommonAncestor() {

        levelCount = beginStack.size();
        if (levelCount != endStack.size()) {
            /* Must have been a root split. */
            return false;
        }

        rootLevel = -1;

        for (int level = levelCount - 1; level >= 0; level -= 1) {

            if (beginStack.get(level).nodeId == endStack.get(level).nodeId) {
                rootLevel = level;
                break;
            }
        }
        if (rootLevel < 0) {
            /* Must have been a split. */
            return false;
        }
        return true;
    }

    /**
     * This method starts with a preliminary average using just the two end
     * points.
     */
    private void getAvgEntries(CursorImpl beginCursor, CursorImpl endCursor) {

        avgEntriesStacks.clear();

        if (!addAvgEntriesSample(beginStack)) {
            sampleNextBIN(beginCursor, true /*moveForward*/);
        }

        if (!addAvgEntriesSample(endStack)) {
            sampleNextBIN(endCursor, false /*moveForward*/);
        }

        computeAvgEntries();
    }

    /**
     * FUTURE: use internal skip method instead, saving a btree lookup.
     */
    private void sampleNextBIN(
        CursorImpl beginOrEndCursor,
        boolean moveForward) {

        final CursorImpl cursor =
            beginOrEndCursor.cloneCursor(true /*samePosition*/);
        
        try {
            cursor.latchBIN();
            if (moveForward) {
                cursor.setOnLastSlot();
            } else {
                cursor.setOnFirstSlot();
            }

            final OperationResult result = cursor.getNext(
                null /*foundKey*/, null /*foundData*/,
                LockType.NONE, false /*dirtyReadAll*/,
                moveForward, true /*alreadyLatched*/,
                null /*rangeConstraint*/);

            if (result != null) {
                final List<TrackingInfo> stack = cursor.getAncestorPath();
                if (stack != null) {
                    addAvgEntriesSample(stack);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * At each level we compute the average number of entries.  This will be
     * used as a multipler to estimate the number of nodes for any IN at that
     * level.
     */
    private void computeAvgEntries() {

        avgEntries = new float[levelCount];

        avgEntries[levelCount - 1] = 1.0F;

        if (avgEntriesStacks.size() == 0) {
            return;
        }

        for (int level = levelCount - 1; level > 0; level -= 1) {
            long totalEntries = 0;
            for (List<TrackingInfo> stack : avgEntriesStacks) {
                totalEntries += stack.get(level).entries;
            }
            final float avg = totalEntries / ((float) avgEntriesStacks.size());
            avgEntries[level - 1] = avg * avgEntries[level];
        }
    }

    private boolean addAvgEntriesSample(List<TrackingInfo> stack) {
        if (isFirstBIN(stack) || isLastBIN(stack)) {
            return false;
        }
        avgEntriesStacks.add(stack);
        return true;
    }

    private boolean isFirstBIN(List<TrackingInfo> stack) {
        for (int i = 0; i < stack.size() - 1; i += 1) {
            final TrackingInfo info = stack.get(i);
            if (info.index != 0) {
                return false;
            }
        }
        return true;
    }

    private boolean isLastBIN(List<TrackingInfo> stack) {
        for (int i = 0; i < stack.size() - 1; i += 1) {
            final TrackingInfo info = stack.get(i);
            if (info.index != info.entries - 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Count the total for each node that is between the two end points.
     */
    private long countTotal() {
        long total = 0;

        /* Add nodes between the end points at the root level. */
        final int rootIndex1 = beginStack.get(rootLevel).index + 1;
        final int rootIndex2 = endStack.get(rootLevel).index;
        if (rootIndex2 > rootIndex1) {
            total += Math.round((rootIndex2 - rootIndex1) *
                                avgEntries[rootLevel]);
        }

        /* Add nodes under the end points at lower levels. */
        for (int level = rootLevel + 1; level < levelCount; level += 1) {

            /* Add nodes under left end point that are to its right. */
            final int leftIndex = beginStack.get(level).index;
            final int lastIndex = beginStack.get(level).entries - 1;
            if (lastIndex > leftIndex) {
                total += Math.round((lastIndex - leftIndex) *
                                    avgEntries[level]);
            }

            /* Add nodes under right end point that are to its left. */
            final int rightIndex = endStack.get(level).index;
            final int firstIndex = 0;
            if (rightIndex > firstIndex) {
                total += Math.round((rightIndex - firstIndex) *
                                    avgEntries[level]);
            }
        }

        return total;
    }

    /* For future use, if getKeyRatios is exposed in the API. */
    static class KeyRatios {
        final double less;
        final double equal;
        final double greater;

        KeyRatios(double less, double equal, double greater) {
            this.less = less;
            this.equal = equal;
            this.greater = greater;
        }

        @Override
        public String toString() {
            return "less: " + less +
                   " equal: " + equal +
                   " greater: " + greater;
        }
    }

    /*
     * For future use, if getKeyRatios is exposed in the API.  Be sure to test
     * boundary conditions when index is 0 or nEntries.
     *
     * Algorithm copied from __bam_key_range in BDB btree/bt_stat.c.
     */
    static KeyRatios getKeyRatios(List<TrackingInfo> infoByLevel,
                                  boolean exact) {
        double factor = 1.0;
        double less = 0.0;
        double greater = 0.0;

        /*
         * At each level we know that INs greater than index contain keys
         * greater than what we are looking for and those less than index are
         * less than.  The one pointed to by index may have some less, some
         * greater or even equal.  If index is equal to the number of entries,
         * then the key is out of range and everything is less.
         */
        for (final TrackingInfo info : infoByLevel) {
            if (info.index == 0) {
                greater += (factor * (info.entries - 1)) / info.entries;
            } else if (info.index == info.entries) {
                less += factor;
            } else {
                less += (factor * info.index) / info.entries;
                greater += (factor * ((info.entries - info.index) - 1)) /
                           info.entries;
            }

            /* Factor at next level down is 1/n'th the amount at this level. */
            factor /= info.entries;

            /*
            System.out.println("factor: " + factor +
                               " less: " + less +
                               " greater: " + greater);
            */
        }

        /*
         * If there was an exact match then assign the 1/n'th factor to the key
         * itself.  Otherwise that factor belongs to those greater than the
         * key, unless the key was out of range.
         */
        final double equal;
        if (exact) {
            equal = factor;
        } else {
            if (less != 1.0) {
                greater += factor;
            }
            equal = 0.0;
        }

        return new KeyRatios(less, equal, greater);
    }
}
