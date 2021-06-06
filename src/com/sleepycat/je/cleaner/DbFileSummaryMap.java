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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.FileManager;

public class DbFileSummaryMap {

    private final static int FILE_ENTRY_OVERHEAD =
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.LONG_OVERHEAD +
        MemoryBudget.DBFILESUMMARY_OVERHEAD;

    private Map<Long, DbFileSummary> map;
    private int memSize;
    private MemoryBudget budget;

    /**
     * Creates a map of Long file number to DbFileSummary.  The init() method
     * must be called after creating this object.
     *
     * <p>Always counts this object and its contained objects in the memory
     * budget.  If countParentMapEntry is true, also counts a single HashMap
     * entry that contains this object.  This option allows all memory budget
     * adjustments for LocalUtilizationTracker to be contained in this
     * class.</p>
     */
    public DbFileSummaryMap(boolean countParentMapEntry) {
        map = new HashMap<Long, DbFileSummary>();
        memSize = MemoryBudget.HASHMAP_OVERHEAD;
        if (countParentMapEntry) {
            memSize += MemoryBudget.HASHMAP_ENTRY_OVERHEAD;
        }
    }

    /**
     * Starts memory budgeting.  The map and its entries will be counted in
     * the budget.  When adding entries via the get() method prior to calling
     * this method, the adjustMemBudget parameter must be false.  After calling
     * this method, the adjustMemBudget parameter must be true.
     *
     * <p>This method is separate from the constructor so that the map may be
     * read from the log without having the EnvironmentImpl object
     * available.</p>
     */
    public void init(EnvironmentImpl env) {
        budget = env.getMemoryBudget();
        budget.updateTreeAdminMemoryUsage(memSize);
    }

    /**
     * Returns the DbFileSummary for the given file, allocating it if
     * necessary.
     *
     * <p>Must be called under the log write latch.</p>
     *
     * @param fileNum the file identifying the summary.
     *
     * @param adjustMemBudget see init().
     *
     * @param checkResurrected is true if this method should check fileNum and
     * return null if the file does not exist. When checkResurrected is false,
     * the expensive call to File.exists will not be made.
     *
     * @param fileManager is used to check for resurrected files and may be
     * null if checkResurrected is false.
     */
    public DbFileSummary get(Long fileNum,
                             boolean adjustMemBudget,
                             boolean checkResurrected,
                             FileManager fileManager) {

        assert adjustMemBudget == (budget != null);

        /*
         * Note that the call below to isFileValid (which calls File.exists) is
         * only made if the file number is less than the last file in the log,
         * and the file is not already present in the map.  When the file is
         * not the last file, we are recording obsoleteness and the file should
         * already be in the map.  So we only incur the overhead of File.exists
         * when resurrecting a file, which should be pretty rare.
         *
         * The reliability of this approach is questionable. Earlier we had an
         * assertion that double-checked this condition after adding a new map
         * entry and the assertion sometimes fired, indicating that the file
         * was deleted during the execution of this method. Luckily, we plan
         * to remove per-DB utilization metadata completely in the future.
         */
        DbFileSummary summary = map.get(fileNum);
        if (summary == null) {
            if (checkResurrected && 
                fileNum < fileManager.getCurrentFileNum() &&
                !fileManager.isFileValid(fileNum)) {
                /* Will return null. */
            } else {
                summary = new DbFileSummary();
                Object oldVal = map.put(fileNum, summary);
                assert oldVal == null;
                memSize += FILE_ENTRY_OVERHEAD;
                if (adjustMemBudget) {
                   budget.updateTreeAdminMemoryUsage(FILE_ENTRY_OVERHEAD);     
                }
            }
        }
        return summary;
    }

    /**
     * Removes the DbFileSummary for the given file.
     *
     * <p>Must be called under the log write latch.</p>
     */
    public boolean remove(Long fileNum) {
        if (map.remove(fileNum) != null) {
            budget.updateTreeAdminMemoryUsage(0 - FILE_ENTRY_OVERHEAD);
            memSize -= FILE_ENTRY_OVERHEAD;
            return true;
        } else {
            return false;
        }
    }

    /*
     * Get this map's memory size. Usually it's built up over time and added to
     * the global memory budget, but this is used to reinitialize the memory
     * budget after recovery, when DbFileSummaryMaps may be cut adrift by the
     * process of overlaying new portions of the btree.
     */
    public long getMemorySize() {
        return memSize;
    }

    public void subtractFromMemoryBudget() {
        /* May not have been initialized if it was read by a FileReader */
        if (budget != null) {
            budget.updateTreeAdminMemoryUsage(0 - memSize);
            memSize = 0;
        }
    }

    public Set<Map.Entry<Long,DbFileSummary>> entrySet() {
        return map.entrySet();
    }

    public boolean contains(Long fileNum) {
        return map.containsKey(fileNum);
    }

    public int size() {
        return map.size();
    }

    public Map<Long, DbFileSummary> cloneMap() {
        final Map<Long, DbFileSummary> clone =
            new HashMap<Long, DbFileSummary>(map.size());
        final Iterator<Map.Entry<Long, DbFileSummary>> i =
            map.entrySet().iterator();
        while (i.hasNext()) {
            final Map.Entry<Long, DbFileSummary> entry = i.next();
            final Long fileNum = entry.getKey();
            final DbFileSummary summary = entry.getValue();
            clone.put(fileNum, summary.clone());
        }
        return clone;
    }

    @Override
    public String toString() {
        return map.toString();
    }

    /**
     * Removes entries for deleted files that were created by JE 3.3.74 and
     * earlier.  [#16610]
     */
    public void repair(EnvironmentImpl env) {
        Long[] existingFiles = env.getFileManager().getAllFileNumbers();
        Iterator<Long> iter = map.keySet().iterator();
        while (iter.hasNext()) {
            Long fileNum = iter.next();
            if (Arrays.binarySearch(existingFiles, fileNum) < 0) {
                iter.remove();
                budget.updateTreeAdminMemoryUsage(0 - FILE_ENTRY_OVERHEAD);
                memSize -= FILE_ENTRY_OVERHEAD;
            }
        }
    }
}
