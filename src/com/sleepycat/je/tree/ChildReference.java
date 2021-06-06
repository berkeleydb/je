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

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A ChildReference is a reference in the tree from parent to child.  It
 * contains a node reference, key, and LSN.
 */
public class ChildReference implements Loggable {

    private Node target;
    private long lsn;
    private byte[] key;
    private byte state;

    /**
     * Construct an empty child reference, for reading from the log.
     */
    public ChildReference() {
        init(null, Key.EMPTY_KEY, DbLsn.NULL_LSN, 0);
    }

    /**
     * Construct a ChildReference for inserting a new entry.
     */
    public ChildReference(Node target, byte[] key, long lsn) {
        init(target, key, lsn, EntryStates.DIRTY_BIT);
    }

    /**
     * Construct a ChildReference for inserting an existing entry.
     */
    public ChildReference(Node target,
                          byte[] key,
                          long lsn,
                          byte existingState) {
        init(target, key, lsn, existingState | EntryStates.DIRTY_BIT);
    }

    private void init(Node target,
                      byte[] key,
                      long lsn,
                      int state) {
        this.target = target;
        this.key = key;
        this.lsn = lsn;
        this.state = (byte) state;
    }

    /**
     * Return the key for this ChildReference.
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Set the key for this ChildReference.
     */
    public void setKey(byte[] key) {
        this.key = key;
        setDirty();
    }

    /**
     * Fetch the target object that this ChildReference refers to.  If the
     * object is already in VM, then just return the reference to it.  If the
     * object is not in VM, then read the object from the log.  If the object
     * has been faulted in and the in arg is supplied, then the total memory
     * size cache in the IN is invalidated.
     *
     * @param database The database that this ChildReference resides in.
     * @param parent The IN that this ChildReference lives in.  If
     * the target is fetched (i.e. it is null on entry), then the
     * total in memory count is invalidated in the IN. May be null.
     * For example, the root is a ChildReference and there is no parent IN
     * when the rootIN is fetched in.
     * @return the Node object representing the target node in the tree, or
     * null if there is no target of this ChildReference, or null if a
     * pendingDelete or knownDeleted entry has been cleaned.
     */
    public Node fetchTarget(DatabaseImpl database, IN parent)
        throws DatabaseException {

        if (target == null) {

            if (lsn == DbLsn.NULL_LSN) {
                if (!isKnownDeleted()) {
                    throw EnvironmentFailureException.unexpectedState(
                        IN.makeFetchErrorMsg(
                            "NULL_LSN without KnownDeleted",
                            parent, lsn, state, 0));
                }
                /* Ignore a NULL_LSN (return null) if KnownDeleted is set. */
                return null;
            }

            EnvironmentImpl envImpl = database.getEnv();
            try {
                Node node = (Node) envImpl.getLogManager().getEntry(lsn);

                /*
                 * For now, fetchTarget is never used to fetch an LN. If
                 * this changes in the future, a CacheMode must be given
                 * as a param, and the parent BIN moved from the dirty LRU
                 * to the mixed LRU if the CacheMode is not EVICT_LN (as
                 * it is done in IN.fetchTarget()).
                 */
                assert(node.isIN());
                assert(!node.isBINDelta());

                final IN in = (IN) node;

                in.latchNoUpdateLRU(database);

                node.postFetchInit(database, lsn);
                target = node;

                in.releaseLatch();

                if (parent != null) {
                    parent.updateMemorySize(null, target);
                }
            } catch (FileNotFoundException e) {
                if (!isKnownDeleted() &&
                    !isPendingDeleted()) {
                    throw new EnvironmentFailureException(
                        envImpl, EnvironmentFailureReason.LOG_FILE_NOT_FOUND,
                        IN.makeFetchErrorMsg(
                            e.toString(), parent, lsn, state, 0),
                        e);
                    }

                /*
                 * This is a LOG_FILE_NOT_FOUND for a KD or PD entry.
                 *
                 * Ignore. Cleaner got to it, so just return null.
                 */
            } catch (EnvironmentFailureException e) {
                e.addErrorMessage(
                    IN.makeFetchErrorMsg(null, parent, lsn, state, 0));
                throw e;
            } catch (RuntimeException e) {
                throw new EnvironmentFailureException(
                    envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                    IN.makeFetchErrorMsg(e.toString(), parent, lsn, state, 0),
                    e);
            }
        }

        return target;
    }

    /*
     * Return the state byte for this ChildReference.
     */
    byte getState() {
        return state;
    }

    /**
     * Return the target for this ChildReference.
     */
    public Node getTarget() {
        return target;
    }

    /**
     * Sets the target for this ChildReference. No need to make dirty, that
     * state only applies to key and LSN.
     */
    public void setTarget(Node target) {
        this.target = target;
    }

    /**
     * Clear the target for this ChildReference. No need to make dirty, that
     * state only applies to key and LSN. This method is public because it's
     * safe and used by RecoveryManager. This can't corrupt the tree.
     */
    public void clearTarget() {
        this.target = null;
    }

    /**
     * Return the LSN for this ChildReference.
     *
     * @return the LSN for this ChildReference.
     */
    public long getLsn() {
        return lsn;
    }

    /**
     * Sets the target LSN for this ChildReference.
     *
     * @param lsn the target LSN.
     */
    public void setLsn(long lsn) {
        this.lsn = lsn;
        setDirty();
    }

    /**
     * Do deferredWrite optional logging check.
     */
    void updateLsnAfterOptionalLog(DatabaseImpl dbImpl, long lsn) {
        if ((lsn == DbLsn.NULL_LSN) &&
            dbImpl.isDeferredWriteMode()) {
            /*
             * Don't update the lsn -- we don't want to overwrite a
             * non-null lsn.
             */
            setDirty();
        } else {
            setLsn(lsn);
        }
    }

    void setDirty() {
        state |= EntryStates.DIRTY_BIT;
    }

    /**
     * @return true if the entry has been deleted, although the transaction the
     * performed the deletion may not be committed.
     */
    private boolean isPendingDeleted() {
        return ((state & EntryStates.PENDING_DELETED_BIT) != 0);
    }

    /**
     * @return true if entry is deleted for sure.
     */
    public boolean isKnownDeleted() {
        return ((state & EntryStates.KNOWN_DELETED_BIT) != 0);
    }

    /**
     * @return true if the object is dirty.
     */
    private boolean isDirty() {
        return ((state & EntryStates.DIRTY_BIT) != 0);
    }

    /*
     * Support for logging.
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        return
            LogUtils.getByteArrayLogSize(key) +   // key
            LogUtils.getPackedLongLogSize(lsn) +  // LSN
            1;                                    // state
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeByteArray(logBuffer, key);  // key
        LogUtils.writePackedLong(logBuffer, lsn);
        logBuffer.put(state);                     // state
        state &= EntryStates.CLEAR_DIRTY_BIT;
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        key = LogUtils.readByteArray(itemBuffer, unpacked);      // key
        lsn = LogUtils.readLong(itemBuffer, unpacked);           // LSN
        state = itemBuffer.get();                                // state
        state &= EntryStates.CLEAR_DIRTY_BIT;
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<ref kd=\"").append(isKnownDeleted());
        sb.append("\" pd=\"").append(isPendingDeleted());
        sb.append("\">");
        sb.append(Key.dumpString(key, 0));
        sb.append(DbLsn.toString(lsn));
        sb.append("</ref>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /*
     * Dumping
     */
    public String dumpString(int nspaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        if (lsn == DbLsn.NULL_LSN) {
            sb.append(TreeUtils.indent(nspaces));
            sb.append("<lsn/>");
        } else {
            sb.append(DbLsn.dumpString(lsn, nspaces));
        }
        sb.append('\n');
        if (key == null) {
            sb.append(TreeUtils.indent(nspaces));
            sb.append("<key/>");
        } else {
            sb.append(Key.dumpString(key, nspaces));
        }
        sb.append('\n');
        if (target == null) {
            sb.append(TreeUtils.indent(nspaces));
            sb.append("<target/>");
        } else {
            sb.append(target.dumpString(nspaces, true));
        }
        sb.append('\n');
        sb.append(TreeUtils.indent(nspaces));
        sb.append("<knownDeleted val=\"");
        sb.append(isKnownDeleted()).append("\"/>");
        sb.append("<pendingDeleted val=\"");
        sb.append(isPendingDeleted()).append("\"/>");
        sb.append("<dirty val=\"").append(isDirty()).append("\"/>");
        return sb.toString();
    }

    @Override
    public String toString() {
        return dumpString(0, false);
    }
}
