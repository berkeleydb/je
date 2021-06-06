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

import java.util.Arrays;

import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * The abstract class that defines the various representations used to
 * represent an array of target pointers to children of an IN node. These
 * arrays can be sparse, so the non-default representations are designed to
 * make efficient representations for the sparse cases. Each specialized
 * representation is a subclass of INTargetReps.
 *
 * A new IN node starts out with the None representation and grows through a
 * sparse into the full default representation. Subsequently, the default
 * representation can be <i>compacted</i> into a Sparse or None representation
 * whenever an IN is stripped. Note that representations do not currently move
 * to more compact forms when entries are nulled to minimize the possibility of
 * tansitionary representation changes, since each representation change has
 * a cpu cost and a gc cost associated with it.
 */
public abstract class INTargetRep
    extends INArrayRep<INTargetRep, INTargetRep.Type, Node> {

    /* Single instance used for None rep. */
    public static final None NONE = new None();

    /* Enumeration for the different types of supported representations. */
    public enum Type { DEFAULT, SPARSE, NONE }

    public INTargetRep() {
    }

    /* The default non-sparse representation. It simply wraps an array. */
    public static class Default extends INTargetRep {

        /* The target nodes */
        private final Node[] targets;

        public Default(int capacity) {
            this.targets = new Node[capacity];
        }

        /* Only for use by the Sizeof utility. */
        public Default(@SuppressWarnings("unused") SizeofMarker marker) {
            targets = null;
        }

        private Default(Node[] targets) {
            this.targets = targets;
        }

        @Override
        public Default resize(int capacity) {
            return new Default(Arrays.copyOfRange(targets, 0, capacity));
        }

        @Override
        public Type getType() {
            return Type.DEFAULT;
        }

        @Override
        public Node get(int idx) {
            return targets[idx];
        }

        @Override
        public INTargetRep set(int idx, Node node, IN parent) {
            targets[idx] = node;
            return this;
        }

        @Override
        public INTargetRep copy(int from, int to, int n, IN parent) {
            System.arraycopy(targets, from, targets, to, n);
            return this;
        }

        @Override
        public INTargetRep compact(IN parent) {
            int count = 0;
            for (Node target : targets) {
                if (target != null) {
                    count++;
                }
            }

            if ((count > Sparse.MAX_ENTRIES) ||
                (targets.length > Sparse.MAX_INDEX)) {
                return this;
            }

            INTargetRep newRep = null;
            if (count == 0) {
                newRep = NONE;
            } else {
                newRep = new Sparse(targets.length);
                for (int i=0; i < targets.length; i++) {
                    if (targets[i] != null) {
                        newRep.set(i, targets[i], parent);
                    }
                }
            }

            noteRepChange(newRep, parent);
            return newRep;
        }

        @Override
        public long calculateMemorySize() {
            return MemoryBudget.DEFAULT_TARGET_ENTRY_OVERHEAD +
                   MemoryBudget.objectArraySize(targets.length);
        }

        @Override
        public void updateCacheStats(@SuppressWarnings("unused")
                                     boolean increment,
                                     @SuppressWarnings("unused")
                                     Evictor evictor) {
            /* No stats for this default rep. */
        }
    }

    /**
     * Representation used when 1-4 children are cached. Note that the IN
     * itself may have more children, but they are not currently cached.
     * The INArrayRep is represented by two parallel arrays: an array of
     * indices (idxs) and an array of values (targets). All elements that are
     * not explicitly represented are null.
     */
    public static class Sparse extends INTargetRep {

        /* The maximum number of entries that can be represented. */
        public static final int MAX_ENTRIES = 4;

        /* The maximum index that can be represented. */
        public static final int MAX_INDEX = Short.MAX_VALUE;

        /*
         * The parallel arrays implementing the INArrayRep.
         */
        final short idxs[] = new short[MAX_ENTRIES];
        final Node targets[] = new Node[MAX_ENTRIES];

        public Sparse(int capacity) {

            /* Unroll initialization. */
            idxs[0] = idxs[1] = idxs[2] = idxs[3] = -1;
        }

        /* Only for use by the Sizeof utility. */
        public Sparse(@SuppressWarnings("unused") SizeofMarker marker) {
        }

        @Override
        public Sparse resize(int capacity) {
            return this;
        }

        @Override
        public Type getType() {
            return Type.SPARSE;
        }

        @Override
        public Node get(int j) {
            assert (j >= 0) && (j <= MAX_INDEX);

            /* Unrolled for loop */
            if (idxs[0] == j) {
                return targets[0];
            }
            if (idxs[1] == j) {
                return targets[1];
            }
            if (idxs[2] == j) {
                return targets[2];
            }
            if (idxs[3] == j) {
                return targets[3];
            }
            return null;
        }

        @Override
        public INTargetRep set(int j, Node node, IN parent) {

            assert (j >= 0) && (j <= MAX_INDEX);

            int slot = -1;
            for (int i=0; i < targets.length; i++) {

                if (idxs[i] == j) {
                    targets[i] = node;
                    return this;
                }

                if ((slot < 0) && (targets[i] == null)) {
                   slot = i;
                }
            }

            if (node == null) {
                return this;
            }

            /* Have a free slot, use it. */
            if (slot >= 0) {
                targets[slot] = node;
                idxs[slot] = (short)j;
                return this;
            }

            /* It's full, mutate it. */
            Default fe = new Default(parent.getMaxEntries());
            noteRepChange(fe, parent);

            for (int i=0; i < targets.length; i++) {
                if (targets[i] != null) {
                    fe.set(idxs[i], targets[i], parent);
                }
            }

            return fe.set(j, node, parent);
        }

        @Override
        public INTargetRep copy(int from, int to, int n, IN parent) {

            INTargetRep target = this;

            if ((to == from) || (n == 0)) {
                /* Nothing to do */
            } else if (to < from) {
                /* Copy ascending */
                for (int i = 0; i < n; i++) {
                    target = target.set(to++, get(from++), parent);
                }
            } else {
                /* to > from. Copy descending */
                from += n;
                to += n;
                for (int i = 0; i < n; i++) {
                    target = target.set(--to, get(--from), parent);
                }
            }
            return target;
        }

        @Override
        public INTargetRep compact(IN parent) {
            int count = 0;
            for (Node target : targets) {
                if (target != null) {
                    count++;
                }
            }
            if (count == 0) {
                None newRep = NONE;
                noteRepChange(newRep, parent);
                return newRep;
            }
            return this;
        }

        @Override
        public long calculateMemorySize() {
            /*
             * Note that fixed array sizes are already accounted for in the
             * SPARSE_TARGET_ENTRY_OVERHEAD computed vis Sizeof.
             */
            return MemoryBudget.SPARSE_TARGET_ENTRY_OVERHEAD;
        }

        @Override
        public void updateCacheStats(boolean increment, Evictor evictor) {
            if (increment) {
                evictor.getNINSparseTarget().incrementAndGet();
            } else {
                evictor.getNINSparseTarget().decrementAndGet();
            }
        }
    }

    /**
     * Representation used when an IN has no children cached.
     */
    public static class None extends INTargetRep {

        private None() {
        }

        /* Only for use by the Sizeof utility. */
        public None(@SuppressWarnings("unused") SizeofMarker marker) {
        }

        @Override
        public None resize(int capacity) {
            return this;
        }

        @Override
        public Type getType() {
            return Type.NONE;
        }

        @Override
        public Node get(@SuppressWarnings("unused") int idx) {
            return null;
        }

        @Override
        public INTargetRep set(int idx, Node node, IN parent) {

            if (node == null) {
                return this;
            }

            INTargetRep targets = new Sparse(parent.getMaxEntries());
            noteRepChange(targets, parent);
            return targets.set(idx, node, parent);
        }

        @Override
        public INTargetRep copy(@SuppressWarnings("unused") int from,
                                @SuppressWarnings("unused") int to,
                                @SuppressWarnings("unused") int n,
                                @SuppressWarnings("unused") IN parent) {
            /* Nothing to copy. */
            return this;
        }

        @Override
        public INTargetRep compact(IN parent) {
            return this;
        }

        @Override
        public long calculateMemorySize() {
            /* A single static instance is used. */
            return 0;
        }

        @Override
        public void updateCacheStats(boolean increment, Evictor evictor) {
            if (increment) {
                evictor.getNINNoTarget().incrementAndGet();
            } else {
                evictor.getNINNoTarget().decrementAndGet();
            }
        }
    }
}
