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
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * Holds an array of non-negative long values, one for each slot in an IN.
 *
 * Zero is the default value and is returned when no value has been set.
 *
 * The EMPTY_REP is used at first, and is mutated as necessary as values are
 * set.  A minimum number of bytes per value is used, based on the largest
 * value passed to set().
 *
 * Optionally, a sparse rep is used when a value is set for EMPTY_REP. Up to 4
 * values are stored along with their indexes. When the 5th values is set, the
 * rep is mutated to the default rep.
 *
 * This object calls IN.updateMemorySize to track the memory it uses.
 * EMPTY_REP uses no memory because it is a singleton.
 */
public abstract class INLongRep {

    public abstract long get(int idx);
    public abstract INLongRep set(int idx, long val, IN parent);
    public abstract INLongRep compact(IN parent, EmptyRep emptyRep);
    public abstract INLongRep clear(IN parent, EmptyRep emptyRep);
    public abstract boolean isEmpty();
    public abstract INLongRep copy(int from, int to, int n, IN parent);
    public abstract INLongRep resize(int capacity);
    public abstract long getMemorySize();

    /**
     * Initially empty (all values are zero) but will mutate as needed when
     * non-zero values are passed to set().
     */
    public static class EmptyRep extends INLongRep {

        final int minLength;
        final boolean allowSparseRep;

        public EmptyRep(int minLength, boolean allowSparseRep) {
            this.minLength = minLength;
            this.allowSparseRep = allowSparseRep;
        }

        @Override
        public INLongRep resize(int capacity) {
            return this;
        }

        @Override
        public long get(int idx) {
            return 0;
        }

        /**
         * When adding to the cache the EMPTY_REP is mutated into a
         * DefaultRep.
         */
        @Override
        public INLongRep set(int idx, long val, IN parent) {

            if (val == 0) {
                return this;
            }

            final INLongRep newCache;

            if (false /*TODO*/ && allowSparseRep) {
                newCache = new SparseRep(minLength);
            } else {
                newCache = new DefaultRep(parent.getMaxEntries(), minLength);
            }

            parent.updateMemorySize(getMemorySize(), newCache.getMemorySize());

            return newCache.set(idx, val, parent);
        }

        @Override
        public INLongRep compact(IN parent, EmptyRep emptyRep) {
            return this;
        }

        @Override
        public INLongRep clear(IN parent, EmptyRep emptyRep) {
            return this;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public INLongRep copy(int from, int to, int n, IN parent) {
            return this;
        }

        /**
         * An EMPTY_REP has no JE cache memory overhead because there is only
         * one global instance.
         */
        @Override
        public long getMemorySize() {
            return 0;
        }
    }

    public static class DefaultRep extends INLongRep {

        /** Maximum value indexed by number of bytes. */
        private static long[] MAX_VALUE = {
            0x0L,
            0xFFL,
            0xFFFFL,
            0xFFFFFFL,
            0xFFFFFFFFL,
            0xFFFFFFFFFFL,
            0xFFFFFFFFFFFFL,
            0xFFFFFFFFFFFFFFL,
            0x7FFFFFFFFFFFFFFFL,
        };

        private final byte[] byteArray;
        final int bytesPerValue;

        public DefaultRep(int capacity, int nBytes) {
            assert capacity >= 1;
            assert nBytes >= 1;
            assert nBytes <= 8;

            bytesPerValue = nBytes;
            byteArray = new byte[capacity * bytesPerValue];
        }

        /* Only for use by the Sizeof utility. */
        public DefaultRep(@SuppressWarnings("unused") SizeofMarker marker) {
            bytesPerValue = 0;
            byteArray = null;
        }

        private DefaultRep(byte[] byteArray, int bytesPerValue) {
            this.byteArray = byteArray;
            this.bytesPerValue = bytesPerValue;
        }

        @Override
        public DefaultRep resize(int capacity) {
            return new DefaultRep(
                Arrays.copyOfRange(byteArray, 0, capacity * bytesPerValue),
                bytesPerValue);
        }

        @Override
        public long get(int idx) {

            int i = idx * bytesPerValue;
            final int end = i + bytesPerValue;

            long val = (byteArray[i] & 0xFF);

            for (i += 1; i < end; i += 1) {
                val <<= 8;
                val |= (byteArray[i] & 0xFF);
            }

            return val;
        }

        /**
         * Mutates to a DefaultRep with a larger number of bytes if necessary
         * to hold the given value.
         */
        @Override
        public INLongRep set(int idx, long val, IN parent) {

            assert idx >= 0;
            assert idx < byteArray.length / bytesPerValue;
            assert val >= 0;

            /*
             * If the value can't be represented using bytesPerValue, mutate
             * to a cache with a larger number of bytes.
             */
            if (val > MAX_VALUE[bytesPerValue]) {

                final int capacity = byteArray.length / bytesPerValue;

                INLongRep newRep;

                if (getClass() == SparseRep.class) {
                    newRep = new SparseRep(bytesPerValue + 1);
                } else {
                    newRep = new DefaultRep(capacity, bytesPerValue + 1);
                }

                parent.updateMemorySize(
                    getMemorySize(), newRep.getMemorySize());

                /*
                 * Set new value in new cache, and copy other values from old
                 * cache.
                 */
                newRep = newRep.set(idx, val, parent);

                for (int i = 0; i < capacity; i += 1) {
                    if (i != idx) {
                        newRep = newRep.set(i, get(i), parent);
                    }
                }

                return newRep;
            }

            /* Set value in this cache. */
            int i = ((idx + 1) * bytesPerValue) - 1;
            final int end = i - bytesPerValue;

            byteArray[i] = (byte) (val & 0xFF);

            for (i -= 1; i > end; i -= 1) {
                val >>= 8;
                byteArray[i] = (byte) (val & 0xFF);
            }

            assert ((val & 0xFFFFFFFFFFFFFF00L) == 0) : val;

            return this;
        }

        @Override
        public INLongRep compact(IN parent, EmptyRep emptyRep) {

            if (isEmpty()) {
                return clear(parent, emptyRep);
            }

            return this;
        }

        @Override
        public INLongRep clear(IN parent, EmptyRep emptyRep) {

            parent.updateMemorySize(
                getMemorySize(), emptyRep.getMemorySize());

            return emptyRep;
        }

        @Override
        public boolean isEmpty() {

            for (byte b : byteArray) {
                if (b != 0) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public INLongRep copy(int from, int to, int n, IN parent) {
            System.arraycopy(byteArray,
                             from * bytesPerValue,
                             byteArray,
                             to * bytesPerValue,
                             n * bytesPerValue);
            return this;
        }

        @Override
        public long getMemorySize() {
            return MemoryBudget.DEFAULT_LONG_REP_OVERHEAD +
                   MemoryBudget.byteArraySize(byteArray.length);
        }
    }

    public static class SparseRep extends DefaultRep {

        private static final int MAX_ENTRIES = 4;

        private final short[] idxs;

        public SparseRep(int nBytes) {

            super(MAX_ENTRIES, nBytes);

            idxs = new short[MAX_ENTRIES];
            Arrays.fill(idxs, (short) (-1));
        }

        /* Only for use by the Sizeof utility. */
        public SparseRep(@SuppressWarnings("unused") SizeofMarker marker) {
            super(marker);
            idxs = null;
        }

        @Override
        public SparseRep resize(int capacity) {
            return this;
        }

        @Override
        public long get(int idx) {

            for (int i = 0; i < idxs.length; i += 1) {
                if (idxs[i] == idx) {
                    return super.get(i);
                }
            }

            return 0;
        }

        @Override
        public INLongRep set(int idx, long val, IN parent) {

            int slot = -1;

            for (int i = 0; i < idxs.length; i++) {

                if (idxs[i] == idx) {
                    if (val == 0) {
                        idxs[i] = -1;
                    }
                    return super.set(i, val, parent);
                }

                if (slot < 0 && idxs[i] == -1) {
                    slot = i;
                }
            }

            if (val == 0) {
                return this;
            }

            /* Have a free slot, use it. */
            if (slot >= 0) {
                idxs[slot] = (short) idx;
                return super.set(slot, val, parent);
            }

            /* It's full, mutate it. */
            INLongRep newRep =
                new DefaultRep(parent.getMaxEntries(), bytesPerValue);

            parent.updateMemorySize(getMemorySize(), newRep.getMemorySize());

            for (int i = 0; i < idxs.length; i++) {
                if (idxs[i] != -1) {
                    newRep = newRep.set(idxs[i], super.get(i), parent);
                }
            }

            return newRep.set(idx, val, parent);
        }

        @Override
        public INLongRep compact(IN parent, EmptyRep emptyRep) {

            if (isEmpty()) {
                return clear(parent, emptyRep);
            }

            return this;
        }

        @Override
        public INLongRep clear(IN parent, EmptyRep emptyRep) {

            parent.updateMemorySize(
                getMemorySize(), emptyRep.getMemorySize());

            return emptyRep;
        }

        @Override
        public boolean isEmpty() {

            for (short idx : idxs) {
                if (idx != -1) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public INLongRep copy(int from, int to, int n, IN parent) {

            INLongRep target = this;

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
        public long getMemorySize() {
            return super.getMemorySize() +
                MemoryBudget.SPARSE_LONG_REP_OVERHEAD -
                MemoryBudget.DEFAULT_KEYVALS_OVERHEAD +
                MemoryBudget.shortArraySize(idxs.length);
        }
    }
}
