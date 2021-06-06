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
import java.util.Comparator;

import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * The abstract class that defines the various formats used to represent
 * the keys associated with the IN node. The class is also used to store
 * embedded records, where the actual key and the data portion of a record are
 * stored together as a single byte sequence.
 * 
 * There are currently two supported representations:
 * <ol>
 * <li>A default representation <code>Default</code> that's capable of holding
 * any set of keys.</li>
 * <li>
 * A compact representation <code>MaxKeySize</code> that's more efficient for
 * holding small keys (LTE 16 bytes) in length. If key prefixing is in use this
 * represents the unprefixed part of the key, since that's what is stored in
 * this array.</li>
 * </ol>
 * <p>
 * The choice of representation is made when an IN node is first read in from
 * the log. The <code>MaxKeySize</code> representation is only used when it is
 * more storage efficient than the default representation for the set of keys
 * currently associated with the IN.
 * <p>
 * Note that no attempt is currently made to optimize the storage
 * representation as keys are added to, or removed from, the
 * <code>Default</code> representation to minimize the chances of transitionary
 * "back and forth" representation changes that could prove to be expensive.
 */
public abstract class INKeyRep
    extends INArrayRep<INKeyRep, INKeyRep.Type, byte[]> {

    /* The different representations for keys. */
    public enum Type { DEFAULT, MAX_KEY_SIZE };

    public INKeyRep() {
    }

    public abstract int length();

    /**
     * Returns true if the key bytes mem usage is accounted for internally
     * here, or false if each key has a separate byte array and its mem usage
     * is accounted for by the parent.
     */
    public abstract boolean accountsForKeyByteMemUsage();

    public abstract int size(int idx);

    public abstract INKeyRep set(int idx, byte[] key, byte[] data, IN parent);

    public abstract INKeyRep setData(int idx, byte[] data, IN parent);

    public abstract byte[] getData(int idx);

    public abstract byte[] getKey(int idx, boolean embeddedData);

    public abstract byte[] getFullKey(
        byte[] prefix,
        int idx,
        boolean embeddedData);

    public abstract int compareKeys(
        byte[] searchKey,
        byte[] prefix,
        int idx,
        boolean embeddedData,
        Comparator<byte[]> comparator);

    /**
     * The default representation that's capable of storing keys of any size.
     */
    public static class Default extends INKeyRep {

        private final byte[][] keys;

        Default(int nodeMaxEntries) {
            this.keys = new byte[nodeMaxEntries][];
        }

        public Default(@SuppressWarnings("unused") SizeofMarker marker) {
            keys = null;
        }

        private Default(byte[][] keys) {
            this.keys = keys;
        }

        @Override
        public INKeyRep resize(int capacity) {
            return new Default(Arrays.copyOfRange(keys, 0, capacity));
        }

        @Override
        public Type getType() {
            return Type.DEFAULT;
        }

        @Override
        public int length() {
            return keys.length;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, IN parent) {
            keys[idx] = key;
            return this;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, byte[] data, IN parent) {

            if (data == null || data.length == 0) {
                keys[idx] = key;
            } else {
                keys[idx] = DupKeyData.combine(key, data);
            }
            return this;
        }

        @Override
        public INKeyRep setData(int idx, byte[] data, IN parent) {

            /*
             * TODO #21488: optimize this to avoid creation of new combined
             * key, when possible.
             */
            return set(idx, getKey(idx, true), data, parent);
        }

        @Override
        public int size(int idx) {
            return keys[idx].length;
        }

        @Override
        public byte[] get(int idx) {
            return keys[idx];
        }

        @Override
        public byte[] getData(int idx) {

            assert(keys[idx] != null);
            return DupKeyData.getData(keys[idx], 0, keys[idx].length);
        }

        @Override
        public byte[] getKey(int idx, boolean embeddedData) {

            byte[] suffix = keys[idx];

            if (suffix == null) {
                return Key.EMPTY_KEY;
            } else if (embeddedData) {
                return DupKeyData.getKey(suffix, 0, suffix.length);
            } else {
                return suffix;
            }
        }

        @Override
        public byte[] getFullKey(
            byte[] prefix,
            int idx, 
            boolean embeddedData) {

            if (prefix == null || prefix.length == 0) {
                return getKey(idx, embeddedData);
            }

            byte[] suffix = keys[idx];

            if (suffix == null) {
                assert(!embeddedData);
                suffix = Key.EMPTY_KEY;
            }

            int prefixLen = prefix.length;
            int suffixLen;

            if (embeddedData) {
                suffixLen = DupKeyData.getKeyLength(suffix, 0, suffix.length);
            } else {
                suffixLen = suffix.length;
            }

            final byte[] key = new byte[prefixLen + suffixLen];
            System.arraycopy(prefix, 0, key, 0, prefixLen);
            System.arraycopy(suffix, 0, key, prefixLen, suffixLen);

            return key;
        }

        @Override
        public int compareKeys(
            byte[] searchKey,
            byte[] prefix,
            int idx,
            boolean embeddedData,
            Comparator<byte[]> comparator)
        {
            if (comparator != null) {
                byte[] myKey = getFullKey(prefix, idx, embeddedData);
                return  Key.compareKeys(searchKey, myKey, comparator);
            }

            int cmp = 0;

            if (prefix == null || prefix.length == 0) {

                return compareSuffixes(
                    searchKey, 0, searchKey.length, idx, embeddedData);
            }

            cmp = Key.compareUnsignedBytes(
                searchKey, 0, Math.min(searchKey.length, prefix.length),
                prefix, 0, prefix.length);

            if (cmp == 0) {

                int searchKeyOffset = prefix.length;
                int searchKeyLen = searchKey.length - prefix.length;

                return compareSuffixes(
                    searchKey, searchKeyOffset, searchKeyLen,
                    idx, embeddedData);
            }

            return cmp;
        }

        private int compareSuffixes(
            byte[] searchKey,
            int searchKeyOff,
            int searchKeyLen,
            int idx,
            boolean embeddedData) {

            byte[] myKey = keys[idx];
            int myKeyLen;

            if (myKey == null) {
                myKey = Key.EMPTY_KEY;
                myKeyLen = 0;
            } else if (embeddedData) {
                myKeyLen = DupKeyData.getKeyLength(myKey, 0, myKey.length);
            } else {
                myKeyLen = myKey.length;
            }

            return Key.compareUnsignedBytes(
                searchKey, searchKeyOff, searchKeyLen, myKey, 0, myKeyLen);
        }


        @Override
        public INKeyRep copy(int from, int to, int n, IN parent) {
            System.arraycopy(keys, from, keys, to, n);
            return this;
        }

        /**
         * Evolves to the MaxKeySize representation if that is more efficient
         * for the current set of keys. Note that since all the keys must be
         * examined to make the decision, there is a reasonable cost to the
         * method and it should not be invoked indiscriminately.
         */
        @Override
        public INKeyRep compact(IN parent) {

            if (keys.length > MaxKeySize.MAX_KEYS) {
                return this;
            }

            final int compactMaxKeyLength = parent.getCompactMaxKeyLength();
            if (compactMaxKeyLength <= 0) {
                return this;
            }

            int keyCount = 0;
            int maxKeyLength = 0;
            int defaultKeyBytes = 0;

            for (byte[] key : keys) {
                if (key != null) {
                    keyCount++;
                    if (key.length > maxKeyLength) {
                        maxKeyLength = key.length;
                        if (maxKeyLength > compactMaxKeyLength) {
                            return this;
                        }
                    }
                    defaultKeyBytes += MemoryBudget.byteArraySize(key.length);
                }
            }

            if (keyCount == 0) {
                return this;
            }

            long defaultSizeWithKeys = calculateMemorySize() + defaultKeyBytes;

            if (defaultSizeWithKeys >
                MaxKeySize.calculateMemorySize(keys.length, maxKeyLength)) {
                return compactToMaxKeySizeRep(maxKeyLength, parent);
            }

            return this;
        }

        private MaxKeySize compactToMaxKeySizeRep(
            int maxKeyLength,
            IN parent) {
            
            MaxKeySize newRep =
                new MaxKeySize(keys.length, (short) maxKeyLength);

            for (int i = 0; i < keys.length; i++) {
                INKeyRep rep = newRep.set(i, keys[i], parent);
                assert rep == newRep; /* Rep remains unchanged. */
            }

            noteRepChange(newRep, parent);

            return newRep;
        }

        @Override
        public long calculateMemorySize() {

            /*
             * Assume empty keys array. The memory consumed by the actual keys
             * is accounted for by the IN.getEntryInMemorySize() method.
             */
            return MemoryBudget.DEFAULT_KEYVALS_OVERHEAD +
                MemoryBudget.objectArraySize(keys.length);
        }

        @Override
        public boolean accountsForKeyByteMemUsage() {
            return false;
        }

        @Override
        void updateCacheStats(@SuppressWarnings("unused") boolean increment,
                              @SuppressWarnings("unused") Evictor evictor) {
            /* No stats for the default representation. */
        }
    }

    /**
     * The compact representation that can be used to represent keys LTE 16
     * bytes in length. The keys are all represented inside a single byte array
     * instead of having one byte array per key. Within the array, all keys are
     * assigned a storage size equal to that taken up by the longest key, plus
     * one byte to hold the actual key length. This makes key retreival fast.
     * However, insertion and deletion for larger keys moves bytes proportional
     * to the storage length of the keys. This is why the representation is
     * restricted to keys LTE 16 bytes in size.
     *
     * On a 32 bit VM the per key overhead for the Default representation is 4
     * bytes for the pointer + 16 bytes for each byte array key object, for a
     * total of 20 bytes/key. On a 64 bit machine the overheads are much
     * larger: 8 bytes for the pointer plus 24 bytes per key.
     *
     * The more fully populated the IN the more the savings with this
     * representation since the single byte array is sized to hold all the keys
     * regardless of the actual number of keys that are present.
     *
     * It's worth noting that the storage savings here are realized in addition
     * to the storage benefits of key prefixing, since the keys stored in the
     * key array are the smaller key values after the prefix has been stripped,
     * reducing the length of the key and making it more likely that it's small
     * enough for this specialized representation.
     */
    public static class MaxKeySize extends INKeyRep {

        private static final int LENGTH_BYTES = 1;
        public static final byte DEFAULT_MAX_KEY_LENGTH = 16;
        public static final int MAX_KEYS = 256;
        private static final byte NULL_KEY = Byte.MAX_VALUE;

        /*
         * The array is sized to hold all the keys associated with the IN node.
         * Each key is allocated a fixed amount of storage equal to the maximum
         * length of all the keys in the IN node + 1 byte to hold the size of
         * each key. The length is biased, by -128. That is, a zero length
         * key is represented by -128, a 1 byte key by -127, etc.
         */
        private final byte[] keys;

        /* 
         * The number of butes used to store each key ==
         * DEFAULT_MAX_KEY_LENGTH (16) + LENGTH_BYTES (1) 
         */
        private final short fixedKeyLen;

        public MaxKeySize(int nodeMaxEntries, short maxKeyLen) {

            assert maxKeyLen < 255;
            this.fixedKeyLen = (short) (maxKeyLen + LENGTH_BYTES);
            this.keys = new byte[fixedKeyLen * nodeMaxEntries];

            for (int i = 0; i < nodeMaxEntries; i++) {
                INKeyRep rep = set(i, null, null);
                assert rep == this; /* Rep remains unchanged. */
            }
        }

        /* Only for use by Sizeof */
        public MaxKeySize(@SuppressWarnings("unused") SizeofMarker marker) {
            keys = null;
            fixedKeyLen = 0;
        }

        private MaxKeySize(byte[] keys, short fixedKeyLen) {
            this.keys = keys;
            this.fixedKeyLen = fixedKeyLen;
        }

        @Override
        public INKeyRep resize(int capacity) {
            return new MaxKeySize(
                Arrays.copyOfRange(keys, 0, capacity * fixedKeyLen),
                fixedKeyLen);
        }

        @Override
        public Type getType() {
            return Type.MAX_KEY_SIZE;
        }

        @Override
        public int length() {
            return keys.length / fixedKeyLen;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, IN parent) {

            int slotOff = idx * fixedKeyLen;

            if (key == null) {
                keys[slotOff] = NULL_KEY;
                return this;
            }

            if (key.length >= fixedKeyLen) {
                Default newRep = expandToDefaultRep(parent);
                return newRep.set(idx, key, parent);
            }

            keys[slotOff] = (byte) (key.length + Byte.MIN_VALUE);

            slotOff += LENGTH_BYTES;

            System.arraycopy(key, 0, keys, slotOff, key.length);

            return this;
        }

        @Override
        public INKeyRep set(int idx, byte[] key, byte[] data, IN parent) {

            if (data == null || data.length == 0) {
                return set(idx, key, parent);
            }

            byte[] twoPartKey = DupKeyData.combine(key, data);

            return set(idx, twoPartKey, parent);
        }

        @Override
        public INKeyRep setData(int idx, byte[] data, IN parent) {

            /*
             * TODO #21488: optimize this to avoid creation of new combined
             * key, when possible.
             */
            return set(idx, getKey(idx, true), data, parent);
        }

        private Default expandToDefaultRep(IN parent) {

            final int capacity = length();
            final Default newRep = new Default(capacity);

            for (int i = 0; i < capacity; i++) {
                final byte[] k = get(i);
                INKeyRep rep = newRep.set(i, k, parent);
                assert rep == newRep; /* Rep remains unchanged. */
            }

            noteRepChange(newRep, parent);
            return newRep;
        }

        @Override
        public int size(int idx) {

            int slotOff = idx * fixedKeyLen;

            assert keys[slotOff] != NULL_KEY;

            return keys[slotOff] - Byte.MIN_VALUE;
        }

        @Override
        public byte[] get(int idx) {

            int slotOff = idx * fixedKeyLen;

            if (keys[slotOff] == NULL_KEY) {
                return null;
            }

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            byte[] info = new byte[slotLen];
            System.arraycopy(keys, slotOff, info, 0, slotLen);
            return info;
        }

        @Override
        public byte[] getData(int idx) {

            int slotOff = idx * fixedKeyLen;

            assert(keys[slotOff] != NULL_KEY);

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            return DupKeyData.getData(keys, slotOff, slotLen);
        }

        @Override
        public byte[] getKey(int idx, boolean embeddedData) {

            int slotOff = idx * fixedKeyLen;

            if (keys[slotOff] == NULL_KEY) {
                assert(!embeddedData);
                return Key.EMPTY_KEY;
            }

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            if (embeddedData) {
                return DupKeyData.getKey(keys, slotOff, slotLen);
            }

            byte[] key = new byte[slotLen];
            System.arraycopy(keys, slotOff, key, 0, slotLen);
            return key;
        }

        @Override
        public byte[] getFullKey(
            byte[] prefix,
            int idx,
            boolean embeddedData) {

            if (prefix == null || prefix.length == 0) {
                return getKey(idx, embeddedData);
            }

            int slotOff = idx * fixedKeyLen;

            if (keys[slotOff] == NULL_KEY) {
                assert(!embeddedData);
                return prefix;
            }

            int slotLen = keys[slotOff] - Byte.MIN_VALUE;

            slotOff += LENGTH_BYTES;

            int prefixLen = prefix.length;
            int suffixLen;

            if (embeddedData) {
                suffixLen = DupKeyData.getKeyLength(keys, slotOff, slotLen);
            } else {
                suffixLen = slotLen;
            }

            byte[] key = new byte[suffixLen + prefixLen];
            System.arraycopy(prefix, 0, key, 0, prefixLen);
            System.arraycopy(keys, slotOff, key, prefixLen, suffixLen);
            return key;
        }

        @Override
        public int compareKeys(
            byte[] searchKey,
            byte[] prefix,
            int idx,
            boolean embeddedData,
            Comparator<byte[]> comparator) {

            if (comparator != null) {
                byte[] myKey = getFullKey(prefix, idx, embeddedData);
                return  Key.compareKeys(searchKey, myKey, comparator);
            }

            int cmp = 0;

            if (prefix == null || prefix.length == 0) {

                return compareSuffixes(
                    searchKey, 0, searchKey.length, idx, embeddedData);
            }

            cmp = Key.compareUnsignedBytes(
                searchKey, 0, Math.min(searchKey.length, prefix.length),
                prefix, 0, prefix.length);

            if (cmp == 0) {

                int searchKeyOff = prefix.length;
                int searchKeyLen = searchKey.length - prefix.length;

                return compareSuffixes(
                    searchKey, searchKeyOff, searchKeyLen,
                    idx, embeddedData);
            }

            return cmp;
        }

        private int compareSuffixes(
            byte[] searchKey,
            int searchKeyOff,
            int searchKeyLen,
            int idx,
            boolean embeddedData) {

            int myKeyOff = idx * fixedKeyLen;
            int myKeyLen = 0;

            if (keys[myKeyOff] != NULL_KEY) {

                myKeyLen = keys[myKeyOff] - Byte.MIN_VALUE;

                myKeyOff += LENGTH_BYTES;

                if (embeddedData) {
                    myKeyLen = DupKeyData.getKeyLength(
                        keys, myKeyOff, myKeyLen);
                }
            } else {
                assert(!embeddedData);
                myKeyOff += LENGTH_BYTES;
            }

            return Key.compareUnsignedBytes(
                searchKey, searchKeyOff, searchKeyLen,
                keys, myKeyOff, myKeyLen);
        }

        @Override
        public INKeyRep copy(int from, int to, int n, IN parent) {
            System.arraycopy(keys, (from * fixedKeyLen),
                             keys, (to * fixedKeyLen),
                             n * fixedKeyLen);
            return this;
        }

        @Override
        public INKeyRep compact(@SuppressWarnings("unused") IN parent) {
            /* It's as compact as it gets. */
            return this;
        }

        @Override
        public long calculateMemorySize() {
            return MemoryBudget.MAX_KEY_SIZE_KEYVALS_OVERHEAD +
                   MemoryBudget.byteArraySize(keys.length);
        }

        private static long calculateMemorySize(int maxKeys, int maxKeySize) {
            return MemoryBudget.MAX_KEY_SIZE_KEYVALS_OVERHEAD +
                   MemoryBudget.byteArraySize(maxKeys *
                                              (maxKeySize + LENGTH_BYTES));
        }

        @Override
        public boolean accountsForKeyByteMemUsage() {
            return true;
        }

        @Override
        void updateCacheStats(boolean increment, Evictor evictor) {
            if (increment) {
                evictor.getNINCompactKey().incrementAndGet();
            } else {
                evictor.getNINCompactKey().decrementAndGet();
            }
        }
    }
}
