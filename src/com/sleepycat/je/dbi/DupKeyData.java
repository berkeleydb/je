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

package com.sleepycat.je.dbi;

import java.util.Comparator;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.util.PackedInteger;

/**
 * Utility methods for combining, splitting and comparing two-part key values
 * for duplicates databases.
 *
 * At the Btree storage level, for the key/data pairs in a duplicates database,
 * the data is always zero length and the key is a two-part key. For embedded
 * records, the key and data parts are visible at the BTree level as well. In
 * both cases, the 'key' parameter in the API is the first part of the key. 
 * The the 'data' parameter in the API is the second part of the key.
 *
 * The length of the first part is stored at the end of the combined key as a
 * packed integer, so that the two parts can be split, combined, and compared
 * separately.  The length is stored at the end, rather than the start, to
 * enable key prefixing for the first part, e.g., for Strings with different
 * lengths but common prefixes.
 */
public class DupKeyData {

    public static final int PREFIX_ONLY = -1;

    /**
     * Returns twoPartKey as:
     *   paramKey bytes,
     *   paramData bytes,
     *   reverse-packed len of paramKey bytes.
     *
     * The byte array in the resulting twoPartKey will be copied again by JE at
     * a lower level.  It would be nice if there were a way to give ownership
     * of the array to JE, to avoid the extra copy.
     */
    public static DatabaseEntry combine(final DatabaseEntry paramKey,
                                        final DatabaseEntry paramData) {
        final byte[] buf = combine
            (paramKey.getData(), paramKey.getOffset(), paramKey.getSize(),
             paramData.getData(), paramData.getOffset(), paramData.getSize());
        return new DatabaseEntry(buf);
    }

    public static byte[] combine(final byte[] key, final byte[] data) {
        return combine(key, 0, key.length, data, 0, data.length);
    }

    public static byte[] combine(final byte[] key,
                                 final int keyOff,
                                 final int keySize,
                                 final byte[] data,
                                 final int dataOff,
                                 final int dataSize) {
        final int keySizeLen = PackedInteger.getWriteIntLength(keySize);
        final byte[] buf = new byte[keySizeLen + keySize + dataSize];
        System.arraycopy(key, keyOff, buf, 0, keySize);
        System.arraycopy(data, dataOff, buf, keySize, dataSize);
        final int nextOff =
            PackedInteger.writeReverseInt(buf, keySize + dataSize, keySize);
        assert nextOff == buf.length;
        return buf;
    }

    /**
     * Splits twoPartKey, previously set by combine, into original paramKey and
     * paramData if they are non-null.
     *
     * The offset of the twoPartKey must be zero.  This can be assumed because
     * the entry is read from the database and JE always returns entries with a
     * zero offset.
     *
     * This method copies the bytes into to new arrays rather than using the
     * DatabaseEntry offset and size to shared the array, to keep with the
     * convention that JE always returns whole arrays.  It would be nice to
     * avoid the copy, but that might break user apps.
     */
    public static void split(final DatabaseEntry twoPartKey,
                             final DatabaseEntry paramKey,
                             final DatabaseEntry paramData) {
        assert twoPartKey.getOffset() == 0;
        split(twoPartKey.getData(), twoPartKey.getSize(), paramKey, paramData);
    }

    /**
     * Same as split method above, but with twoPartKey/twoPartKeySize byte
     * array and array size params.
     */
    public static void split(final byte[] twoPartKey,
                             final int twoPartKeySize,
                             final DatabaseEntry paramKey,
                             final DatabaseEntry paramData) {
        final int keySize =
            PackedInteger.readReverseInt(twoPartKey, twoPartKeySize - 1);
        assert keySize != PREFIX_ONLY;

        if (paramKey != null) {
            final byte[] keyBuf = new byte[keySize];
            System.arraycopy(twoPartKey, 0, keyBuf, 0, keySize);

            if (keySize == 0 || paramKey.getPartial()) {
                LN.setEntry(paramKey, keyBuf);
            } else {
                paramKey.setData(keyBuf, 0, keySize);
            }
        }

        if (paramData != null) {
            final int keySizeLen =
                PackedInteger.getReadIntLength(twoPartKey, twoPartKeySize - 1);

            final int dataSize = twoPartKeySize - keySize - keySizeLen;
            final byte[] dataBuf = new byte[dataSize];
            System.arraycopy(twoPartKey, keySize, dataBuf, 0, dataSize);

            if (dataSize == 0 || paramData.getPartial()) {
                LN.setEntry(paramData, dataBuf);
            } else {
                paramData.setData(dataBuf, 0, dataSize);
            }
        }
    }

    /**
     * Splits twoPartKey and returns a two-part key entry containing the key
     * portion of twoPartKey combined with newData.
     */
    public static byte[] replaceData(final byte[] twoPartKey,
                                     final byte[] newData) {
        final int origKeySize =
            PackedInteger.readReverseInt(twoPartKey, twoPartKey.length - 1);
        final int keySize = (origKeySize == PREFIX_ONLY) ?
            (twoPartKey.length - 1) :
            origKeySize;
        return combine(twoPartKey, 0, keySize, newData, 0, newData.length);
    }

    /**
     * Splits twoPartKey and returns a two-part key entry containing the key
     * portion from twoPartKey, no data, and the special PREFIX_ONLY value for
     * the key length.  When used for a search, this will compare as less than
     * any other entry having the same first part, i.e., in the same duplicate
     * set.
     */
    public static DatabaseEntry removeData(final byte[] twoPartKey) {
        final int keySize =
            PackedInteger.readReverseInt(twoPartKey, twoPartKey.length - 1);
        assert keySize != PREFIX_ONLY;
        return new DatabaseEntry(makePrefixKey(twoPartKey, 0, keySize));
    }

    /**
     * Returns a two-part key entry with the given key portion, no data, and
     * the special PREFIX_ONLY value for the key length.  When used for a
     * search, this will compare as less than any other entry having the same
     * first part, i.e., in the same duplicate set.
     */
    public static byte[] makePrefixKey(
        final byte[] key,
        final int keyOff,
        final int keySize) {

        final byte[] buf = new byte[keySize + 1];
        System.arraycopy(key, 0, buf, 0, keySize);
        buf[keySize] = (byte) PREFIX_ONLY;
        return buf;
    }

    public static int getKeyLength(final byte[] buf, int off, int len) {

        assert(buf.length >= off + len);

        int keyLen = PackedInteger.readReverseInt(buf, off + len - 1);
        assert(keyLen != PREFIX_ONLY);
        assert(keyLen >= 0 && keyLen <= len);

        return keyLen;
    }

    public static byte[] getKey(final byte[] buf, int off, int len) {

        assert(buf.length >= off + len);

        int keyLen = PackedInteger.readReverseInt(buf, off + len - 1);
        assert(keyLen != PREFIX_ONLY);
        assert(keyLen >= 0 && keyLen <= len);

        byte[] key = new byte[keyLen];
        System.arraycopy(buf, off, key, 0, keyLen);

        return key;
    }

    public static byte[] getData(final byte[] buf, int off, int len) {

        assert(buf.length >= off + len);

       int keyLen = PackedInteger.readReverseInt(buf, off + len - 1);
       assert(keyLen != PREFIX_ONLY);
       assert(keyLen >= 0 && keyLen <= len);

       int keyLenSize = PackedInteger.getReadIntLength(buf, off + len - 1);

       int dataLen = len - keyLen - keyLenSize;
       assert(dataLen > 0);
       assert(keyLen + dataLen <= len);

       byte[] data = new byte[dataLen];
       System.arraycopy(buf, off + keyLen, data, 0, dataLen);
       return data;
    }

    /**
     * Comparator that compares the combined key/data two-part key, calling the
     * user-defined btree and duplicate comparator as needed.
     */
    public static class TwoPartKeyComparator implements Comparator<byte[]> {

        private final Comparator<byte[]> btreeComparator;
        private final Comparator<byte[]> duplicateComparator;

        public TwoPartKeyComparator(final Comparator<byte[]> btreeComparator,
                                    final Comparator<byte[]> dupComparator) {
            this.btreeComparator = btreeComparator;
            this.duplicateComparator = dupComparator;
        }

        public int compare(final byte[] twoPartKey1,
                           final byte[] twoPartKey2) {

            /* Compare key portion. */
            final int origKeySize1 = PackedInteger.readReverseInt
                (twoPartKey1, twoPartKey1.length - 1);

            final int keySize1 = (origKeySize1 == PREFIX_ONLY) ?
                (twoPartKey1.length - 1) :
                origKeySize1;

            final int origKeySize2 = PackedInteger.readReverseInt
                (twoPartKey2, twoPartKey2.length - 1);

            final int keySize2 = (origKeySize2 == PREFIX_ONLY) ?
                (twoPartKey2.length - 1) :
                origKeySize2;

            final int keyCmp;

            if (btreeComparator == null) {
                keyCmp = Key.compareUnsignedBytes(
                    twoPartKey1, 0, keySize1, twoPartKey2, 0, keySize2);
            } else {
                final byte[] key1 = new byte[keySize1];
                final byte[] key2 = new byte[keySize2];
                System.arraycopy(twoPartKey1, 0, key1, 0, keySize1);
                System.arraycopy(twoPartKey2, 0, key2, 0, keySize2);
                keyCmp = btreeComparator.compare(key1, key2);
            }

            if (keyCmp != 0) {
                return keyCmp;
            }

            if (origKeySize1 == PREFIX_ONLY || origKeySize2 == PREFIX_ONLY) {
                if (origKeySize1 == origKeySize2) {
                    return 0;
                }
                return (origKeySize1 == PREFIX_ONLY) ? -1 : 1;
            }

            /* Compare data portion. */
            final int keySizeLen1 = PackedInteger.getReadIntLength
                (twoPartKey1, twoPartKey1.length - 1);
            final int keySizeLen2 = PackedInteger.getReadIntLength
                (twoPartKey2, twoPartKey2.length - 1);

            final int dataSize1 = twoPartKey1.length - keySize1 - keySizeLen1;
            final int dataSize2 = twoPartKey2.length - keySize2 - keySizeLen2;

            final int dataCmp;
            if (duplicateComparator == null) {
                dataCmp = Key.compareUnsignedBytes
                    (twoPartKey1, keySize1, dataSize1,
                     twoPartKey2, keySize2, dataSize2);
            } else {
                final byte[] data1 = new byte[dataSize1];
                final byte[] data2 = new byte[dataSize2];
                System.arraycopy(twoPartKey1, keySize1, data1, 0, dataSize1);
                System.arraycopy(twoPartKey2, keySize2, data2, 0, dataSize2);
                dataCmp = duplicateComparator.compare(data1, data2);
            }
            return dataCmp;
        }
    }

    /**
     * Used to perform the getNextNoDup operation.
     *
     * Compares the left parameter (the key parameter in a user-initiated
     * search operation) as:
     *  - less than a right operand with a prefix with is less than the
     *    prefix of the left operand.  This is standard.
     *  - greater than a right operand with a prefix with is greater than the
     *    prefix of the left operand.  This is standard.
     *  - greater than a right operand with a prefix equal to the prefix of
     *    the left operation.  This is non-standard.
     *
     * The last property causes the range search to find the first duplicate in
     * the duplicate set following the duplicate set of the left operand.
     */
    public static class NextNoDupComparator implements Comparator<byte[]> {

        private final Comparator<byte[]> btreeComparator;

        public NextNoDupComparator(final Comparator<byte[]> btreeComparator) {
            this.btreeComparator = btreeComparator;
        }

        public int compare(final byte[] twoPartKey1,
                           final byte[] twoPartKey2) {
            final int cmp = compareMainKey(twoPartKey1, twoPartKey2,
                                           btreeComparator);
            return (cmp != 0) ? cmp : 1;
        }
    }

    /**
     * Used to perform the putNoOverwrite operation.  Only used to find the
     * insertion position in the BIN, after the standard comparator is used to
     * find the correct BIN for insertion.  Because it compares part-one only,
     * it prevents insertion of a duplicate for the main key given.
     */
    public static class PutNoOverwriteComparator
        implements Comparator<byte[]> {

        private final Comparator<byte[]> btreeComparator;

        public PutNoOverwriteComparator(final Comparator<byte[]> cmp) {
            this.btreeComparator = cmp;
        }

        public int compare(final byte[] twoPartKey1,
                           final byte[] twoPartKey2) {
            return compareMainKey(twoPartKey1, twoPartKey2, btreeComparator);
        }
    }

    /**
     * Compares the first part of the two keys.
     */
    public static int compareMainKey(
        final byte[] keyBytes1,
        final byte[] keyBytes2,
        final Comparator<byte[]> btreeComparator) {

        final int origKeySize2 =
            PackedInteger.readReverseInt(keyBytes2, keyBytes2.length - 1);
        final int keySize2 = (origKeySize2 == PREFIX_ONLY) ?
            (keyBytes2.length - 1) :
            origKeySize2;
        return compareMainKey(keyBytes1, keyBytes2, 0, keySize2,
                              btreeComparator);
    }

    /**
     * Compares the first part of the two keys.
     */
    public static int compareMainKey(
        final byte[] keyBytes1,
        final byte[] keyBytes2,
        final int keyOff2,
        final int keySize2,
        final Comparator<byte[]> btreeComparator) {

        final int origKeySize1 =
            PackedInteger.readReverseInt(keyBytes1, keyBytes1.length - 1);
        final int keySize1 = (origKeySize1 == PREFIX_ONLY) ?
            (keyBytes1.length - 1) :
            origKeySize1;
        final int keyCmp;
        if (btreeComparator == null) {
            keyCmp = Key.compareUnsignedBytes
                (keyBytes1, 0, keySize1,
                 keyBytes2, keyOff2, keySize2);
        } else {
            final byte[] key1 = new byte[keySize1];
            final byte[] key2 = new byte[keySize2];
            System.arraycopy(keyBytes1, 0, key1, 0, keySize1);
            System.arraycopy(keyBytes2, keyOff2, key2, 0, keySize2);
            keyCmp = btreeComparator.compare(key1, key2);
        }
        return keyCmp;
    }
}
