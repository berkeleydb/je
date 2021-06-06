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

import java.util.Comparator;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.utilint.StringUtils;

/**
 * Key represents a JE B-Tree Key.  Keys are immutable.  Within JE, keys are
 * usually represented as byte arrays rather than as Key instances in order to
 * reduce the in-memory footprint. The static methods of this class are used to
 * operate on the byte arrays.
 *
 * One exception is when keys are held within a collection. In that case, Key
 * objects are instantiated so that keys are hashed and compared by value.
 */
public final class Key implements Comparable<Key> {

    public abstract static class DumpType {

        private String name;

        private DumpType(String name) {
            this.name = name;
        }

        public static final DumpType BINARY = new DumpType("BINARY") {
                @Override
                void dumpByteArrayInternal(StringBuilder sb, byte[] b) {
                    for (int i = 0; i < b.length; i++) {
                        sb.append(b[i] & 0xFF).append(" ");
                    }
                }
            };

        public static final DumpType HEX = new DumpType("HEX") {
                @Override
                void dumpByteArrayInternal(StringBuilder sb, byte[] b) {
                    for (int i = 0; i < b.length; i++) {
                        sb.append(Integer.toHexString(b[i] & 0xFF)).
                            append(" ");
                    }
                }
            };

        public static final DumpType TEXT = new DumpType("TEXT") {
                @Override
                void dumpByteArrayInternal(StringBuilder sb, byte[] b) {
                    sb.append(StringUtils.fromUTF8(b));
                }
            };

        public static final DumpType OBFUSCATE = new DumpType("OBFUSCATE") {
                @Override
                void dumpByteArrayInternal(StringBuilder sb, byte[] b) {
                    int len = b.length;
                    sb.append("[").append(len).
                        append(len == 1 ? " byte]" : " bytes]");
                }
            };

        public String dumpByteArray(byte[] b) {
            StringBuilder sb = new StringBuilder();
            if (b != null) {
                dumpByteArrayInternal(sb, b);
            } else {
                sb.append("null");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return name;
        }

        abstract void dumpByteArrayInternal(StringBuilder sb, byte[] b);
    }

    public static DumpType DUMP_TYPE = DumpType.BINARY;

    public static final byte[] EMPTY_KEY = new byte[0];

    private byte[] key;

    /**
     * Construct a new key from a byte array.
     */
    public Key(byte[] key) {
        if (key == null) {
            this.key = null;
        } else {
            this.key = new byte[key.length];
            System.arraycopy(key, 0, this.key, 0, key.length);
        }
    }

    public static byte[] makeKey(DatabaseEntry dbt) {
        byte[] entryKey = dbt.getData();
        if (entryKey == null) {
            return EMPTY_KEY;
        } else {
            byte[] newKey = new byte[dbt.getSize()];
            System.arraycopy(entryKey, dbt.getOffset(), newKey,
                             0, dbt.getSize());
            return newKey;
        }
    }

    /**
     * Get the byte array for the key.
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Compare two keys.  Standard compareTo function and returns.
     *
     * Note that any configured user comparison function is not used, and
     * therefore this method should not be used for comparison of keys during
     * Btree operations.
     */
    public int compareTo(Key argKey) {
        return compareUnsignedBytes(this.key, argKey.key);
    }

    /**
     * Support Set of Key in BINReference.
     */
    @Override
    public boolean equals(Object o) {
        return (o instanceof Key) && (compareTo((Key)o) == 0);
    }

    /**
     * Support HashSet of Key in BINReference.
     */
    @Override
    public int hashCode() {
        int code = 0;
        for (int i = 0; i < key.length; i += 1) {
            code += key[i];
        }
        return code;
    }

    /**
     * Compare keys with an optional comparator.
     */
    public static int compareKeys(byte[] key1,
                                  int off1,
                                  int len1,
                                  byte[] key2,
                                  int off2,
                                  int len2,
                                  Comparator<byte[]> comparator) {
        if (comparator == null) {
            return compareUnsignedBytes(key1, off1, len1,
                                        key2, off2, len2);
        }
        if (off1 != 0 || len1 != key1.length) {
            final byte[] b = new byte[len1];
            System.arraycopy(key1, off1, b, 0, len1);
            key1 = b;
        }
        if (off2 != 0 || len2 != key2.length) {
            final byte[] b = new byte[len2];
            System.arraycopy(key2, off2, b, 0, len2);
            key2 = b;
        }
        return comparator.compare(key1, key2);
    }

    /**
     * Compare keys with an optional comparator.
     */
    public static int compareKeys(byte[] key1,
                                  byte[] key2,
                                  Comparator<byte[]> comparator) {
        if (comparator != null) {
            return comparator.compare(key1, key2);
        } else {
            return compareUnsignedBytes(key1, key2);
        }
    }

    /**
     * Compare keys with an optional comparator.
     */
    public static int compareKeys(DatabaseEntry entry1,
                                  DatabaseEntry entry2,
                                  Comparator<byte[]> comparator) {
        byte[] key1 = Key.makeKey(entry1);
        byte[] key2 = Key.makeKey(entry2);
        if (comparator != null) {
            return comparator.compare(key1, key2);
        } else {
            return compareUnsignedBytes(key1, key2);
        }
    }

    /**
     * Compare using a default unsigned byte comparison.
     */
    private static int compareUnsignedBytes(byte[] key1, byte[] key2) {
        return compareUnsignedBytes(key1, 0, key1.length,
                                    key2, 0, key2.length);
    }

    /**
     * Compare using a default unsigned byte comparison.
     */
    public static int compareUnsignedBytes(byte[] key1,
                                           int off1,
                                           int len1,
                                           byte[] key2,
                                           int off2,
                                           int len2) {
        int limit = Math.min(len1, len2);

        for (int i = 0; i < limit; i++) {
            byte b1 = key1[i + off1];
            byte b2 = key2[i + off2];
            if (b1 == b2) {
                continue;
            } else {

                /* 
                 * Remember, bytes are signed, so convert to shorts so that we
                 * effectively do an unsigned byte comparison.
                 */
                return (b1 & 0xff) - (b2 & 0xff);
            }
        }

        return (len1 - len2);
    }

    /*
     * Return the length of the common prefix between 2 keys. The 1st key
     * consists of the first "a1Len" bytes of "key1". The second key is
     * "key2".
     */
    public static int getKeyPrefixLength(byte[] key1, int a1Len, byte[] key2) {
        assert key1 != null && key2 != null;

        int a2Len = key2.length;

        int limit = Math.min(a1Len, a2Len);

        for (int i = 0; i < limit; i++) {
            byte b1 = key1[i];
            byte b2 = key2[i];
            if (b1 != b2) {
                return i;
            }
        }

        return limit;
    }

    /*
     * Return a new byte[] containing the common prefix of key1 and key2.
     * Return null if there is no common prefix.
     */
    public static byte[] createKeyPrefix(byte[] key1, byte[] key2) {

        int len = getKeyPrefixLength(key1, key1.length, key2);
        if (len == 0) {
            return null;
        }

        byte[] ret = new byte[len];
        System.arraycopy(key1, 0, ret, 0, len);

        return ret;
    }

    public static String dumpString(byte[] key, int nspaces) {

        return dumpString(key, "key", nspaces);
    }

    public static String dumpString(byte[] key, String xmltag, int nspaces) {

        StringBuilder sb = new StringBuilder();

        sb.append(TreeUtils.indent(nspaces));
        sb.append("<").append(xmltag).append(" v=\"");

        sb.append(getNoFormatString(key));

        sb.append("\"/>");

        return sb.toString();
    }

    /**
     * Print the string w/out XML format.
     */
    public static String getNoFormatString(byte[] key) {

        StringBuilder sb = new StringBuilder();

        if (DUMP_TYPE == DumpType.BINARY ||
            DUMP_TYPE == DumpType.HEX) {
            if (key == null) {
                sb.append("<null>");
            } else {
                sb.append(DUMP_TYPE.dumpByteArray(key));
            }
        } else if (DUMP_TYPE == DumpType.TEXT) {
            sb.append(key == null ? "" : StringUtils.fromUTF8(key));
        } else if (DUMP_TYPE == DumpType.OBFUSCATE) {
            if (key == null) {
                sb.append("<null>");
            } else {
                int len = key.length;
                sb.append("[").append(len);
                sb.append(len == 1 ? " byte]" : " bytes]");
            }
        }

        return sb.toString();
    }
}
