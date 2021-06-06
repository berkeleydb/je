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

package com.sleepycat.je.log.entry;

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.VersionedLN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * An LNLogEntry is the in-memory image of an LN logrec describing a write op
 * (insertion, update, or deletion) performed by a locker T on a record R.
 * T always locks R in exclusive (WRITE or WRITE_RANGE) mode before performing
 * any write ops on it, and it retains its exclusive lock on R until it
 * terminates (commits or aborts). (Non-transactional lockers can be viewed as
 * "simple" transactions that perform at most one write op, and then
 * immediately commit).
 *
 * On disk, an LN logrec contains :
 *
 * 1 <= version <= 5
 *
 *   LN data
 *   databaseid
 *   key
 *   abortLsn             -- if transactional
 *   abortKnownDeleted    -- if transactional
 *   txn id               -- if transactional
 *   prev LSN of same txn -- if transactional
 *
 * 6 <= versions <= 10 :
 *
 *   databaseid
 *   abortLsn             -- if transactional
 *   abortKnownDeleted    -- if transactional
 *   txn id               -- if transactional
 *   prev LSN of same txn -- if transactional
 *   data
 *   key
 *
 * 11 == version :
 *
 *   databaseid
 *   abortLsn               -- if transactional
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *   txn id                 -- if transactional
 *   prev LSN of same txn   -- if transactional
 *   abort key              -- if haveAbortKey
 *   abort data             -- if haveAbortData
 *   abort vlsn             -- if haveAbortVLSN
 *   data
 *   key
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     embeddedLN, haveAbortKey, haveAbortData, haveAbortVLSN,
 *     abort key, abort data, abort vlsn
 *
 * 12 <= version :
 *
 *   1-byte flags
 *     abortKnownDeleted
 *     embeddedLN
 *     haveAbortKey
 *     haveAbortData
 *     haveAbortVLSN
 *     haveAbortLSN
 *     haveAbortExpiration
 *     haveExpiration
 *   databaseid
 *   abortLsn                -- if transactional and haveAbortLSN
 *   txn id                  -- if transactional
 *   prev LSN of same txn    -- if transactional
 *   abort key               -- if haveAbortKey
 *   abort data              -- if haveAbortData
 *   abort vlsn              -- if haveAbortVLSN
 *   abort expiration        -- if haveAbortExpiration
 *   expiration              -- if haveExpiration
 *   data
 *   key
 *
 *   In forReplication mode, these flags and fields are omitted:
 *     abortKnownDeleted, embeddedLN, haveAbortKey, haveAbortData,
 *     haveAbortVLSN, abort key, abort data, abort vlsn
 *
 * NOTE: LNLogEntry is sub-classed by NameLNLogEntry, which adds some extra
 * fields after the record key.
 */
public class LNLogEntry<T extends LN> extends BaseReplicableEntry<T> {

    private static final byte ABORT_KD_MASK = 0x1;
    private static final byte EMBEDDED_LN_MASK = 0x2;
    private static final byte HAVE_ABORT_KEY_MASK = 0x4;
    private static final byte HAVE_ABORT_DATA_MASK = 0x8;
    private static final byte HAVE_ABORT_VLSN_MASK = 0x10;
    private static final byte HAVE_ABORT_LSN_MASK = 0x20;
    private static final byte HAVE_ABORT_EXPIRATION_MASK = 0x40;
    private static final byte HAVE_EXPIRATION_MASK = (byte) 0x80;

    /**
     * Used for computing the minimum log space used by an LNLogEntry.
     */
    public static final int MIN_LOG_SIZE = 1 + // Flags
                                           1 + // DatabaseId
                                           1 + // LN with zero-length data
                                           LogEntryHeader.MIN_HEADER_SIZE;

    /**
     * The log version when the most recent format change for this entry was
     * made (including any changes to the format of the underlying LN and other
     * loggables).
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 12;

    /*
     * Persistent fields.
     */

    /*
     * The id of the DB containing the record.
     */
    private DatabaseId dbId;

    /*
     * The Txn performing the write op. It is null for non-transactional DBs.
     * On disk we store only the txn id and the LSN of the previous logrec
     * (if any) generated by this txn.
     */
    private Txn txn;

    /*
     * The LSN of the record's "abort" version, i.e., the version to revert to
     * if this logrec must be undone as a result of a txn abort. It is set to
     * the most recent version before the record was locked by the locker T
     * associated with this logrec. Because T locks R before it writes it, the
     * abort version is always a committed version.
     *
     * It is null for non-transactional lockers, because such lockers never
     * abort.
     */
    private long abortLsn = DbLsn.NULL_LSN;

    /*
     * Whether the record's abort version was a deleted version or not.
     */
    private boolean abortKnownDeleted;

    /*
     * The key of the record's abort version, if haveAbortKey is true;
     * null otherwise.
     */
    private byte[] abortKey = null;

    /*
     * The data portion of the record's abort version, if haveAbortData is
     * true; null otherwise.
     */
    private byte[] abortData = null;

    /*
     * The VLSN of the record's abort version, if haveAbortVLSN is true;
     * NULL_VLSN otherwise.
     */
    private long abortVLSN = VLSN.NULL_VLSN_SEQUENCE;

    /* Abort expiration time in days or hours. */
    private int abortExpiration = 0;
    private boolean abortExpirationInHours = false;

    /*
     * True if the logrec stores an abort LSN, which is the case only if
     * (a) this is a transactional logrec (b) the abort LSN is non-null.
     */
    private boolean haveAbortLSN;

    /*
     * True if the logrec stores an abort key, which is the case only if
     * (a) this is a transactional logrec, (b) the record's abort version
     * was embedded in the BIN, and (c) the DB allows key updates.
     */
    private boolean haveAbortKey;

    /*
     * True if the logrec stores abort data, which is the case only if
     * (a) this is a transactional logrec and (b) the record's abort
     * version was embedded in the BIN.
     */
    private boolean haveAbortData;

    /*
     * True if the logrec stores an abort VLSN, which is the case only if
     * (a) this is a transactional logrec (b) the record's abort version
     * was embedded in the BIN, and (c) VLSN caching is enabled.
     */
    private boolean haveAbortVLSN;

    /*
     * True if the logrec stores an abort expiration, which is the case only if
     * (a) this is a transactional logrec (b) the record's abort version has a
     * non-zero expiration.
     */
    private boolean haveAbortExpiration;

    /*
     * True if the logrec stores a non-zero expiration.
     */
    private boolean haveExpiration;

    /*
     * Whether, after the write op described by this logrec, the record is
     * embedded in the BIN or not.
     */
    private boolean embeddedLN;

    /*
     * The LN storing the record's data, after the write op described by this
     * logrec. The ln has a null data value if the write op is a deletion. For
     * replicated DBs, the ln contains the record's VLSN as well.
     */
    private LN ln;

    /*
     * The value of the record's key, after the write op described by this
     * logrec.
     */
    private byte[] key;

    /* Expiration time in days or hours. */
    private int expiration;
    private boolean expirationInHours;

    /*
     * Transient fields.
     */

    /* Transient field for duplicates conversion and user key/data methods. */
    enum DupStatus { UNKNOWN, NEED_CONVERSION, DUP_DB, NOT_DUP_DB }
    private DupStatus dupStatus;

    /* For construction of VersionedLN, when VLSN is preserved. */
    private final Constructor<VersionedLN> versionedLNConstructor;

    /**
     * Creates an instance to read an entry.
     *
     * @param <T> the type of the contained LN
     * @param cls the class of the contained LN
     * @return the log entry
     */
    public static <T extends LN> LNLogEntry<T> create(final Class<T> cls) {
        return new LNLogEntry<>(cls);
    }

    /* Constructor to read an entry. */
    LNLogEntry(final Class<T> cls) {
        super(cls);
        if (cls == LN.class) {
            versionedLNConstructor = getNoArgsConstructor(VersionedLN.class);
        } else {
            versionedLNConstructor = null;
        }
    }

    /* Constructor to write an entry. */
    public LNLogEntry(
        LogEntryType entryType,
        DatabaseId dbId,
        Txn txn,
        long abortLsn,
        boolean abortKD,
        byte[] abortKey,
        byte[] abortData,
        long abortVLSN,
        int abortExpiration,
        boolean abortExpirationInHours,
        byte[] key,
        T ln,
        boolean embeddedLN,
        int expiration,
        boolean expirationInHours) {

        setLogType(entryType);
        this.dbId = dbId;
        this.txn = txn;
        this.abortLsn = abortLsn;
        this.abortKnownDeleted = abortKD;
        this.abortKey = abortKey;
        this.abortData = abortData;
        this.abortVLSN = abortVLSN;
        this.abortExpiration = abortExpiration;
        this.abortExpirationInHours = abortExpirationInHours;

        this.haveAbortLSN = (abortLsn != DbLsn.NULL_LSN);
        this.haveAbortKey = (abortKey != null);
        this.haveAbortData = (abortData != null);
        this.haveAbortVLSN = !VLSN.isNull(abortVLSN);
        this.haveAbortExpiration = (abortExpiration != 0);
        this.haveExpiration = (expiration != 0);

        this.embeddedLN = embeddedLN;
        this.key = key;
        this.ln = ln;
        this.expiration = expiration;
        this.expirationInHours = expirationInHours;

        versionedLNConstructor = null;

        /* A txn should only be provided for transactional entry types. */
        assert(entryType.isTransactional() == (txn != null));
    }

    private void reset() {
        dbId = null;
        txn = null;
        abortLsn = DbLsn.NULL_LSN;
        abortKnownDeleted = false;
        abortKey = null;
        abortData = null;
        abortVLSN = VLSN.NULL_VLSN_SEQUENCE;
        abortExpiration = 0;
        abortExpirationInHours = false;

        haveAbortLSN = false;
        haveAbortKey = false;
        haveAbortData = false;
        haveAbortVLSN = false;
        haveAbortExpiration = false;
        haveExpiration = false;

        embeddedLN = false;
        key = null;
        ln = null;
        expiration = 0;
        expirationInHours = false;

        dupStatus = null;
    }

    @Override
    public void readEntry(
        EnvironmentImpl envImpl,
        LogEntryHeader header,
        ByteBuffer entryBuffer) {

        /* Subclasses must call readBaseLNEntry. */
        assert getClass() == LNLogEntry.class;

        /*
         * Prior to version 8, the optimization to omit the key size was
         * mistakenly not applied to internal LN types such as FileSummaryLN
         * and MapLN, and was only applied to user LN types.  The optimization
         * should be applicable whenever LNLogEntry is not subclassed to add
         * additional fields. [#18055]
         */
        final boolean keyIsLastSerializedField =
            header.getVersion() >= 8 || entryType.isUserLNType();

        readBaseLNEntry(envImpl, header, entryBuffer,
                        keyIsLastSerializedField);
    }

    /**
     * Method shared by LNLogEntry subclasses.
     *
     * @param keyIsLastSerializedField specifies whether the key length can be
     * omitted because the key is the last field.  This should be false when
     * an LNLogEntry subclass adds fields to the serialized format.
     */
    final void readBaseLNEntry(
        EnvironmentImpl envImpl,
        LogEntryHeader header,
        ByteBuffer entryBuffer,
        boolean keyIsLastSerializedField) {

        reset();

        int logVersion = header.getVersion();
        boolean unpacked = (logVersion < 6);
        int recStartPosition = entryBuffer.position();

        if (logVersion >= 12) {
            setFlags(entryBuffer.get());
        }

        /*
         * For log version 6 and above we store the key last so that we can
         * avoid storing the key size. Instead, we derive it from the LN size
         * and the total entry size. The DatabaseId is also packed.
         */
        if (logVersion < 6) {
            /* LN is first for log versions prior to 6. */
            ln = newLNInstance(envImpl);
            ln.readFromLog(entryBuffer, logVersion);
        }

        /* DatabaseImpl Id. */
        dbId = new DatabaseId();
        dbId.readFromLog(entryBuffer, logVersion);

        /* Key. */
        if (logVersion < 6) {
            key = LogUtils.readByteArray(entryBuffer, true/*unpacked*/);
        }

        if (entryType.isTransactional()) {

            /*
             * AbortLsn. If it was a marker LSN that was used to fill in a
             * create, mark it null.
             */
            if (haveAbortLSN || logVersion < 12) {
                abortLsn = LogUtils.readLong(entryBuffer, unpacked);
                if (DbLsn.getFileNumber(abortLsn) ==
                    DbLsn.getFileNumber(DbLsn.NULL_LSN)) {
                    abortLsn = DbLsn.NULL_LSN;
                }
            }

            if (logVersion < 12) {
                setFlags(entryBuffer.get());
                haveAbortLSN = (abortLsn != DbLsn.NULL_LSN);
            }

            /* txn id and prev LSN by same txn. */
            txn = new Txn();
            txn.readFromLog(entryBuffer, logVersion);

        } else if (logVersion == 11) {
            setFlags(entryBuffer.get());
        }

        if (logVersion >= 11) {
            if (haveAbortKey) {
                abortKey = LogUtils.readByteArray(entryBuffer, false);
            }
            if (haveAbortData) {
                abortData = LogUtils.readByteArray(entryBuffer, false);
            }
            if (haveAbortVLSN) {
                abortVLSN = LogUtils.readPackedLong(entryBuffer);
            }
        }

        if (logVersion >= 12) {
            if (haveAbortExpiration) {
                abortExpiration = LogUtils.readPackedInt(entryBuffer);
                if (abortExpiration < 0) {
                    abortExpiration = (- abortExpiration);
                    abortExpirationInHours = true;
                }
            }
            if (haveExpiration) {
                expiration = LogUtils.readPackedInt(entryBuffer);
                if (expiration < 0) {
                    expiration = (- expiration);
                    expirationInHours = true;
                }
            }
        }

        if (logVersion >= 6) {

            ln = newLNInstance(envImpl);
            ln.readFromLog(entryBuffer, logVersion);

            int keySize;
            if (keyIsLastSerializedField) {
                int bytesWritten = entryBuffer.position() - recStartPosition;
                keySize = header.getItemSize() - bytesWritten;
            } else {
                keySize = LogUtils.readPackedInt(entryBuffer);
            }
            key = LogUtils.readBytesNoLength(entryBuffer, keySize);
        }

        /* Save transient fields after read. */

        if (header.getVLSN() != null) {
            ln.setVLSNSequence(header.getVLSN().getSequence());
        }

        /* Dup conversion will be done by postFetchInit. */
        dupStatus =
            (logVersion < 8) ? DupStatus.NEED_CONVERSION : DupStatus.UNKNOWN;
    }

    private void setFlags(final byte flags) {
        embeddedLN = ((flags & EMBEDDED_LN_MASK) != 0);
        abortKnownDeleted = ((flags & ABORT_KD_MASK) != 0);
        haveAbortLSN = ((flags & HAVE_ABORT_LSN_MASK) != 0);
        haveAbortKey = ((flags & HAVE_ABORT_KEY_MASK) != 0);
        haveAbortData = ((flags & HAVE_ABORT_DATA_MASK) != 0);
        haveAbortVLSN = ((flags & HAVE_ABORT_VLSN_MASK) != 0);
        haveAbortExpiration = ((flags & HAVE_ABORT_EXPIRATION_MASK) != 0);
        haveExpiration = ((flags & HAVE_EXPIRATION_MASK) != 0);
    }

    @Override
    public boolean hasReplicationFormat() {
        return true;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {

        /* The replication format is optimized only in versions >= 11. */
        if (destVersion < 11) {
            return false;
        }

        /*
         * It is too much trouble to parse versions older than 12, because the
         * flags are not at the front in older versions.
         */
        if (srcVersion < 12) {
            return false;
        }

        final byte flags = logBuffer.get(0);

        /*
         * If we have an abort key or data, assume that the savings is
         * substantial enough to be worthwhile.
         *
         * The abort key is unusual and implies that data is hidden in the key
         * using a partial comparator, so we assume it is probably large,
         * relative to the total size.
         *
         * If there is abort data, it may be small, however, because the
         * presence of abort data implies that this is an update or deletion,
         * there will also be an abort LSN and an abort VLSN (with HA). Plus,
         * abort data is likely to be around the same size as the non-abort
         * data, and keys are normally smallish, meaning that the abort data is
         * largish relative to the total record size. So we assume the savings
         * are substantial enough.
         */
        return (flags &
            (HAVE_ABORT_KEY_MASK | HAVE_ABORT_DATA_MASK)) != 0;
    }

    /**
     * newLNInstance usually returns exactly the type of LN of the type that
     * was contained in in the log. For example, if a LNLogEntry holds a MapLN,
     * newLNInstance will return that MapLN. There is one extra possibility for
     * vanilla (data record) LNs. In that case, this method may either return a
     * LN or a generated type, the VersionedLN, which adds the vlsn information
     * from the log header to the LN object.
     */
    LN newLNInstance(EnvironmentImpl envImpl) {
        if (versionedLNConstructor != null && envImpl.getPreserveVLSN()) {
            return newInstanceOfType(versionedLNConstructor);
        }
        return newInstanceOfType();
    }

    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {

        dbId.dumpLog(sb, verbose);

        ln.dumpKey(sb, key);
        ln.dumpLog(sb, verbose);

        sb.append("<embeddedLN val=\"");
        sb.append(embeddedLN);
        sb.append("\"/>");

        if (haveExpiration) {
            sb.append("<expires val=\"");
            sb.append(TTL.formatExpiration(expiration, expirationInHours));
            sb.append("\"/>");
        }

        if (entryType.isTransactional()) {

            txn.dumpLog(sb, verbose);

            sb.append("<abortLSN val=\"");
            sb.append(DbLsn.getNoFormatString(abortLsn));
            sb.append("\"/>");

            sb.append("<abortKD val=\"");
            sb.append(abortKnownDeleted ? "true" : "false");
            sb.append("\"/>");

            if (haveAbortKey) {
                sb.append(Key.dumpString(abortKey, "abortKey", 0));
            }
            if (haveAbortData) {
                sb.append(Key.dumpString(abortData, "abortData", 0));
            }
            if (haveAbortVLSN) {
                sb.append("<abortVLSN v=\"");
                sb.append(abortVLSN);
                sb.append("\"/>");
            }
            if (haveAbortExpiration) {
                sb.append("<abortExpires val=\"");
                sb.append(TTL.formatExpiration(
                    abortExpiration, abortExpirationInHours));
                sb.append("\"/>");
            }
        }

        return sb;
    }

    @Override
    public void dumpRep(StringBuilder sb) {
        if (entryType.isTransactional()) {
            sb.append(" txn=").append(txn.getId());
        }
    }

    @Override
    public LN getMainItem() {
        return ln;
    }

    @Override
    public long getTransactionId() {
        if (entryType.isTransactional()) {
            return txn.getId();
        }
        return 0;
    }

    /*
     * Writing support.
     */

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Arrays.asList(new LN(), new DatabaseId(), new Txn());
    }

    @Override
    public int getSize(final int logVersion, final boolean forReplication) {

        assert getClass() == LNLogEntry.class;

        return getBaseLNEntrySize(
            logVersion, true /*keyIsLastSerializedField*/, forReplication);
    }

    /**
     * Method shared by LNLogEntry subclasses.
     *
     * @param keyIsLastSerializedField specifies whether the key length can be
     * omitted because the key is the last field.  This should be false when
     * an LNLogEntry subclass adds fields to the serialized format.
     */
    final int getBaseLNEntrySize(
        final int logVersion,
        final boolean keyIsLastSerializedField,
        final boolean forReplication) {

        int size = ln.getLogSize(logVersion, forReplication) +
            dbId.getLogSize(logVersion, forReplication) +
            key.length;

        if (!keyIsLastSerializedField) {
            size += LogUtils.getPackedIntLogSize(key.length);
        }

        if (entryType.isTransactional() || logVersion >= 11) {
            size += 1;   // flags
        }

        if (entryType.isTransactional()) {
            if (logVersion < 12 || (haveAbortLSN && !forReplication)) {
                size += LogUtils.getPackedLongLogSize(abortLsn);
            }
            size += txn.getLogSize(logVersion, forReplication);
        }

        if (!forReplication) {
            if (logVersion >= 11 ) {
                if (haveAbortKey) {
                    size += LogUtils.getByteArrayLogSize(abortKey);
                }
                if (haveAbortData) {
                    size += LogUtils.getByteArrayLogSize(abortData);
                }
                if (haveAbortVLSN) {
                    size += LogUtils.getPackedLongLogSize(abortVLSN);
                }
            }
            if (haveAbortExpiration) {
                size += LogUtils.getPackedIntLogSize(
                    abortExpirationInHours ?
                        (- abortExpiration) : abortExpiration);
            }
        }

        if (logVersion >= 12) {
            if (haveExpiration) {
                size += LogUtils.getPackedIntLogSize(
                    expirationInHours ? (- expiration) : expiration);
            }
        }

        return size;
    }

    @Override
    public void writeEntry(final ByteBuffer destBuffer,
                           final int logVersion,
                           final boolean forReplication) {

        /* Subclasses must call writeBaseLNEntry. */
        assert getClass() == LNLogEntry.class;

        writeBaseLNEntry(
            destBuffer, logVersion, true /*keyIsLastSerializedField*/,
            forReplication);
    }

    /**
     * Method shared by LNLogEntry subclasses.
     *
     * @param keyIsLastSerializedField specifies whether the key length can be
     * omitted because the key is the last field.  This should be false when
     * an LNLogEntry subclass adds fields to the serialized format.
     */
    final void writeBaseLNEntry(
        final ByteBuffer destBuffer,
        final int logVersion,
        final boolean keyIsLastSerializedField,
        final boolean forReplication) {

        byte flags = 0;

        if (entryType.isTransactional() &&
            (logVersion < 12 || !forReplication)) {

            if (abortKnownDeleted) {
                flags |= ABORT_KD_MASK;
            }
            if (haveAbortLSN) {
                flags |= HAVE_ABORT_LSN_MASK;
            }
        }

        if (!forReplication) {
            if (logVersion >= 11) {
                if (embeddedLN) {
                    flags |= EMBEDDED_LN_MASK;
                }
                if (haveAbortKey) {
                    flags |= HAVE_ABORT_KEY_MASK;
                }
                if (haveAbortData) {
                    flags |= HAVE_ABORT_DATA_MASK;
                }
                if (haveAbortVLSN) {
                    flags |= HAVE_ABORT_VLSN_MASK;
                }
            }
            if (logVersion >= 12) {
                if (haveAbortExpiration) {
                    flags |= HAVE_ABORT_EXPIRATION_MASK;
                }
            }
        }

        if (logVersion >= 12) {
            if (haveExpiration) {
                flags |= HAVE_EXPIRATION_MASK;
            }
            destBuffer.put(flags);
        }

        dbId.writeToLog(destBuffer, logVersion, forReplication);

        if (entryType.isTransactional()) {

            if (logVersion < 12 || (haveAbortLSN && !forReplication)) {
                LogUtils.writePackedLong(destBuffer, abortLsn);
            }

            if (logVersion < 12) {
                destBuffer.put(flags);
            }

            txn.writeToLog(destBuffer, logVersion, forReplication);

        } else if (logVersion == 11) {
            destBuffer.put(flags);
        }

        if (!forReplication) {
            if (logVersion >= 11) {
                if (haveAbortKey) {
                    LogUtils.writeByteArray(destBuffer, abortKey);
                }
                if (haveAbortData) {
                    LogUtils.writeByteArray(destBuffer, abortData);
                }
                if (haveAbortVLSN) {
                    LogUtils.writePackedLong(destBuffer, abortVLSN);
                }
            }
            if (logVersion >= 12) {
                if (haveAbortExpiration) {
                    LogUtils.writePackedInt(
                        destBuffer,
                        abortExpirationInHours ?
                            (-abortExpiration) : abortExpiration);
                }
            }
        }

        if (logVersion >= 12) {
            if (haveExpiration) {
                LogUtils.writePackedInt(
                    destBuffer,
                    expirationInHours ? (-expiration) : expiration);
            }
        }

        ln.writeToLog(destBuffer, logVersion, forReplication);

        if (!keyIsLastSerializedField) {
            LogUtils.writePackedInt(destBuffer, key.length);
        }
        LogUtils.writeBytesNoLength(destBuffer, key);
    }

    @Override
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl) {
        return (ln.isDeleted() ||
                embeddedLN ||
                dbImpl.isLNImmediatelyObsolete());
    }

    @Override
    public boolean isDeleted() {
        return ln.isDeleted();
    }

    /**
     * For LN entries, we need to record the latest LSN for that node with the
     * owning transaction, within the protection of the log latch. This is a
     * callback for the log manager to do that recording.
     */
    @Override
    public void postLogWork(
        LogEntryHeader header,
        long justLoggedLsn,
        VLSN vlsn) {

        if (entryType.isTransactional()) {
            txn.addLogInfo(justLoggedLsn);
        }

        /* Save transient fields after write. */
        if (vlsn != null) {
            ln.setVLSNSequence(vlsn.getSequence());
        }
    }

    @Override
    public void postFetchInit(DatabaseImpl dbImpl) {
        postFetchInit(dbImpl.getSortedDuplicates());
    }

    /**
     * Converts the key/data for old format LNs in a duplicates DB.
     *
     * This method MUST be called before calling any of the following methods:
     *  getLN
     *  getKey
     *  getUserKeyData
     *
     * TODO:
     * This method is not called by the HA feeder when materializing entries.
     * This is OK because entries with log version 7 and below are never
     * materialized. But we may want to rename this method to make it clear
     * that it only is, and only must be, called for the log versions < 8.
     */
    public void postFetchInit(boolean isDupDb) {

        final boolean needConversion =
            (dupStatus == DupStatus.NEED_CONVERSION);

        dupStatus = isDupDb ? DupStatus.DUP_DB : DupStatus.NOT_DUP_DB;

        /* Do not convert more than once. */
        if (!needConversion) {
            return;
        }

        /* Nothing to convert for non-duplicates DB. */
        if (dupStatus == DupStatus.NOT_DUP_DB) {
            return;
        }

        key = combineDupKeyData();
    }

    /**
     * Combine old key and old LN's data into a new key, and set the LN's data
     * to empty.
     */
    byte[] combineDupKeyData() {
        assert !ln.isDeleted(); // DeletedLNLogEntry overrides this method.
        return DupKeyData.combine(key, ln.setEmpty());
    }

    /**
     * Translates two-part keys in duplicate DBs back to the original user
     * operation params.  postFetchInit must be called before calling this
     * method.
     */
    public void getUserKeyData(
        DatabaseEntry keyParam,
        DatabaseEntry dataParam) {

        requireKnownDupStatus();

        if (dupStatus == DupStatus.DUP_DB) {
            DupKeyData.split(new DatabaseEntry(key), keyParam, dataParam);
        } else {
            if (keyParam != null) {
                keyParam.setData(key);
            }
            if (dataParam != null) {
                dataParam.setData(ln.getData());
            }
        }
    }

    /*
     * Accessors.
     */
    public boolean isEmbeddedLN() {
        return embeddedLN;
    }

    public LN getLN() {
        requireKnownDupStatus();
        return ln;
    }

    public byte[] getKey() {
        requireKnownDupStatus();
        return key;
    }

    public byte[] getData() {
        return ln.getData();
    }

    public byte[] getEmbeddedData() {

        if (!isEmbeddedLN()) {
            return null;
        }

        if (ln.isDeleted()) {
            return Key.EMPTY_KEY;
        }

        return ln.getData();
    }

    public int getExpiration() {
        return expiration;
    }

    public boolean isExpirationInHours() {
        return expirationInHours;
    }

    private void requireKnownDupStatus() {
        if (dupStatus != DupStatus.DUP_DB &&
            dupStatus != DupStatus.NOT_DUP_DB) {
            throw unexpectedState(
                "postFetchInit was not called");
        }
    }

    /**
     * This method is only used when the converted length is not needed, for
     * example by StatsFileReader.
     */
    public int getUnconvertedDataLength() {
        return ln.getData().length;
    }

    /**
     * This method is only used when the converted length is not needed, for
     * example by StatsFileReader.
     */
    public int getUnconvertedKeyLength() {
        return key.length;
    }

    @Override
    public DatabaseId getDbId() {
        return dbId;
    }

    public long getAbortLsn() {
        return abortLsn;
    }

    public boolean getAbortKnownDeleted() {
        return abortKnownDeleted;
    }

    public byte[] getAbortKey() {
        return abortKey;
    }

    public byte[] getAbortData() {
        return abortData;
    }

    public long getAbortVLSN() {
        return abortVLSN;
    }

    public int getAbortExpiration() {
        return abortExpiration;
    }

    public boolean isAbortExpirationInHours() {
        return abortExpirationInHours;
    }

    public Long getTxnId() {
        if (entryType.isTransactional()) {
            return txn.getId();
        }
        return null;
    }

    public Txn getUserTxn() {
        if (entryType.isTransactional()) {
            return txn;
        }
        return null;
    }

    @Override
    public boolean logicalEquals(LogEntry other) {
        if (!(other instanceof LNLogEntry)) {
            return false;
        }

        LNLogEntry<?> otherEntry = (LNLogEntry<?>) other;

        if (!dbId.logicalEquals(otherEntry.dbId)) {
            return false;
        }

        if (txn != null) {
            if (!txn.logicalEquals(otherEntry.txn)) {
                return false;
            }
        } else {
            if (otherEntry.txn != null) {
                return false;
            }
        }

        if (!Arrays.equals(key, otherEntry.key)) {
            return false;
        }

        if (!ln.logicalEquals(otherEntry.ln)) {
            return false;
        }

        return true;
    }
}
