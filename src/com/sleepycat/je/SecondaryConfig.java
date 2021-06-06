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

package com.sleepycat.je;

import java.util.List;

import com.sleepycat.je.trigger.Trigger;
import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * The configuration properties of a <code>SecondaryDatabase</code> extend
 * those of a primary <code>Database</code>. The secondary database
 * configuration is specified when calling {@link
 * Environment#openSecondaryDatabase Environment.openSecondaryDatabase}.
 *
 * <p>To create a configuration object with default attributes:</p>
 *
 * <pre>
 *     SecondaryConfig config = new SecondaryConfig();
 * </pre>
 *
 * <p>To set custom attributes:</p>
 *
 * <pre>
 *     SecondaryConfig config = new SecondaryConfig();
 *     config.setAllowCreate(true);
 *     config.setSortedDuplicates(true);
 *     config.setKeyCreator(new MyKeyCreator());
 * </pre>
 *
 * @see Environment#openSecondaryDatabase
 * Environment.openSecondaryDatabase @see SecondaryDatabase
 */
public class SecondaryConfig extends DatabaseConfig {

    /*
     * For internal use, to allow null as a valid value for the config
     * parameter.
     */
    public static final SecondaryConfig DEFAULT = new SecondaryConfig();

    private boolean allowPopulate;
    private SecondaryKeyCreator keyCreator;
    private SecondaryMultiKeyCreator multiKeyCreator;
    private Database foreignKeyDatabase;
    private ForeignKeyDeleteAction foreignKeyDeleteAction =
            ForeignKeyDeleteAction.ABORT;
    private ForeignKeyNullifier foreignKeyNullifier;
    private ForeignMultiKeyNullifier foreignMultiKeyNullifier;
    private boolean extractFromPrimaryKeyOnly;
    private boolean immutableSecondaryKey;

    /**
     * Creates an instance with the system's default settings.
     */
    public SecondaryConfig() {
    }

    /**
     * Specifies the user-supplied object used for creating single-valued
     * secondary keys.
     *
     * <p>Unless the primary database is read-only, a key creator is required
     * when opening a secondary database.  Either a KeyCreator or
     * MultiKeyCreator must be specified, but both may not be specified.</p>
     *
     * <p>Unless the primary database is read-only, a key creator is required
     * when opening a secondary database.</p>
     *
     * <p><em>WARNING:</em> Key creator instances are shared by multiple
     * threads and key creator methods are called without any special
     * synchronization.  Therefore, key creators must be thread safe.  In
     * general no shared state should be used and any caching of computed
     * values must be done with proper synchronization.</p>
     *
     * @param keyCreator the user-supplied object used for creating
     * single-valued secondary keys.
     *
     * @return this
     */
    public SecondaryConfig setKeyCreator(SecondaryKeyCreator keyCreator) {
        setKeyCreatorVoid(keyCreator);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setKeyCreatorVoid(SecondaryKeyCreator keyCreator) {
        this.keyCreator = keyCreator;
    }

    /**
     * Returns the user-supplied object used for creating single-valued
     * secondary keys.
     *
     * @return the user-supplied object used for creating single-valued
     * secondary keys.
     *
     * @see #setKeyCreator
     */
    public SecondaryKeyCreator getKeyCreator() {
        return keyCreator;
    }

    /**
     * Specifies the user-supplied object used for creating multi-valued
     * secondary keys.
     *
     * <p>Unless the primary database is read-only, a key creator is required
     * when opening a secondary database.  Either a KeyCreator or
     * MultiKeyCreator must be specified, but both may not be specified.</p>
     *
     * <p><em>WARNING:</em> Key creator instances are shared by multiple
     * threads and key creator methods are called without any special
     * synchronization.  Therefore, key creators must be thread safe.  In
     * general no shared state should be used and any caching of computed
     * values must be done with proper synchronization.</p>
     *
     * @param multiKeyCreator the user-supplied object used for creating
     * multi-valued secondary keys.
     *
     * @return this
     */
    public SecondaryConfig
        setMultiKeyCreator(SecondaryMultiKeyCreator multiKeyCreator) {

        setMultiKeyCreatorVoid(multiKeyCreator);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void 
        setMultiKeyCreatorVoid(SecondaryMultiKeyCreator multiKeyCreator) {

        this.multiKeyCreator = multiKeyCreator;
    }

    /**
     * Returns the user-supplied object used for creating multi-valued
     * secondary keys.
     *
     * @return the user-supplied object used for creating multi-valued
     * secondary keys.
     *
     * @see #setKeyCreator
     */
    public SecondaryMultiKeyCreator getMultiKeyCreator() {
        return multiKeyCreator;
    }

    /**
     * Specifies whether automatic population of the secondary is allowed.
     *
     * <p>If automatic population is allowed, when the secondary database is
     * opened it is checked to see if it is empty.  If it is empty, the primary
     * database is read in its entirety and keys are added to the secondary
     * database using the information read from the primary.</p>
     *
     * <p>If this property is set to true and the database is transactional,
     * the population of the secondary will be done within the explicit or
     * auto-commit transaction that is used to open the database.</p>
     *
     * @param allowPopulate whether automatic population of the secondary is
     * allowed.
     *
     * @return this
     */
    public SecondaryConfig setAllowPopulate(boolean allowPopulate) {
        setAllowPopulateVoid(allowPopulate);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setAllowPopulateVoid(boolean allowPopulate) {
        this.allowPopulate = allowPopulate;
    }

    /**
     * Returns whether automatic population of the secondary is allowed.  If
     * {@link #setAllowPopulate} has not been called, this method returns
     * false.
     *
     * @return whether automatic population of the secondary is allowed.
     *
     * @see #setAllowPopulate
     */
    public boolean getAllowPopulate() {
        return allowPopulate;
    }

    /**
     * Defines a foreign key integrity constraint for a given foreign key
     * database.
     *
     * <p>If this property is non-null, a record must be present in the
     * specified foreign database for every record in the secondary database,
     * where the secondary key value is equal to the foreign database key
     * value. Whenever a record is to be added to the secondary database, the
     * secondary key is used as a lookup key in the foreign database.  If the
     * key is not found in the foreign database, a {@link
     * ForeignConstraintException} is thrown.</p>
     *
     * <p>The foreign database must not have duplicates allowed.  If duplicates
     * are allowed, an IllegalArgumentException will be thrown when the
     * secondary database is opened.</p>
     *
     * @param foreignKeyDatabase the database used to check the foreign key
     * integrity constraint, or null if no foreign key constraint should be
     * checked.
     *
     * @return this
     */
    public SecondaryConfig setForeignKeyDatabase(Database foreignKeyDatabase) {
        setForeignKeyDatabaseVoid(foreignKeyDatabase);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setForeignKeyDatabaseVoid(Database foreignKeyDatabase) {
        this.foreignKeyDatabase = foreignKeyDatabase;
    }

    /**
     * Returns the database used to check the foreign key integrity constraint,
     * or null if no foreign key constraint will be checked.
     *
     * @return the foreign key database, or null.
     *
     * @see #setForeignKeyDatabase
     */
    public Database getForeignKeyDatabase() {
        return foreignKeyDatabase;
    }

    /**
     * Specifies the action taken when a referenced record in the foreign key
     * database is deleted.
     *
     * <p>This property is ignored if the foreign key database property is
     * null.</p>
     *
     * @param foreignKeyDeleteAction the action taken when a referenced record
     * in the foreign key database is deleted.
     *
     * @see ForeignKeyDeleteAction @see #setForeignKeyDatabase
     *
     * @return this
     */
    public SecondaryConfig setForeignKeyDeleteAction
        (ForeignKeyDeleteAction foreignKeyDeleteAction) {

        setForeignKeyDeleteActionVoid(foreignKeyDeleteAction);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setForeignKeyDeleteActionVoid
        (ForeignKeyDeleteAction foreignKeyDeleteAction) {

        DatabaseUtil.checkForNullParam(foreignKeyDeleteAction,
                                       "foreignKeyDeleteAction");
        this.foreignKeyDeleteAction = foreignKeyDeleteAction;
    }

    /**
     * Returns the action taken when a referenced record in the foreign key
     * database is deleted.
     *
     * @return the action taken when a referenced record in the foreign key
     * database is deleted.
     *
     * @see #setForeignKeyDeleteAction
     */
    public ForeignKeyDeleteAction getForeignKeyDeleteAction() {
        return foreignKeyDeleteAction;
    }

    /**
     * Specifies the user-supplied object used for setting single-valued
     * foreign keys to null.
     *
     * <p>This method may <em>not</em> be used along with {@link
     * #setMultiKeyCreator}.  When using a multi-key creator, use {@link
     * #setForeignMultiKeyNullifier} instead.</p>
     *
     * <p>If the foreign key database property is non-null and the foreign key
     * delete action is <code>NULLIFY</code>, this property is required to be
     * non-null; otherwise, this property is ignored.</p>
     *
     * <p><em>WARNING:</em> Key nullifier instances are shared by multiple
     * threads and key nullifier methods are called without any special
     * synchronization.  Therefore, key creators must be thread safe.  In
     * general no shared state should be used and any caching of computed
     * values must be done with proper synchronization.</p>
     *
     * @param foreignKeyNullifier the user-supplied object used for setting
     * single-valued foreign keys to null.
     *
     * @see ForeignKeyNullifier @see ForeignKeyDeleteAction#NULLIFY @see
     * #setForeignKeyDatabase
     *
     * @return this
     */
    public SecondaryConfig
        setForeignKeyNullifier(ForeignKeyNullifier foreignKeyNullifier) {

        setForeignKeyNullifierVoid(foreignKeyNullifier);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void 
        setForeignKeyNullifierVoid(ForeignKeyNullifier foreignKeyNullifier) {

        this.foreignKeyNullifier = foreignKeyNullifier;
    }

    /**
     * Returns the user-supplied object used for setting single-valued foreign
     * keys to null.
     *
     * @return the user-supplied object used for setting single-valued foreign
     * keys to null.
     *
     * @see #setForeignKeyNullifier
     */
    public ForeignKeyNullifier getForeignKeyNullifier() {
        return foreignKeyNullifier;
    }

    /**
     * Specifies the user-supplied object used for setting multi-valued foreign
     * keys to null.
     *
     * <p>If the foreign key database property is non-null and the foreign key
     * delete action is <code>NULLIFY</code>, this property is required to be
     * non-null; otherwise, this property is ignored.</p>
     *
     * <p><em>WARNING:</em> Key nullifier instances are shared by multiple
     * threads and key nullifier methods are called without any special
     * synchronization.  Therefore, key creators must be thread safe.  In
     * general no shared state should be used and any caching of computed
     * values must be done with proper synchronization.</p>
     *
     * @param foreignMultiKeyNullifier the user-supplied object used for
     * setting multi-valued foreign keys to null.
     *
     * @see ForeignMultiKeyNullifier @see ForeignKeyDeleteAction#NULLIFY @see
     * #setForeignKeyDatabase
     *
     * @return this
     */
    public SecondaryConfig setForeignMultiKeyNullifier
        (ForeignMultiKeyNullifier foreignMultiKeyNullifier) {

        setForeignMultiKeyNullifierVoid(foreignMultiKeyNullifier);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setForeignMultiKeyNullifierVoid
        (ForeignMultiKeyNullifier foreignMultiKeyNullifier) {

        this.foreignMultiKeyNullifier = foreignMultiKeyNullifier;
    }

    /**
     * Returns the user-supplied object used for setting multi-valued foreign
     * keys to null.
     *
     * @return the user-supplied object used for setting multi-valued foreign
     * keys to null.
     *
     * @see #setForeignMultiKeyNullifier
     */
    public ForeignMultiKeyNullifier getForeignMultiKeyNullifier() {
        return foreignMultiKeyNullifier;
    }

    /**
     * @hidden
     * For internal use only.
     *
     * Specifies whether the key extractor/creator will only use the primary
     * key.
     *
     * <p>Specifying that only the primary key is needed can be used to
     * optimize primary database updates and deletions.  If a primary record is
     * updated or deleted, and all associated secondaries have this property
     * set to true, then the existing primary record will not be read.  This
     * potentially saves an I/O.  When this property is <em>not</em> set to
     * true (it is false by default), the existing primary record must be read
     * if it is not already in cache, order to pass the primary data to the key
     * extractor/creator.</p>
     *
     * <p>Note that if this property is true, either null or a non-null value
     * may be passed to the key extractor/creator for the primary data
     * parameter. The key extractor/creator is expected to ignore this
     * parameter.<p>
     *
     * @param extractFromPrimaryKeyOnly whether the key extractor/creator will
     * only use the primary key.
     *
     * @return this
     */
    public SecondaryConfig
        setExtractFromPrimaryKeyOnly(boolean extractFromPrimaryKeyOnly) {

        setExtractFromPrimaryKeyOnlyVoid(extractFromPrimaryKeyOnly);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void
        setExtractFromPrimaryKeyOnlyVoid(boolean extractFromPrimaryKeyOnly) {

        this.extractFromPrimaryKeyOnly = extractFromPrimaryKeyOnly;
    }

    /**
     * @hidden
     * For internal use only.
     *
     * Returns whether the key extractor/creator will only use the primary key.
     * If {@link #setExtractFromPrimaryKeyOnly} has not been called, this
     * method returns false.
     *
     * @return whether the key extractor/creator will only use the primary key.
     *
     * @see #setExtractFromPrimaryKeyOnly
     */
    public boolean getExtractFromPrimaryKeyOnly() {
        return extractFromPrimaryKeyOnly;
    }

    /**
     * Specifies whether the secondary key is immutable.
     *
     * <p>Specifying that a secondary key is immutable can be used to optimize
     * updates when the secondary key in a primary record will never be changed
     * after that primary record is inserted.  For immutable secondary keys, a
     * best effort is made to avoid calling
     * <code>SecondaryKeyCreator.createSecondaryKey</code> when a primary
     * record is updated.  This optimization may reduce the overhead of an
     * update operation significantly if the <code>createSecondaryKey</code>
     * operation is expensive.</p>
     *
     * <p>Be sure to set this property to true only if the secondary key in the
     * primary record is never changed.  If this rule is violated, the
     * secondary index will become corrupted, that is, it will become out of
     * sync with the primary.</p>
     *
     * @param immutableSecondaryKey whether the secondary key is immutable.
     *
     * @return this
     */
    public SecondaryConfig
        setImmutableSecondaryKey(boolean immutableSecondaryKey) {

        setImmutableSecondaryKeyVoid(immutableSecondaryKey);
        return this;
    }
    
    /**
     * @hidden
     * The void return setter for use by Bean editors.
     */
    public void setImmutableSecondaryKeyVoid(boolean immutableSecondaryKey) {

        this.immutableSecondaryKey = immutableSecondaryKey;
    }

    /**
     * Returns whether the secondary key is immutable.  If {@link
     * #setImmutableSecondaryKey} has not been called, this method returns
     * false.
     *
     * @return whether the secondary key is immutable.
     *
     * @see #setImmutableSecondaryKey
     */
    public boolean getImmutableSecondaryKey() {
        return immutableSecondaryKey;
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public SecondaryConfig clone() {
        return (SecondaryConfig) super.clone();
    }

    /**
     * For JCA Database handle caching.
     *
     * @throws IllegalArgumentException via JEConnection.openSecondaryDatabase.
     */
    @Override
    void validate(DatabaseConfig configArg)
        throws DatabaseException {

        super.validate(configArg);

        if (configArg == null ||
            !(configArg instanceof SecondaryConfig)) {
            throw new IllegalArgumentException
                ("The SecondaryConfig argument is null.");
        }

        SecondaryConfig config = (SecondaryConfig) configArg;

        boolean kcMatch = equalOrBothNull
            (config.getKeyCreator(), keyCreator);
        boolean mkcMatch = equalOrBothNull
            (config.getMultiKeyCreator(), multiKeyCreator);
        boolean fkdMatch =
            (config.getForeignKeyDatabase() == foreignKeyDatabase);
        boolean fkdaMatch =
            (config.getForeignKeyDeleteAction() == foreignKeyDeleteAction);
        boolean fknMatch = equalOrBothNull
            (config.getForeignKeyNullifier(), foreignKeyNullifier);
        boolean fmknMatch = equalOrBothNull
            (config.getForeignMultiKeyNullifier(), foreignMultiKeyNullifier);
        boolean imskMatch =
            (config.getImmutableSecondaryKey() == immutableSecondaryKey);
        if (kcMatch &&
            mkcMatch &&
            fkdMatch &&
            fkdaMatch &&
            fknMatch &&
            fmknMatch &&
            imskMatch) {
            return;
        }

        String message =
            genSecondaryConfigMismatchMessage(
                config, kcMatch, mkcMatch, fkdMatch, fkdaMatch,
                fknMatch, fmknMatch, imskMatch);
        throw new IllegalArgumentException(message);
    }
    
    /**
     * @hidden
     * For internal use only.
     */
    @Override
    public DatabaseConfig setTriggers(List<Trigger> triggers) {

        if ((triggers == null) || (triggers.size() == 0)) {
            return this;
        }

        throw new IllegalArgumentException
            ("Triggers may only be associated with a Primary database");
    }

    /**
     * @hidden
     * For internal use only.
     */
    @Override
    public DatabaseConfig setOverrideTriggers(@SuppressWarnings("unused")
                                              boolean override) {
        throw new IllegalArgumentException
            ("Triggers may only be associated with a Primary database");
    }
    
    private boolean equalOrBothNull(Object o1, Object o2) {
        return (o1 != null) ? o1.equals(o2) : (o2 == null);
    }

    String genSecondaryConfigMismatchMessage(DatabaseConfig config,
                                             boolean kcMatch,
                                             boolean mkcMatch,
                                             boolean fkdMatch,
                                             boolean fkdaMatch,
                                             boolean fknMatch,
                                             boolean fmknMatch,
                                             boolean imskMatch) {
        StringBuilder ret = new StringBuilder
            ("The following SecondaryConfig parameters for the\n" +
             "cached Database do not match the parameters for the\n" +
             "requested Database:\n");
        if (!kcMatch) {
            ret.append(" SecondaryKeyCreator\n");
        }

        if (!mkcMatch) {
            ret.append(" SecondaryMultiKeyCreator\n");
        }

        if (!fkdMatch) {
            ret.append(" ForeignKeyDelete\n");
        }

        if (!fkdaMatch) {
            ret.append(" ForeignKeyDeleteAction\n");
        }

        if (!fknMatch) {
            ret.append(" ForeignKeyNullifier\n");
        }

        if (!fknMatch) {
            ret.append(" ForeignMultiKeyNullifier\n");
        }

        if (!imskMatch) {
            ret.append(" ImmutableSecondaryKey\n");
        }

        return ret.toString();
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "keyCreator=" + keyCreator +
            "\nmultiKeyCreator=" + multiKeyCreator +
            "\nallowPopulate=" + allowPopulate +
            "\nforeignKeyDatabase=" + foreignKeyDatabase +
            "\nforeignKeyDeleteAction=" + foreignKeyDeleteAction +
            "\nforeignKeyNullifier=" + foreignKeyNullifier +
            "\nforeignMultiKeyNullifier=" + foreignMultiKeyNullifier +
            "\nimmutableSecondaryKey=" + immutableSecondaryKey +
            "\n";
    }
}
