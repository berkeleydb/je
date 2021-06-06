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

package com.sleepycat.collections;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.TupleSerialMarshalledBinding;
import com.sleepycat.bind.serial.TupleSerialMarshalledKeyCreator;
import com.sleepycat.bind.tuple.MarshalledTupleEntry; // for javadoc
import com.sleepycat.bind.tuple.MarshalledTupleKeyEntity;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleMarshalledBinding;
import com.sleepycat.je.Database;

/**
 * Creates stored collections having tuple keys and serialized entity values.
 * The entity classes must be Serializable and must implement the
 * MarshalledTupleKeyEntity interfaces.  The key classes must either implement
 * the MarshalledTupleEntry interface or be one of the Java primitive type
 * classes.  Underlying binding objects are created automatically.
 *
 * @author Mark Hayes
 */
public class TupleSerialFactory {

    private ClassCatalog catalog;

    /**
     * Creates a tuple-serial factory for given environment and class catalog.
     *
     * @param catalog the ClassCatalog.
     */
    public TupleSerialFactory(ClassCatalog catalog) {

        this.catalog = catalog;
    }

    /**
     * Returns the class catalog associated with this factory.
     *
     * @return the catalog.
     */
    public final ClassCatalog getCatalog() {

        return catalog;
    }

    /**
     * Creates a map from a previously opened Database object.
     *
     * @param db the previously opened Database object.
     *
     * @param keyClass is the class used for map keys.  It must implement the
     * {@link MarshalledTupleEntry} interface or be one of the Java primitive
     * type classes.
     *
     * @param valueBaseClass the base class of the entity values for this
     * store.  It must implement the  {@link MarshalledTupleKeyEntity}
     * interface.
     *
     * @param writeAllowed is true to create a read-write collection or false
     * to create a read-only collection.
     *
     * @param <K> the key class.
     *
     * @param <V> the value base class.
     *
     * @return the map.
     */
    public <K, V extends MarshalledTupleKeyEntity> StoredMap<K, V>
        newMap(Database db,
               Class<K> keyClass,
               Class<V> valueBaseClass,
               boolean writeAllowed) {

        return new StoredMap<K, V>(db,
                        getKeyBinding(keyClass),
                        getEntityBinding(valueBaseClass),
                        writeAllowed);
    }

    /**
     * Creates a sorted map from a previously opened Database object.
     *
     * @param db the previously opened Database object.
     *
     * @param keyClass is the class used for map keys.  It must implement the
     * {@link MarshalledTupleEntry} interface or be one of the Java primitive
     * type classes.
     *
     * @param valueBaseClass the base class of the entity values for this
     * store.  It must implement the  {@link MarshalledTupleKeyEntity}
     * interface.
     *
     * @param writeAllowed is true to create a read-write collection or false
     * to create a read-only collection.
     *
     * @param <K> the key class.
     *
     * @param <V> the value base class.
     *
     * @return the sorted map.
     */
    public <K, V extends MarshalledTupleKeyEntity> StoredSortedMap<K, V>
        newSortedMap(Database db,
                     Class<K> keyClass,
                     Class<V> valueBaseClass,
                     boolean writeAllowed) {

        return new StoredSortedMap(db,
                        getKeyBinding(keyClass),
                        getEntityBinding(valueBaseClass),
                        writeAllowed);
    }

    /**
     * Creates a <code>SecondaryKeyCreator</code> object for use in configuring
     * a <code>SecondaryDatabase</code>.  The returned object implements
     * the {@link com.sleepycat.je.SecondaryKeyCreator} interface.
     *
     * @param valueBaseClass the base class of the entity values for this
     * store.  It must implement the  {@link MarshalledTupleKeyEntity}
     * interface.
     *
     * @param keyName is the key name passed to the {@link
     * MarshalledTupleKeyEntity#marshalSecondaryKey} method to identify the
     * secondary key.
     *
     * @param <V> the value base class.
     *
     * @return the key creator.
     */
    public <V extends MarshalledTupleKeyEntity>
        TupleSerialMarshalledKeyCreator<V>
        getKeyCreator(Class<V> valueBaseClass, String keyName) {

        return new TupleSerialMarshalledKeyCreator<V>
            (getEntityBinding(valueBaseClass), keyName);
    }

    public <V extends MarshalledTupleKeyEntity>
        TupleSerialMarshalledBinding<V>
        getEntityBinding(Class<V> baseClass) {

        return new TupleSerialMarshalledBinding<V>(catalog, baseClass);
    }

    private <K> EntryBinding<K> getKeyBinding(Class<K> keyClass) {

        EntryBinding<K> binding = TupleBinding.getPrimitiveBinding(keyClass);
        if (binding == null) {

            /*
             * Cannot use type param <K> here because it does not implement
             * MarshalledTupleEntry if it is a primitive class.
             */
            binding = new TupleMarshalledBinding(keyClass);
        }
        return binding;
    }
}
