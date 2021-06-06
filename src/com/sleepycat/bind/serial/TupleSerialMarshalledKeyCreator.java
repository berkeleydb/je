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

package com.sleepycat.bind.serial;

import com.sleepycat.bind.tuple.MarshalledTupleKeyEntity;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * A concrete key creator that works in conjunction with a {@link
 * TupleSerialMarshalledBinding}.  This key creator works by calling the
 * methods of the {@link MarshalledTupleKeyEntity} interface to create and
 * clear the index key fields.
 *
 * @see <a href="SerialBinding.html#evolution">Class Evolution</a>
 *
 * @author Mark Hayes
 */
public class TupleSerialMarshalledKeyCreator<D extends
                                             MarshalledTupleKeyEntity>
    extends TupleSerialKeyCreator<D> {

    private TupleSerialMarshalledBinding<D> binding;
    private String keyName;

    /**
     * Creates a tuple-serial marshalled key creator.
     *
     * @param binding is the binding used for the tuple-serial entity.
     *
     * @param keyName is the key name passed to the {@link
     * MarshalledTupleKeyEntity#marshalSecondaryKey} method to identify the
     * index key.
     */
    public TupleSerialMarshalledKeyCreator(TupleSerialMarshalledBinding<D>
                                           binding,
                                           String keyName) {

        super(binding.dataBinding);
        this.binding = binding;
        this.keyName = keyName;

        if (dataBinding == null) {
            throw new NullPointerException("dataBinding may not be null");
        }
    }

    // javadoc is inherited
    public boolean createSecondaryKey(TupleInput primaryKeyInput,
                                      D dataInput,
                                      TupleOutput indexKeyOutput) {

        /*
         * The primary key is unmarshalled before marshalling the index key, to
         * account for cases where the index key includes fields taken from the
         * primary key.
         */
        MarshalledTupleKeyEntity entity =
            binding.entryToObject(primaryKeyInput, dataInput);

        return entity.marshalSecondaryKey(keyName, indexKeyOutput);
    }

    // javadoc is inherited
    public D nullifyForeignKey(D dataInput) {

        MarshalledTupleKeyEntity entity =
            binding.entryToObject(null, dataInput);

        return entity.nullifyForeignKey(keyName) ? dataInput : null;
    }
}
