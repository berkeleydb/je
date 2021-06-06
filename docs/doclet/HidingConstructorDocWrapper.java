/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ConstructorDoc;

class HidingConstructorDocWrapper extends HidingExecutableMemberDocWrapper
                                  implements ConstructorDoc {
    public HidingConstructorDocWrapper(ConstructorDoc constrdoc,
                                       Map mapWrappers) {
        super(constrdoc, mapWrappers);
    }

    private ConstructorDoc _getConstructorDoc() {
        return (ConstructorDoc)getWrappedObject();
    }
}
