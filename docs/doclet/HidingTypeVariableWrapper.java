/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;

class HidingTypeVariableWrapper extends HidingTypeWrapper
        implements TypeVariable {
    public HidingTypeVariableWrapper(TypeVariable type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private TypeVariable _getTypeVariable() {
        return (TypeVariable)getWrappedObject();
    }

    @Override
    public Type[] bounds() {
        return (Type[]) wrapOrHide(_getTypeVariable().bounds());
    }

    @Override
    public ProgramElementDoc owner() {
        return (ProgramElementDoc) wrapOrHide(_getTypeVariable().owner());
    }

    @Override
    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[]) wrapOrHide(_getTypeVariable().annotations());
    }
}
