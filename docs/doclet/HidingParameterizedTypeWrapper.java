/*-
 * See the file LICENSE for redistributiion information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.Type;

class HidingParameterizedTypeWrapper extends HidingTypeWrapper
                                     implements ParameterizedType {
    public HidingParameterizedTypeWrapper(ParameterizedType type, 
                                          Map mapWrappers) {
        super(type, mapWrappers);
    }

    private ParameterizedType _getParameterizedType() {
        return (ParameterizedType)getWrappedObject();
    }

    @Override
    public ClassDoc asClassDoc() {
        return (ClassDoc) wrapOrHide(_getParameterizedType().asClassDoc());
    }

    @Override
    public Type[] typeArguments() {
        return (Type[])wrapOrHide(_getParameterizedType().typeArguments());
    }

    @Override
    public Type superclassType() {
        return (Type)wrapOrHide(_getParameterizedType().superclassType());
    }

    @Override
    public Type[] interfaceTypes() {
        return (Type[])wrapOrHide(_getParameterizedType().interfaceTypes());
    }

    @Override
    public Type containingType() {
        return (Type)wrapOrHide(_getParameterizedType().containingType());
    }
}
