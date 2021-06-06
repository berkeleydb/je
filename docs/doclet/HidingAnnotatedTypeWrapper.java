/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotatedType;
import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.Type;

class HidingAnnotatedTypeWrapper extends HidingTypeWrapper
        implements AnnotatedType {

    public HidingAnnotatedTypeWrapper(AnnotatedType type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private AnnotatedType _getAnnotatedType() {
        return (AnnotatedType) getWrappedObject();
    }

    @Override
    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[]) wrapOrHide(
            _getAnnotatedType().annotations());
    }

    @Override
    public Type underlyingType() {
        return (Type) wrapOrHide(_getAnnotatedType().underlyingType());
    }
}
