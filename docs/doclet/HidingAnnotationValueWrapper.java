/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationValue;

class HidingAnnotationValueWrapper extends HidingWrapper
                                   implements AnnotationValue {
    public HidingAnnotationValueWrapper(AnnotationValue value,
                                            Map mapWrappers) {
        super(value, mapWrappers);
    }

    private AnnotationValue _getAnnotationValue() {
        return (AnnotationValue)getWrappedObject();
    }

    @Override
    public Object value() {
        return _getAnnotationValue().value();
    }

    @Override
    public String toString() {
        return _getAnnotationValue().toString();
    }
}
