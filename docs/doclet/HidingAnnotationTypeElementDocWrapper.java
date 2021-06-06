/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationTypeElementDoc;
import com.sun.javadoc.AnnotationValue;

class HidingAnnotationTypeElementDocWrapper extends HidingMethodDocWrapper
    implements AnnotationTypeElementDoc {

    public HidingAnnotationTypeElementDocWrapper(
        AnnotationTypeElementDoc memdoc, Map mapWrappers) {
        super(memdoc, mapWrappers);
    }

    private AnnotationTypeElementDoc _getAnnotationTypeElementDoc() {
        return (AnnotationTypeElementDoc)getWrappedObject();
    }

    @Override
    public AnnotationValue defaultValue() {
        return (AnnotationValue)
                wrapOrHide(_getAnnotationTypeElementDoc().defaultValue());
    }
}
