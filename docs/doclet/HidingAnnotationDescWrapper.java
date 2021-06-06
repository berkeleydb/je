/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;

class HidingAnnotationDescWrapper extends HidingWrapper
                                  implements AnnotationDesc {

    public HidingAnnotationDescWrapper(AnnotationDesc type,
                                       Map mapWrappers) {
        super(type, mapWrappers);
    }

    private AnnotationDesc _getAnnotationDesc() {
        return (AnnotationDesc)getWrappedObject();
    }

    @Override
    public AnnotationTypeDoc annotationType() {
        return (AnnotationTypeDoc)
                wrapOrHide(_getAnnotationDesc().annotationType());
    }

    @Override
    public AnnotationDesc.ElementValuePair[] elementValues() {
        return _getAnnotationDesc().elementValues();
    }

    @Override
    public boolean isSynthesized() {
        return _getAnnotationDesc().isSynthesized();
    }
}
