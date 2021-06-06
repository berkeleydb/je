/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.AnnotationTypeElementDoc;

class HidingAnnotationTypeDocWrapper extends HidingClassDocWrapper
                                     implements AnnotationTypeDoc {
    public HidingAnnotationTypeDocWrapper(AnnotationTypeDoc type,
                                          Map mapWrappers) {
        super(type, mapWrappers);
    }

    private AnnotationTypeDoc _getAnnotationTypeDoc() {
        return (AnnotationTypeDoc)getWrappedObject();
    }

    @Override
    public AnnotationTypeElementDoc[] elements() {
        return (AnnotationTypeElementDoc[])
                wrapOrHide(_getAnnotationTypeDoc().elements());
    }
}
