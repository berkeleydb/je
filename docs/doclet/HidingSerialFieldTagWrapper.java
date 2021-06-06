/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.SerialFieldTag;

class HidingSerialFieldTagWrapper extends HidingTagWrapper
                                  implements SerialFieldTag {
    public HidingSerialFieldTagWrapper(SerialFieldTag serfldtag,
                                       Map mapWrappers) {
        super(serfldtag, mapWrappers);
    }

    private SerialFieldTag _getSerialFieldTag() {
        return (SerialFieldTag)getWrappedObject();
    }

    /* SerialFieldTag */

    @Override
    public String fieldName() {
        return _getSerialFieldTag().fieldName();
    }

    @Override
    public String fieldType() {
        return _getSerialFieldTag().fieldType();
    }

    @Override
    public ClassDoc fieldTypeDoc() {
        return (ClassDoc)wrapOrHide(_getSerialFieldTag().fieldTypeDoc());
    }

    @Override
    public String description() {
        return _getSerialFieldTag().description();
    }

    /* Comparable */

    @Override
    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getSerialFieldTag().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getSerialFieldTag().compareTo(obj);
        }
    }


}
