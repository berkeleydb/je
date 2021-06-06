/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.SerialFieldTag;
import com.sun.javadoc.Type;

class HidingFieldDocWrapper extends HidingMemberDocWrapper
        implements FieldDoc {

    public HidingFieldDocWrapper(FieldDoc fielddoc, Map mapWrappers) {
        super(fielddoc, mapWrappers);
    }

    private FieldDoc _getFieldDoc() {
        return (FieldDoc)getWrappedObject();
    }

    @Override
    public Type type() {
        return (Type)wrapOrHide(_getFieldDoc().type());
    }

    @Override
    public boolean isTransient() {
        return _getFieldDoc().isTransient();
    }

    @Override
    public boolean isVolatile() {
        return _getFieldDoc().isVolatile();
    }

    @Override
    public SerialFieldTag[] serialFieldTags() {
        return (SerialFieldTag[])wrapOrHide(_getFieldDoc().serialFieldTags());
    }

    @Override
    public Object constantValue() {
        return _getFieldDoc().constantValue();
    }

    @Override
    public String constantValueExpression() {
        return _getFieldDoc().constantValueExpression();
    }
}
