/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;

class HidingThrowsTagWrapper extends HidingTagWrapper implements ThrowsTag {
    public HidingThrowsTagWrapper(ThrowsTag thrtag, Map mapWrappers) {
        super(thrtag, mapWrappers);
    }

    private ThrowsTag _getThrowsTag() {
        return (ThrowsTag)getWrappedObject();
    }

    @Override
    public String exceptionName() {
        return _getThrowsTag().exceptionName();
    }

    @Override
    public String exceptionComment() {
        return _getThrowsTag().exceptionComment();
    }

    @Override
    public ClassDoc exception() {
        return (ClassDoc)wrapOrHide(_getThrowsTag().exception());
    }

    @Override
    public Type exceptionType() {
        return (Type)wrapOrHide(_getThrowsTag().exceptionType());
    }
}
