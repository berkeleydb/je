/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.MemberDoc;

class HidingMemberDocWrapper extends HidingProgramElementDocWrapper
        implements MemberDoc {

    public HidingMemberDocWrapper(MemberDoc memdoc, Map mapWrappers) {
        super(memdoc, mapWrappers);
    }

    private MemberDoc _getMemberDoc() {
        return (MemberDoc)getWrappedObject();
    }

    @Override
    public boolean isSynthetic() {
        return _getMemberDoc().isSynthetic();
    }
}
