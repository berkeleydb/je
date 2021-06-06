/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.MemberDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.SeeTag;

class HidingSeeTagWrapper extends HidingTagWrapper implements SeeTag {
    public HidingSeeTagWrapper(SeeTag seetag, Map mapWrappers) {
        super(seetag, mapWrappers);
    }

    private SeeTag _getSeeTag() {
        return (SeeTag)getWrappedObject();
    }

    @Override
    public String label() {
        return _getSeeTag().label();
    }

    @Override
    public PackageDoc referencedPackage() {
        return (PackageDoc)wrapOrHide(_getSeeTag().referencedPackage());
    }

    @Override
    public String referencedClassName() {
        return _getSeeTag().referencedClassName();
    }

    @Override
    public ClassDoc referencedClass() {
        return (ClassDoc)wrapOrHide(_getSeeTag().referencedClass());
    }

    @Override
    public String referencedMemberName() {
        return _getSeeTag().referencedMemberName();
    }

    @Override
    public MemberDoc referencedMember() {
        return (MemberDoc)wrapOrHide(_getSeeTag().referencedMember());
    }
}
