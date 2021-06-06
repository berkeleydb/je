/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ProgramElementDoc;

class HidingProgramElementDocWrapper extends HidingDocWrapper
                                     implements ProgramElementDoc {
    public HidingProgramElementDocWrapper(ProgramElementDoc progelemdoc,
                                          Map mapWrappers) {
        super(progelemdoc, mapWrappers);
    }

    private ProgramElementDoc _getProgramElementDoc() {
        return (ProgramElementDoc)getWrappedObject();
    }

    @Override
    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getProgramElementDoc().containingClass());
    }

    @Override
    public PackageDoc containingPackage() {
        return (PackageDoc)
                wrapOrHide(_getProgramElementDoc().containingPackage());
    }

    @Override
    public String qualifiedName() {
        return _getProgramElementDoc().qualifiedName();
    }

    @Override
    public int modifierSpecifier() {
        return _getProgramElementDoc().modifierSpecifier();
    }

    @Override
    public String modifiers() {
        return _getProgramElementDoc().modifiers();
    }

    @Override
    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])
                wrapOrHide(_getProgramElementDoc().annotations()); 
    }

    @Override
    public boolean isPublic() {
        return _getProgramElementDoc().isPublic();
    }

    @Override
    public boolean isProtected() {
        return _getProgramElementDoc().isProtected();
    }

    @Override
    public boolean isPrivate() {
        return _getProgramElementDoc().isPrivate();
    }

    @Override
    public boolean isPackagePrivate() {
        return _getProgramElementDoc().isPackagePrivate();
    }

    @Override
    public boolean isStatic() {
        return _getProgramElementDoc().isStatic();
    }

    @Override
    public boolean isFinal() {
        return _getProgramElementDoc().isFinal();
    }
}
