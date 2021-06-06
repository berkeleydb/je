/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.PackageDoc;

class HidingPackageDocWrapper extends HidingDocWrapper implements PackageDoc {
    public HidingPackageDocWrapper(PackageDoc packdoc, Map mapWrappers) {
        super(packdoc, mapWrappers);
    }

    private PackageDoc _getPackageDoc() {
        return (PackageDoc)getWrappedObject();
    }

    @Override
    public ClassDoc[] allClasses(boolean filter) {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().allClasses(filter));
    }

    @Override
    public ClassDoc[] allClasses() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().allClasses());
    }

    @Override
    public ClassDoc[] ordinaryClasses() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().ordinaryClasses());
    }

    @Override
    public ClassDoc[] exceptions() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().exceptions());
    }

    @Override
    public ClassDoc[] errors() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().errors());
    }

    @Override
    public ClassDoc[] enums() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().enums());
    }

    @Override
    public ClassDoc[] interfaces() {
        return (ClassDoc[])wrapOrHide(_getPackageDoc().interfaces());
    }

    @Override
    public AnnotationTypeDoc[] annotationTypes() {
        return _getPackageDoc().annotationTypes();
    }

    @Override
    public AnnotationDesc[] annotations() {
        return _getPackageDoc().annotations();
    }

    @Override
    public ClassDoc findClass(String szClassName) {
        return (ClassDoc)wrapOrHide(_getPackageDoc().findClass(szClassName));
    }
}
