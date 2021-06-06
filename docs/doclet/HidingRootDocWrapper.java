/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.SourcePosition;

class HidingRootDocWrapper extends HidingDocWrapper implements RootDoc {
    public HidingRootDocWrapper(RootDoc rootdoc, Map mapWrappers) {
        super(rootdoc, mapWrappers);
    }

    private RootDoc _getRootDoc() {
        return (RootDoc)getWrappedObject();
    }

    /* RootDoc */

    @Override
    public String[][] options() {
        return _getRootDoc().options();
    }

    @Override
    public PackageDoc[] specifiedPackages() {
        return (PackageDoc[])wrapOrHide(_getRootDoc().specifiedPackages());
    }

    @Override
    public ClassDoc[] specifiedClasses() {
        return (ClassDoc[])wrapOrHide(_getRootDoc().specifiedClasses()); 
    }

    @Override
    public ClassDoc[] classes() {
        return (ClassDoc[])wrapOrHide(_getRootDoc().classes());
    }

    @Override
    public PackageDoc packageNamed(String szName) {
        return (PackageDoc)wrapOrHide(_getRootDoc().packageNamed(szName));
    }

    @Override
    public ClassDoc classNamed(String szName) {
        return (ClassDoc)wrapOrHide(_getRootDoc().classNamed(szName));
    }

    /* DocErrorReporter */

    @Override
    public void printError(String szError) {
        _getRootDoc().printError(szError);
    }

    @Override
    public void printError(SourcePosition pos, String szError) {
        _getRootDoc().printError(pos, szError);
    }

    @Override
    public void printWarning(String szWarning) {
        _getRootDoc().printWarning(szWarning);
    }

    @Override
    public void printWarning(SourcePosition pos, String szWarning) {
        _getRootDoc().printWarning(pos, szWarning);
    }

    @Override
    public void printNotice(String szNotice) {
        _getRootDoc().printNotice(szNotice);
    }

    @Override
    public void printNotice(SourcePosition pos, String szNotice) {
        _getRootDoc().printNotice(pos, szNotice);
    }
}
