/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.Doc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingDocWrapper extends HidingWrapper implements Doc {

    public HidingDocWrapper(Doc doc, Map mapWrappers) {
        super(doc, mapWrappers);
    }

    private Doc _getDoc() {
        return (Doc)getWrappedObject();
    }

    @Override
    public String commentText() {
        return _getDoc().commentText();
    }

    @Override
    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getDoc().tags());
    }

    @Override
    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getDoc().tags(szTagName));
    }

    @Override
    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getDoc().seeTags());
    }

    @Override
    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getDoc().inlineTags());
    }

    @Override
    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getDoc().firstSentenceTags());
    }

    @Override
    public String getRawCommentText() {
        return _getDoc().getRawCommentText();
    }

    @Override
    public void setRawCommentText(String szText) {
        _getDoc().setRawCommentText(szText);
    }

    @Override
    public String name() {
        return _getDoc().name();
    }

    @Override
    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getDoc().compareTo(obj);
        }
    }

    @Override
    public boolean isField() {
        return _getDoc().isField();
    }

    @Override
    public boolean isEnumConstant() {
        return _getDoc().isEnumConstant();
    }

    @Override
    public boolean isConstructor() {
        return _getDoc().isConstructor();
    }

    @Override
    public boolean isMethod() {
        return _getDoc().isMethod();
    }

    @Override
    public boolean isAnnotationTypeElement() {
        return _getDoc().isAnnotationTypeElement();
    }

    @Override
    public boolean isInterface() {
        return _getDoc().isInterface();
    }

    @Override
    public boolean isException() {
        return _getDoc().isException();
    }

    @Override
    public boolean isError() {
        return _getDoc().isError();
    }

    @Override
    public boolean isEnum() {
        return _getDoc().isEnum();
    }

    @Override
    public boolean isAnnotationType() {
        return _getDoc().isAnnotationType();
    }

    @Override
    public boolean isOrdinaryClass() {
        return _getDoc().isOrdinaryClass();
    }

    @Override
    public boolean isClass() {
        return _getDoc().isClass();
    }

    @Override
    public boolean isIncluded() {
        return _getDoc().isIncluded();
    }

    @Override
    public SourcePosition position() {
        return _getDoc().position();
    }
}
