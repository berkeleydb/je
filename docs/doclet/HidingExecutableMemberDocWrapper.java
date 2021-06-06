/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ExecutableMemberDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;

class HidingExecutableMemberDocWrapper extends HidingMemberDocWrapper
                                       implements ExecutableMemberDoc {

    public HidingExecutableMemberDocWrapper(ExecutableMemberDoc execmemdoc,
                                            Map mapWrappers) {
        super(execmemdoc, mapWrappers);
    }

    private ExecutableMemberDoc _getExecutableMemberDoc() {
        return (ExecutableMemberDoc)getWrappedObject();
    }

    @Override
    public ClassDoc[] thrownExceptions() {
        return (ClassDoc[])
                wrapOrHide(_getExecutableMemberDoc().thrownExceptions());
    }

    @Override
    public Type[] thrownExceptionTypes() {
        return (Type[])
                wrapOrHide(_getExecutableMemberDoc().thrownExceptionTypes());
    }

    @Override
    public boolean isNative() {
        return _getExecutableMemberDoc().isNative();
    }

    @Override
    public boolean isSynchronized() {
        return _getExecutableMemberDoc().isSynchronized();
    }

    @Override
    public boolean isVarArgs() {
        return _getExecutableMemberDoc().isVarArgs();
    }

    @Override
    public Parameter[] parameters() {
        return (Parameter[])wrapOrHide(_getExecutableMemberDoc().parameters());
    }

    @Override
    public Type receiverType() {
        return (Type)wrapOrHide(_getExecutableMemberDoc().receiverType());
    }

    @Override
    public ThrowsTag[] throwsTags() {
        return (ThrowsTag[])
                wrapOrHide(_getExecutableMemberDoc().throwsTags());
    }

    @Override
    public ParamTag[] paramTags() {
        return (ParamTag[])
                wrapOrHide(_getExecutableMemberDoc().paramTags());
    }

    @Override
    public ParamTag[] typeParamTags() {
        return (ParamTag[])
                wrapOrHide(_getExecutableMemberDoc().typeParamTags());
    }

    @Override
    public String signature() {
        return _getExecutableMemberDoc().signature();
    }

    @Override
    public String flatSignature() {
        return _getExecutableMemberDoc().flatSignature();
    }

    @Override
    public TypeVariable[] typeParameters() {
        return (TypeVariable[])
                wrapOrHide(_getExecutableMemberDoc().typeParameters());
    }
}
