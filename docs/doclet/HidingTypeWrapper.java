/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotatedType;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingTypeWrapper extends HidingWrapper implements Type {
    public HidingTypeWrapper(Type type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private Type _getType() {
        return (Type)getWrappedObject();
    }

    @Override
    public String typeName() {
        return _getType().typeName();
    }

    @Override
    public String qualifiedTypeName() {
        return _getType().qualifiedTypeName();
    }

    @Override
    public String simpleTypeName() {
        return _getType().simpleTypeName();
    }

    @Override
    public String dimension() {
        return _getType().dimension();
    }

    @Override
    public String toString() {
        return _getType().toString();
    }

    @Override
    public boolean isPrimitive() {
        return _getType().isPrimitive();
    }

    @Override
    public ClassDoc asClassDoc() {
        return (ClassDoc)wrapOrHide(_getType().asClassDoc());
    }

    @Override
    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)wrapOrHide(_getType().asParameterizedType());
    }

    @Override
    public TypeVariable asTypeVariable() {
        return (TypeVariable)wrapOrHide(_getType().asTypeVariable());
    }

    @Override
    public WildcardType asWildcardType() {
        return (WildcardType)wrapOrHide(_getType().asWildcardType()); 
    }

    @Override
    public AnnotatedType asAnnotatedType() {
        return (AnnotatedType)wrapOrHide(_getType().asAnnotatedType());
    }

    @Override
    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)wrapOrHide(_getType().asAnnotationTypeDoc());
    }

    @Override
    public Type getElementType() {
        return (Type)wrapOrHide(_getType().getElementType());
    }
}
