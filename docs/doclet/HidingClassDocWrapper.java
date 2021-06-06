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
import com.sun.javadoc.ConstructorDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingClassDocWrapper extends HidingProgramElementDocWrapper
        implements ClassDoc {

    public HidingClassDocWrapper(ClassDoc classdoc, Map mapWrappers) {
        super(classdoc, mapWrappers);
    }

    private ClassDoc _getClassDoc() {
        return (ClassDoc)getWrappedObject();
    }

    /* ClassDoc */

    @Override
    public boolean isAbstract() {
        return _getClassDoc().isAbstract();
    }

    @Override
    public boolean isSerializable() {
        return _getClassDoc().isSerializable();
    }

    @Override
    public boolean isExternalizable() {
        return _getClassDoc().isExternalizable();
    }

    @Override
    public MethodDoc[] serializationMethods() {
        return (MethodDoc[])wrapOrHide(_getClassDoc().serializationMethods());
    }

    @Override
    public FieldDoc[] serializableFields() {
        return (FieldDoc[])wrapOrHide(_getClassDoc().serializableFields());
    }

    @Override
    public boolean definesSerializableFields() {
        return _getClassDoc().definesSerializableFields();
    }

    @Override
    public ClassDoc superclass() {
        return (ClassDoc)wrapOrHide(_getClassDoc().superclass());
    }

    @Override
    public Type superclassType() {
        return (Type) wrapOrHide(_getClassDoc().superclassType());
    }

    @Override
    public boolean subclassOf(ClassDoc classdoc) {
        if (classdoc instanceof HidingClassDocWrapper) {
            classdoc = (ClassDoc)
                       ((HidingClassDocWrapper)classdoc).getWrappedObject();
        }

        return _getClassDoc().subclassOf(classdoc);
    }

    @Override
    public ClassDoc[] interfaces() {
        return (ClassDoc[])wrapOrHide(_getClassDoc().interfaces());
    }

    @Override
    public Type[] interfaceTypes() {
        return (Type[]) wrapOrHide(_getClassDoc().interfaceTypes());
    }

    @Override
    public TypeVariable[] typeParameters() {
        return (TypeVariable[]) wrapOrHide(_getClassDoc().typeParameters());
    }

    @Override
    public ParamTag[] typeParamTags() {
        return (ParamTag[]) wrapOrHide(_getClassDoc().typeParamTags());
    }

    @Override
    public FieldDoc[] fields() {
        return (FieldDoc[])wrapOrHide(_getClassDoc().fields());
    }

    @Override
    public FieldDoc[] fields(boolean filter) {
        return (FieldDoc[])wrapOrHide(_getClassDoc().fields(filter));
    }

    @Override
    public FieldDoc[] enumConstants() {
        return (FieldDoc[])wrapOrHide(_getClassDoc().enumConstants());
    }

    @Override
    public MethodDoc[] methods() {
        return (MethodDoc[])wrapOrHide(_getClassDoc().methods());
    }

    @Override
    public MethodDoc[] methods(boolean filter) {
        return (MethodDoc[])wrapOrHide(_getClassDoc().methods(filter));
    }

    @Override
    public ConstructorDoc[] constructors() {
        return (ConstructorDoc[])wrapOrHide(_getClassDoc().constructors());
    }

    @Override
    public ConstructorDoc[] constructors(boolean filter) {
        return (ConstructorDoc[])
                wrapOrHide(_getClassDoc().constructors(filter));
    }

    @Override
    public ClassDoc[] innerClasses() {
        return (ClassDoc[])wrapOrHide(_getClassDoc().innerClasses());
    }

    @Override
    public ClassDoc[] innerClasses(boolean filter) {
        return (ClassDoc[])wrapOrHide(_getClassDoc().innerClasses(filter));
    }

    @Override
    public ClassDoc findClass(String szClassName) {
        return (ClassDoc)wrapOrHide(_getClassDoc().findClass(szClassName));
    }

    /**
     * @deprecated as of 11.0
     */
    @Override
    public ClassDoc[] importedClasses() {
        return (ClassDoc[])wrapOrHide(_getClassDoc().importedClasses());
    }

    /**
     * @deprecated as of 11.0
     */
    @Override
    public PackageDoc[] importedPackages() {
        return (PackageDoc[])wrapOrHide(_getClassDoc().importedPackages());
    }

    /* Type */

    @Override
    public String typeName() {
        return _getClassDoc().typeName();
    }

    @Override
    public String qualifiedTypeName() {
        return _getClassDoc().qualifiedTypeName();
    }

    @Override
    public String simpleTypeName() {
        return _getClassDoc().simpleTypeName();
    }

    @Override
    public String dimension() {
        return _getClassDoc().dimension();
    }

    @Override
    public String toString() {
        return _getClassDoc().toString();
    }

    @Override
    public boolean isPrimitive() {
        return _getClassDoc().isPrimitive();
    }

    @Override
    public ClassDoc asClassDoc() {
        return this;
    }

    @Override
    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)wrapOrHide(
            _getClassDoc().asParameterizedType());
    }

    @Override
    public TypeVariable asTypeVariable() {
        return (TypeVariable)wrapOrHide(_getClassDoc().asTypeVariable());
    }

    @Override
    public WildcardType asWildcardType() {
        return (WildcardType)wrapOrHide(_getClassDoc().asWildcardType());
    }

    @Override
    public AnnotatedType asAnnotatedType() {
        return (AnnotatedType)wrapOrHide(_getClassDoc().asAnnotatedType());
    }

    @Override
    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)wrapOrHide(
            _getClassDoc().asAnnotationTypeDoc());
    }

    @Override
    public Type getElementType() {
        return (Type)wrapOrHide(_getClassDoc().getElementType());
    }
}
