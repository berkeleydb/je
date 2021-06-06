/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotatedType;
import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.AnnotationTypeElementDoc;
import com.sun.javadoc.AnnotationValue;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ConstructorDoc;
import com.sun.javadoc.Doc;
import com.sun.javadoc.ExecutableMemberDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MemberDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SerialFieldTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingWrapper {
    private Object _objWrapped;
    private Map _mapWrappers;

    public HidingWrapper(Object objWrapped, Map mapWrappers) {
        _objWrapped = objWrapped;
        _mapWrappers = mapWrappers;
    }

    public Object getWrappedObject() {
        return _objWrapped;
    }

    public Map getWrapperMap() {
        return _mapWrappers;
    }

    public String toString() {
        return getWrappedObject().toString();
    }

    public HidingWrapper wrapOrHide(Object object) {
        if ((object == null) || (object instanceof HidingWrapper)) {
            return (HidingWrapper)object;
        } else if (getWrapperMap().containsKey(object)) {
            return (HidingWrapper)getWrapperMap().get(object);
        } else {
            HidingWrapper wrapper = _wrapOrHide(object);
            getWrapperMap().put(object, wrapper);
            return wrapper;
        }
    }

    public Object[] wrapOrHide(Object[] objects) {
        HidingWrapper[] wrappers = new HidingWrapper[objects.length];
        int iFilteredCount = 0;

        for (int i = 0; i < objects.length; i++) {
            HidingWrapper wrapper = wrapOrHide(objects[i]);

            if (wrapper != null) {
                wrappers[iFilteredCount] = wrapper;
                iFilteredCount++;
            }
        }

        Object[] wrappersTrimmedAndTyped =
                  _createHidingWrapperArray(objects, iFilteredCount);
        System.arraycopy(wrappers, 0,
                         wrappersTrimmedAndTyped, 0, iFilteredCount);

        return wrappersTrimmedAndTyped;
    }

    private boolean _isHidden(Doc doc) {
        if (doc == null) {
            return false;
        } else {
            return (doc.tags("hidden").length > 0);
        }
    }

    /**
     * This is the method that actually instantiates objects.  Update it if
     * the Doclet API changes.  One hack here: ClassDoc must be handled before
     * Type because ClassDocs are also Type.  If we instantiate a
     * HidingTypeWrapper to hold a ClassDoc,
     * we'll have problems.  We should only
     * instantiate HidingTypeWrapper for otherwise unknown Types.
     */
    private HidingWrapper _wrapOrHide(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Doc) {
            if (_isHidden((Doc)object)) {
                return null;
            } else if (object instanceof PackageDoc) {
                return new HidingPackageDocWrapper((PackageDoc)object,
                                                    getWrapperMap());
            } else if (object instanceof ProgramElementDoc) {
                if ((_isHidden(((ProgramElementDoc)object).
                     containingClass())) || (_isHidden(((ProgramElementDoc)
                     object).containingPackage()))) {
                    return null;
                }

                if (object instanceof ClassDoc) {
                    if (object instanceof AnnotationTypeDoc) {
                        return new HidingAnnotationTypeDocWrapper(
                                   (AnnotationTypeDoc)object, getWrapperMap());
                    } else {
                        return new HidingClassDocWrapper((ClassDoc)object,
                                                          getWrapperMap());
                    }
                } else if (object instanceof MemberDoc) {
                    if (object instanceof ExecutableMemberDoc)
                    {
                        if (object instanceof ConstructorDoc) {
                            return new HidingConstructorDocWrapper(
                                       (ConstructorDoc)object,
                                        getWrapperMap());
                        } else if (object instanceof MethodDoc) {
                            // Added new classes for 1.5.
                            if (object instanceof AnnotationTypeElementDoc) {
                                return
                                    new HidingAnnotationTypeElementDocWrapper(
                                           (AnnotationTypeElementDoc)object,
                                            getWrapperMap());
                            } else {
                                return new HidingMethodDocWrapper(
                                           (MethodDoc)object, getWrapperMap());
                            }
                        } else if (object instanceof
                                              AnnotationTypeElementDoc) {
                            return new HidingAnnotationTypeElementDocWrapper(
                                       (AnnotationTypeElementDoc)object,
                                        getWrapperMap());
                        } else {
                            return new HidingExecutableMemberDocWrapper(
                                       (ExecutableMemberDoc)object,
                                        getWrapperMap());
                        }
                    } else if (object instanceof FieldDoc) {
                        return new HidingFieldDocWrapper((FieldDoc)object,
                                                          getWrapperMap());
                    } else {
                        return new HidingMemberDocWrapper((MemberDoc)object,
                                                           getWrapperMap());
                    }
                } else {
                    return new HidingProgramElementDocWrapper(
                               (ProgramElementDoc)object, getWrapperMap());
                }
            } else if (object instanceof RootDoc) {
                return new HidingRootDocWrapper((RootDoc)object,
                                                 getWrapperMap());
            } else {
                return new HidingDocWrapper((Doc)object, getWrapperMap());
            }
        } else if (object instanceof Parameter) {
            return new HidingParameterWrapper((Parameter)object,
                                               getWrapperMap());
        } else if (object instanceof Tag) {
            if (object instanceof ParamTag) {
                return new HidingParamTagWrapper((ParamTag)object,
                                                  getWrapperMap());
            } else if (object instanceof SeeTag) {
                return new HidingSeeTagWrapper((SeeTag)object, getWrapperMap());
            } else if (object instanceof SerialFieldTag) {
                return new HidingSerialFieldTagWrapper((SerialFieldTag)object,
                                                        getWrapperMap());
            } else if (object instanceof ThrowsTag) {
                return new HidingThrowsTagWrapper((ThrowsTag)object,
                                                   getWrapperMap());
            } else {
                return new HidingTagWrapper((Tag)object, getWrapperMap());
            }
        } else if (object instanceof Type) {
            if (object instanceof AnnotatedType) {
                return new HidingAnnotatedTypeWrapper(
                    (AnnotatedType) object, getWrapperMap());
            } else if (object instanceof AnnotationTypeDoc) {
                return new HidingAnnotationTypeDocWrapper(
                           (AnnotationTypeDoc)object, getWrapperMap());
            } else if (object instanceof ParameterizedType) {
                return new HidingParameterizedTypeWrapper(
                           (ParameterizedType)object, getWrapperMap());
            } else if (object instanceof TypeVariable) {
                return new HidingTypeVariableWrapper((TypeVariable)object,
                                                      getWrapperMap());
            } else if (object instanceof WildcardType) {
                return new HidingWildcardTypeWrapper((WildcardType)object,
                                                      getWrapperMap());
            } else {
                return new HidingTypeWrapper((Type)object, getWrapperMap());
            }
        } else if (object instanceof AnnotationDesc) {
            return new HidingAnnotationDescWrapper((AnnotationDesc)object,
                                                    getWrapperMap());
        } else if (object instanceof AnnotationValue) {
            return new HidingAnnotationValueWrapper((AnnotationValue)object,
                                                     getWrapperMap());
        } else if (object instanceof SourcePosition) {
            return new HidingSourcePositionWrapper((SourcePosition)object,
                                                    getWrapperMap());
        } else {
            return new HidingWrapper(object, getWrapperMap());
        }
    }

    /**
     * This is the method that instantiates types arrays.
     * @see _wrapOrHide(Object)
     */
    private static Object[] _createHidingWrapperArray(Object[] objects,
                                                      int size) {
        if (objects instanceof Doc[]) {
            if (objects instanceof PackageDoc[]) {
                return new PackageDoc[size];
            } else if (objects instanceof ProgramElementDoc[]) {
                if (objects instanceof ClassDoc[]) {
                    if (objects instanceof AnnotationTypeDoc[]) {
                        return new AnnotationTypeDoc[size];
                    } else {
                        return new ClassDoc[size];
                    }
                } else if (objects instanceof MemberDoc[]) {
                    if (objects instanceof ExecutableMemberDoc[]) {
                        if (objects instanceof ConstructorDoc[]) {
                            return new ConstructorDoc[size];
                        } else if (objects instanceof MethodDoc[]) {
                            if (objects instanceof
                                            AnnotationTypeElementDoc[]) {
                                return new AnnotationTypeElementDoc[size];
                            } else {
                                return new MethodDoc[size];
                            }
                        } else if (objects instanceof
                                               AnnotationTypeElementDoc[]) {
                            return new AnnotationTypeElementDoc[size];
                        } else {
                            return new ExecutableMemberDoc[size];
                        }
                    } else if (objects instanceof FieldDoc[]) {
                        return new FieldDoc[size];
                    } else {
                        return new MemberDoc[size];
                    }
                } else {
                    return new ProgramElementDoc[size];
                }
            } else if (objects instanceof RootDoc[]) {
                return new RootDoc[size];
            } else {
                return new Doc[size];
            }
        } else if (objects instanceof Parameter[]) {
            return new Parameter[size];
        } else if (objects instanceof Tag[]) {
            if (objects instanceof ParamTag[]) {
                return new ParamTag[size];
            } else if (objects instanceof SeeTag[]) {
                return new SeeTag[size];
            } else if (objects instanceof SerialFieldTag[]) {
                return new SerialFieldTag[size];
            } else if (objects instanceof ThrowsTag[]) {
                return new ThrowsTag[size];
            } else {
                return new Tag[size];
            }
        } else if (objects instanceof Type[]) {
            if (objects instanceof AnnotationTypeDoc[]) {
                return new AnnotationTypeDoc[size];
            } else if (objects instanceof ParameterizedType[]) {
                return new ParameterizedType[size];
            } else if (objects instanceof TypeVariable[]) {
                return new TypeVariable[size];
            } else if (objects instanceof WildcardType[]) {
                return new WildcardType[size];
            } else {
                return new Type[size];
            }
        } else if (objects instanceof AnnotationDesc[]) {
            return new AnnotationDesc[size];
        } else if (objects instanceof AnnotationValue[]) {
            return new AnnotationValue[size];
        } else if (objects instanceof SourcePosition[]) {
                   return new SourcePosition[size];
        } else {
            return new Object[size];
        }
    }
}
