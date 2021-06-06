/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2016 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.Type;

class HidingParameterWrapper extends HidingWrapper implements Parameter {
    public HidingParameterWrapper(Parameter param, Map mapWrappers) {
        super(param, mapWrappers);
    }

    private Parameter _getParameter() {
        return (Parameter)getWrappedObject();
    }

    @Override
    public Type type() {
        return (Type)wrapOrHide(_getParameter().type());
    }

    @Override
    public String name() {
        return _getParameter().name();
    }

    @Override
    public String typeName() {
        return _getParameter().typeName();
    }

    @Override
    public String toString() {
        return _getParameter().toString();
    }

    @Override
    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])wrapOrHide(_getParameter().annotations());
    }
}
