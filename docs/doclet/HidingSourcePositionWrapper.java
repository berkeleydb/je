/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.io.File;
import java.util.Map;

import com.sun.javadoc.SourcePosition;

class HidingSourcePositionWrapper extends HidingWrapper
                                       implements SourcePosition {
    public HidingSourcePositionWrapper(SourcePosition type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private SourcePosition _getSourcePosition() {
        return (SourcePosition)getWrappedObject();
    }

    @Override
    public File file() {
        return _getSourcePosition().file();
    }

    @Override
    public int line() {
        return _getSourcePosition().line();
    }

    @Override
    public int column() {
        return _getSourcePosition().column();
    }

    @Override
    public String toString() {
        return _getSourcePosition().toString();
    }
}
