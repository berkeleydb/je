/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.persist.model;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Overrides the default rules for field persistence and defines a field as
 * being persistent even when it is declared with the <code>transient</code>
 * keyword.
 *
 * <p>By default, the persistent fields of a class are all declared instance
 * fields that are non-transient (are not declared with the
 * <code>transient</code> keyword).  The default rules may be overridden by
 * specifying the {@link NotPersistent} or {@link NotTransient} annotation.</p>
 *
 * <p>For example, the following field is transient with respect to Java
 * serialization but is persistent with respect to the DPL.</p>
 *
 * <pre style="code">
 *      {@code @NotTransient}
 *      transient int myField;
 * }
 * </pre>
 *
 * @see NotPersistent
 * @author Mark Hayes
 */
@Documented @Retention(RUNTIME) @Target(FIELD)
public @interface NotTransient {
}
