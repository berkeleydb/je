/*-
 * See the files LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.HashMap;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;

/**
 * A doclet that allows developers to hide documentation for java elements.
 *
 * @see oracle.olapi.hidingDoclet
 */
public class HidingDoclet extends Standard {
    
    private static Class s_classBaseDoclet;

    /**
     * javadoc calls this method to generate documentation
     */
    public static boolean start(RootDoc root) {
        return Standard.start(new HidingRootDocWrapper(root, new HashMap())); 
    }

    /**
     * javadoc calls this method to check the validity of doclet-specific
     * command-line arguments.
     * <P>
     * Any arguments accepted by the standard doclet will be accepted by
     * HidingDoclet.
     */
    public static boolean validOptions(String options[][],
                                       DocErrorReporter reporter) {
        return Standard.validOptions(options, reporter);
    }

    /**
     * javadoc calls this method to check the number of non-flag command-line
     * arguments that should follow the given command-line flag.
     * <P>
     * Any arguments accepted by the standard doclet will be accepted by
     * HidingDoclet.
     */
    public static int optionLength(String option) {
        return Standard.optionLength(option);
    }

    /**
     * javadoc calls this method to check whether the doclet supports the
     * Java 5 extensions (generic types, annotations, enums, and varArgs)
     * <P>
     * Any arguments accepted by the standard doclet will be accepted by
     * HidingDoclet.
     */
    public static LanguageVersion languageVersion() {
        return LanguageVersion.JAVA_1_5;  //dgm code change
    }
}
