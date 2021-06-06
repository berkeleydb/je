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

package com.sleepycat.je.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Splitter is used to split a string based on a delimiter.
 * Support includes double quoted strings, and the escape character.
 * Raw tokens are returned that include the double quotes, white space,
 * and escape characters.
 *
 */
public class Splitter {
    private static final char QUOTECHAR = '"';
    private static final char ESCAPECHAR = '\\';
    private final char delimiter;
    private final List<String> tokens = new ArrayList<String>();
    private enum StateType {COLLECT, COLLECTANY, QUOTE};
    private StateType prevState;
    private StateType state;
    private int startIndex;
    private int curIndex;
    private String row;

    public Splitter(char delimiter) {
        this.delimiter = delimiter;
    }

    public String[] tokenize(String inrow) {
        row = inrow;
        state = StateType.COLLECT;
        tokens.clear();
        startIndex = 0;
        curIndex = 0;
        for (int cur = 0; cur < row.length(); cur++) {
            char c = row.charAt(cur);
            switch (state) {
                case COLLECT :
                    if (isDelimiter(c)) {
                        outputToken();
                        startIndex = cur + 1;
                        curIndex = startIndex;
                    } else {
                        if (isQuote(c) && isQuoteState()) {
                            state = StateType.QUOTE;
                        } else if (isEscape(c)) {
                            prevState = state;
                            state = StateType.COLLECTANY;
                        }
                        curIndex++;
                    }
                    break;
                case COLLECTANY:
                    curIndex++;
                    state = prevState;
                    break;
                case QUOTE:
                    if (isEscape(c)) {
                        prevState = state;
                        state = StateType.COLLECTANY;
                    } else if (isQuote(c)) {
                        state = StateType.COLLECT;
                    }
                    curIndex++;
                    break;
            }
        }
        outputToken();
        String[] retvals = new String[tokens.size()];
        tokens.toArray(retvals);
        return retvals;
    }

    private boolean isQuote(char c) {
        return (c == QUOTECHAR) ? true : false;
    }

    private boolean isEscape(char c) {
        return (c == ESCAPECHAR) ? true : false;
    }

    private boolean isDelimiter(char c) {
        return (c == delimiter) ? true : false;
    }

    private void outputToken() {
        if (startIndex < curIndex) {
            tokens.add(row.substring(startIndex, curIndex));
        } else {
            tokens.add("");
        }
    }

    private boolean isQuoteState() {
        for (int i = startIndex; i < curIndex; i++) {
            if (!Character.isWhitespace(row.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
