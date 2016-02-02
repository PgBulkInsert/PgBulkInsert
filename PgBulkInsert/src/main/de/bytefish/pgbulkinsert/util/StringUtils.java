// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util;

public class StringUtils {

    private StringUtils() {}

    public static boolean isNullOrWhiteSpace(String input) {
        return  input == null || input.trim().length() == 0;
    }

    public static String[] trimAllElements(String[] elements) {
        String[] result = new String[elements.length];
        for(int pos = 0; pos < elements.length; pos++) {
            result[pos] = elements[pos].trim();
        }
        return result;
    }

}