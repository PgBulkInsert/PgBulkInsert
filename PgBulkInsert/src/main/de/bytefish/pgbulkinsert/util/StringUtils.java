// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;

public class StringUtils {

    private StringUtils() {}

    public static boolean isNullOrWhiteSpace(String input) {
        return  input == null || input.trim().length() == 0;
    }

}