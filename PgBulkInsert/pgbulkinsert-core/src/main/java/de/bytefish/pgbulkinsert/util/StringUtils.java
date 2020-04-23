// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.util;

import java.nio.charset.Charset;

public class StringUtils {
	
	private static Charset utf8Charset = Charset.forName("UTF-8");

    private StringUtils() {}

    public static boolean isNullOrWhiteSpace(String input) {
        return  input == null || input.trim().length() == 0;
    }

    public static byte[] getUtf8Bytes(String value) {
        return value.getBytes(utf8Charset);
    }

	public static String removeNullCharacter(String data) {
		if (data == null) {
			return data;
		}

		return data.replaceAll("\u0000", "");
	}
}