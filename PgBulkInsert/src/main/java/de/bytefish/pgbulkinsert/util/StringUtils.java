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

	/**
	 * Remove invalid utf-8 character of 0x00
	 */
	public static String escapeString(String data) {
		if (null == data) {
			return data;
		}

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < data.length(); ++i) {
			char c = data.charAt(i);
			switch (c) {
			case 0x00:
				continue;
			default:
				break;
			}

			sb.append(c);
		}
		return sb.toString();
	}
}